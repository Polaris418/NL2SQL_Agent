from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable

from app.agent.chart_suggester import ChartSuggester
from app.agent.error_reflector import ErrorReflector
from app.agent.query_rewriter import QueryRewriter
from app.agent.result_summarizer import ResultSummarizer
from app.agent.schema_retriever import SchemaRetriever
from app.agent.sql_executor import SQLExecutor
from app.agent.sql_generator import SQLGenerator
from app.agent.utils import (
    build_follow_up_prompt_context,
    build_user_friendly_error,
    categorize_error_message,
    detect_database_mismatch,
    log_query_audit,
    log_exception_details,
    query_cache_key,
    schema_fingerprint,
    sanitize_error_message,
    table_name,
)
from app.db.manager import DBManager
from app.db.repositories.history_repo import QueryHistoryRepository
from app.schemas.query import AgentStep, QueryHistoryItem, QueryResult, QueryTelemetry


StepHandler = Callable[[AgentStep], Awaitable[None] | None]


class TooManyRequestsError(RuntimeError):
    pass


class NL2SQLAgent:
    def __init__(
        self,
        db_manager: DBManager,
        query_rewriter: QueryRewriter,
        schema_retriever: SchemaRetriever,
        sql_generator: SQLGenerator,
        sql_executor: SQLExecutor,
        error_reflector: ErrorReflector,
        chart_suggester: ChartSuggester,
        result_summarizer: ResultSummarizer,
        max_retries: int = 3,
    ):
        self.db_manager = db_manager
        self.query_rewriter = query_rewriter
        self.schema_retriever = schema_retriever
        self.sql_generator = sql_generator
        self.sql_executor = sql_executor
        self.error_reflector = error_reflector
        self.chart_suggester = chart_suggester
        self.result_summarizer = result_summarizer
        self.max_retries = max_retries
        self._semaphore = asyncio.Semaphore(db_manager.settings.max_concurrent_queries)
        self._active_queries = 0
        self._peak_active_queries = 0
        self._cache: dict[str, tuple[float, QueryResult]] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def _clone_result(self, result: QueryResult) -> QueryResult:
        if hasattr(result, "model_copy"):
            return result.model_copy()
        if hasattr(result, "copy"):
            return result.copy()
        return result

    async def process_query(
        self,
        question: str,
        connection_id: str,
        *,
        page_number: int = 1,
        page_size: int | None = None,
        include_total_count: bool = False,
        previous_query_id: str | None = None,
        follow_up_instruction: str | None = None,
        step_handler: StepHandler | None = None,
    ) -> QueryResult:
        queue_started_at = time.perf_counter()
        async with self._semaphore:
            queue_wait_ms = (time.perf_counter() - queue_started_at) * 1000.0
            self._active_queries += 1
            self._peak_active_queries = max(self._peak_active_queries, self._active_queries)
            started_at = time.perf_counter()
            steps: list[AgentStep] = []
            llm_latency = 0.0
            history_repo = QueryHistoryRepository(self.db_manager.metadata_db)
            connector = self.db_manager.get_connector(connection_id)
            schema_cache = self.db_manager.get_schema_cache(connection_id) or self.db_manager.refresh_schema_cache(connection_id)
            await self.schema_retriever.index_schema(connection_id, schema_cache.tables)
            context_query_id, augmented_question = self._resolve_follow_up_context(
                connection_id=connection_id,
                question=question,
                previous_query_id=previous_query_id,
                follow_up_instruction=follow_up_instruction,
            )
            question_for_model = augmented_question or question
            schema_fp = schema_fingerprint(schema_cache.tables)
            db_type = getattr(connector.config.db_type, "value", connector.config.db_type)
            cache_key = query_cache_key(
                question=question_for_model,
                connection_id=connection_id,
                schema_fp=schema_fp,
                db_type=db_type,
                page_number=page_number,
                page_size=page_size or 1000,
                include_total_count=include_total_count,
                follow_up_instruction=follow_up_instruction,
                previous_query_id=previous_query_id,
            )
            audit_id = log_query_audit(
                "nl2sql.query.start",
                extra={
                    "connection_id": connection_id,
                    "cache_key": cache_key,
                    "queue_wait_ms": round(queue_wait_ms, 2),
                    "active_concurrency": self._active_queries,
                    "peak_concurrency": self._peak_active_queries,
                    "schema_fingerprint": schema_fp,
                },
            )
            telemetry = QueryTelemetry(
                cache_hit=False,
                cache_key=cache_key,
                queue_wait_ms=round(queue_wait_ms, 2),
                active_concurrency=self._active_queries,
                peak_concurrency=self._peak_active_queries,
                schema_fingerprint=schema_fp,
                schema_table_count=len(schema_cache.tables),
                retrieval_backend=self.schema_retriever.retrieval_backend,
                embedding_backend=self.schema_retriever.embedding_backend,
                audit_id=audit_id,
            )

            try:
                cached = await self._cache_get(cache_key, step_handler)
                if cached is not None:
                    cached.telemetry.cache_hit = True
                    cached.telemetry.cache_key = cache_key
                    cached.telemetry.queue_wait_ms = telemetry.queue_wait_ms
                    cached.telemetry.active_concurrency = telemetry.active_concurrency
                    cached.telemetry.peak_concurrency = telemetry.peak_concurrency
                    cached.telemetry.schema_fingerprint = schema_fp
                    cached.telemetry.schema_table_count = len(schema_cache.tables)
                    cached.telemetry.retrieval_backend = telemetry.retrieval_backend
                    cached.telemetry.embedding_backend = telemetry.embedding_backend
                    cached.telemetry.audit_id = log_query_audit(
                        "nl2sql.query.cache_hit",
                        extra={
                            "connection_id": connection_id,
                            "cache_key": cache_key,
                            "question": question,
                            "context_query_id": context_query_id,
                        },
                    )
                    history_repo.save_query_result(connection_id, cached)
                    return cached

                rewritten, latency = await self.query_rewriter.rewrite(question_for_model)
                llm_latency += latency
                await self._emit(steps, step_handler, "rewrite", rewritten)

                retrieval_result = await self.schema_retriever.retrieve_detailed(rewritten, connection_id)
                tables = list(getattr(retrieval_result, "tables", []) or [])
                retrieved_documents = list(getattr(retrieval_result, "documents", []) or [])
                retrieval_metadata = dict(getattr(retrieval_result, "metadata", {}) or {})
                retrieval_stats = self.schema_retriever.last_stats
                retrieval_failure_category = (
                    retrieval_metadata.get("failure_category")
                    or retrieval_stats.get("failure_category")
                )
                retrieval_failure_message = (
                    getattr(getattr(retrieval_result, "telemetry", None), "error_message", None)
                    or retrieval_metadata.get("error_message")
                    or retrieval_stats.get("error_message")
                )
                telemetry.retrieval_scope_count = retrieval_stats.get("scope_count")
                telemetry.retrieval_candidates = retrieval_stats.get("candidates")
                telemetry.retrieval_selected = retrieval_stats.get("selected")
                telemetry.retrieval_lexical_count = retrieval_stats.get("lexical_count")
                telemetry.retrieval_vector_count = retrieval_stats.get("vector_count")
                telemetry.retrieval_latency_ms = retrieval_stats.get("latency_ms")
                telemetry.retrieval_top_score = retrieval_stats.get("top_score")
                telemetry.retrieval_backend = retrieval_stats.get("retrieval_backend", telemetry.retrieval_backend)
                telemetry.embedding_backend = retrieval_stats.get("embedding_backend", telemetry.embedding_backend)
                telemetry.relationship_count = retrieval_stats.get("relationship_count")
                telemetry.relationship_tables = list(retrieval_stats.get("relationship_tables") or [])
                telemetry.column_annotation_count = retrieval_stats.get("column_annotation_count")
                schema_retrieval_message = ", ".join(table_name(table) for table in tables)
                if not schema_retrieval_message and retrieval_failure_category:
                    schema_retrieval_message = retrieval_failure_message or retrieval_failure_category
                await self._emit(steps, step_handler, "schema_retrieval", schema_retrieval_message or "????")

                if retrieval_failure_category and not tables:
                    error_type, user_message, error_suggestion = build_user_friendly_error(retrieval_failure_message)
                    error_type = retrieval_failure_category or error_type
                    error_suggestion = error_suggestion or "????????? Schema / RAG ??????"
                    await self._emit(
                        steps,
                        step_handler,
                        "error_reflection",
                        user_message,
                        {
                            "error_type": error_type,
                            "error_suggestion": error_suggestion,
                            "failure_stage": retrieval_metadata.get("failure_stage") or retrieval_stats.get("failure_stage"),
                            "db_type": db_type,
                        },
                    )
                    response = QueryResult(
                        question=question,
                        rewritten_query=rewritten,
                        retrieved_tables=tables,
                        sql="",
                        result=None,
                        chart=None,
                        steps=steps,
                        status="failed",
                        retry_count=0,
                        llm_latency_ms=round(llm_latency, 2),
                        db_latency_ms=None,
                        error_message=user_message,
                        error_type=error_type,
                        error_suggestion=error_suggestion,
                        context_source_query_id=context_query_id,
                        telemetry=telemetry,
                    )
                    await self._emit(steps, step_handler, "done", "failed")
                    history_repo.save_query_result(connection_id, response)
                    return response

                few_shot_domain = None
                for document in retrieved_documents:
                    domain_tags = list(getattr(document, "domain_tags", []) or [])
                    if domain_tags:
                        few_shot_domain = str(domain_tags[0])
                        break

                sql, latency = await self.sql_generator.generate(
                    question_for_model,
                    rewritten,
                    tables,
                    db_type,
                    schema_context_details={
                        "query_id": retrieval_metadata.get("query_id") or context_query_id,
                        "connection_id": connection_id,
                        "relationship_clues": retrieval_metadata.get("relationship_clues"),
                        "column_annotations": retrieval_metadata.get("column_annotations"),
                        "documents": retrieved_documents,
                    },
                    retrieval_metadata=retrieval_metadata,
                    few_shot_scope={"connection_id": connection_id, "db_type": db_type},
                    few_shot_domain=few_shot_domain,
                )
                telemetry.few_shot_example_ids = [
                    str(item.get("id"))
                    for item in getattr(self.sql_generator, "last_few_shot_examples", []) or []
                    if item.get("id")
                ]
                packed_metadata = dict(getattr(self.sql_generator, "last_schema_context_metadata", {}) or {})
                telemetry.packed_context_tables = len(tables)
                telemetry.packed_context_chars = len(getattr(self.sql_generator, "last_schema_context_text", "") or "")
                telemetry.packed_context_truncated = bool(packed_metadata.get("truncated"))
                telemetry.packed_context_tokens = int(packed_metadata.get("final_token_count") or 0) or None
                telemetry.packed_context_budget = dict(packed_metadata.get("budget") or {})
                telemetry.packed_context_limit_reason = packed_metadata.get("limit_reason")
                telemetry.packed_context_dropped_tables = list(packed_metadata.get("dropped_tables") or [])
                telemetry.packed_context_dropped_columns = {
                    str(key): list(value or [])
                    for key, value in dict(packed_metadata.get("dropped_columns") or {}).items()
                }
                telemetry.packed_context_dropped_relationship_clues = [
                    dict(item) for item in list(packed_metadata.get("dropped_relationship_clues") or [])
                ]
                telemetry.tenant_isolation_key = retrieval_metadata.get("cache_key")
                llm_latency += latency
                record_generation = getattr(self.schema_retriever, "record_generation_artifacts", None)
                if callable(record_generation):
                    record_generation(
                        retrieval_metadata.get("query_id"),
                        prompt_schema=getattr(self.sql_generator, "last_schema_context_text", None),
                        final_sql=sql,
                    )
                await self._emit(
                    steps,
                    step_handler,
                    "sql_generation",
                    sql,
                    {
                        "context_tables": len(tables),
                        "relationship_clues": len(list(retrieval_metadata.get("relationship_clues") or [])),
                        "column_annotation_tables": len(dict(retrieval_metadata.get("column_annotations") or {})),
                    },
                )

                mismatch_detected, mismatch_message = detect_database_mismatch(
                    question=f"{question_for_model} {rewritten}",
                    schema_tables=schema_cache.tables,
                    sql=sql,
                )
                if mismatch_detected:
                    await self._emit(
                        steps,
                        step_handler,
                        "error_reflection",
                        mismatch_message or "The current database may not match this question.",
                        {
                            "error_type": "database_mismatch",
                            "error_suggestion": "切换到包含相关业务表的数据库，或改问当前数据库里的内容。",
                            "db_type": db_type,
                        },
                    )
                    response = QueryResult(
                        question=question,
                        rewritten_query=rewritten,
                        retrieved_tables=tables,
                        sql=sql,
                        result=None,
                        chart=None,
                        steps=steps,
                        status="failed",
                        retry_count=0,
                        llm_latency_ms=round(llm_latency, 2),
                        db_latency_ms=None,
                        error_message="当前数据库可能不匹配。请切换到包含相关表的数据库后再试。",
                        error_type="database_mismatch",
                        error_suggestion="切换数据库后重新提问，或询问当前数据库里已有的表和字段。",
                        context_source_query_id=context_query_id,
                        telemetry=telemetry,
                    )
                    await self._emit(steps, step_handler, "done", "failed")
                    history_repo.save_query_result(connection_id, response)
                    return response

                result = None
                retry_count = 0
                last_error = None
                while retry_count <= self.max_retries:
                    try:
                        result = self._execute_sql(
                            connector,
                            sql,
                            page_number=page_number,
                            page_size=page_size,
                            include_total_count=include_total_count,
                        )
                        await self._emit(steps, step_handler, "sql_execution", "SQL executed successfully.")
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_error = sanitize_error_message(str(exc))
                        log_exception_details(
                            "SQL execution attempt failed",
                            exc=exc,
                            extra={
                                "connection_id": connection_id,
                                "question": question,
                                "retry_count": retry_count,
                                "sql": sanitize_error_message(sql),
                            },
                        )
                        mismatch_detected, mismatch_message = detect_database_mismatch(
                            question=f"{question_for_model} {rewritten}",
                            schema_tables=schema_cache.tables,
                            sql=sql,
                            error_message=last_error,
                        )
                        if mismatch_detected:
                            last_error = mismatch_message or last_error
                            break
                        if retry_count >= self.max_retries:
                            break
                        analysis, sql, latency = await self.error_reflector.reflect(question, sql, last_error, tables)
                        llm_latency += latency
                        retry_count += 1
                        await self._emit(steps, step_handler, "error_reflection", analysis, {"error": last_error})
                        await self._emit(steps, step_handler, "retry", sql)

                error_type = None
                error_suggestion = None
                if result is None:
                    error_type, user_message, error_suggestion = build_user_friendly_error(last_error)
                    last_error = user_message
                else:
                    last_error = None

                chart = self.chart_suggester.suggest(result) if result is not None else None
                
                # 生成结果总结
                summary = None
                if result is not None:
                    try:
                        summary = await self.result_summarizer.summarize(question, result)
                        if summary:
                            await self._emit(steps, step_handler, "result_summary", summary)
                    except Exception:  # noqa: BLE001
                        # 总结生成失败不影响主流程
                        pass
                
                telemetry.llm_cache_hit = bool(
                    getattr(self.query_rewriter, "last_cache_hit", False)
                    or getattr(self.sql_generator, "last_cache_hit", False)
                    or getattr(self.error_reflector, "last_cache_hit", False)
                )
                response = QueryResult(
                    question=question,
                    rewritten_query=rewritten,
                    retrieved_tables=tables,
                    sql=sql,
                    result=result,
                    chart=chart,
                    summary=summary,
                    steps=steps,
                    status="success" if result is not None else "failed",
                    retry_count=retry_count,
                    llm_latency_ms=round(llm_latency, 2),
                    db_latency_ms=None if result is None else result.db_latency_ms,
                    error_message=last_error,
                    error_type=error_type,
                    error_suggestion=error_suggestion,
                    context_source_query_id=context_query_id,
                    telemetry=telemetry,
                )
                await self._emit(steps, step_handler, "done", response.status.value if hasattr(response.status, "value") else response.status)
                history_repo.save_query_result(connection_id, response)
                log_query_audit(
                    "nl2sql.query.complete",
                    extra={
                        "connection_id": connection_id,
                        "cache_key": cache_key,
                        "status": getattr(response.status, "value", response.status),
                        "retry_count": retry_count,
                        "llm_latency_ms": response.llm_latency_ms,
                        "db_latency_ms": response.db_latency_ms,
                        "error_type": response.error_type,
                        "elapsed_ms": round((time.perf_counter() - started_at) * 1000.0, 2),
                    },
                )
                if getattr(response.status, "value", response.status) == "success":
                    self._cache_put(cache_key, response)
                return response
            except Exception as exc:  # noqa: BLE001
                log_exception_details(
                    "NL2SQL query processing failed",
                    exc=exc,
                    extra={"connection_id": connection_id, "question": question, "context_query_id": context_query_id},
                )
                error_type, user_message, error_suggestion = build_user_friendly_error(str(exc))
                await self._emit(steps, step_handler, "done", "failed")
                telemetry.llm_cache_hit = bool(
                    getattr(self.query_rewriter, "last_cache_hit", False)
                    or getattr(self.sql_generator, "last_cache_hit", False)
                    or getattr(self.error_reflector, "last_cache_hit", False)
                )
                response = QueryResult(
                    question=question,
                    rewritten_query=question_for_model,
                    retrieved_tables=[],
                    sql="",
                    result=None,
                    chart=None,
                    steps=steps,
                    status="failed",
                    retry_count=0,
                    llm_latency_ms=round(llm_latency, 2),
                    db_latency_ms=None,
                    error_message=user_message,
                    error_type=error_type,
                    error_suggestion=error_suggestion,
                    context_source_query_id=context_query_id,
                    telemetry=telemetry,
                )
                history_repo.save_query_result(connection_id, response)
                log_query_audit(
                    "nl2sql.query.failed",
                    extra={
                        "connection_id": connection_id,
                        "cache_key": cache_key,
                        "error_type": error_type,
                        "error_message": user_message,
                        "elapsed_ms": round((time.perf_counter() - started_at) * 1000.0, 2),
                    },
                )
                return response
            finally:
                self._active_queries = max(0, self._active_queries - 1)

    async def _emit(self, steps: list[AgentStep], step_handler: StepHandler | None, step_type: str, content: str, metadata: dict | None = None) -> None:
        step = AgentStep(step_type=step_type, content=content, metadata=metadata or {})
        steps.append(step)
        if step_handler is not None:
            maybe = step_handler(step)
            if maybe is not None:
                await maybe

    def _execute_sql(
        self,
        connector,
        sql: str,
        *,
        page_number: int,
        page_size: int | None,
        include_total_count: bool,
    ):
        signature = inspect.signature(self.sql_executor.execute)
        if "page_number" in signature.parameters:
            return self.sql_executor.execute(
                connector,
                sql,
                page_number=page_number,
                page_size=page_size,
                include_total_count=include_total_count,
            )
        return self.sql_executor.execute(connector, sql)

    def cache_stats(self) -> dict[str, float | int]:
        total = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total if total else 0.0
        return {
            "entries": len(self._cache),
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "hit_rate": round(hit_rate, 4),
            "active_queries": self._active_queries,
            "peak_active_queries": self._peak_active_queries,
        }

    async def _cache_get(self, cache_key, step_handler: StepHandler | None) -> QueryResult | None:
        record = self._cache.get(cache_key)
        if record is None:
            self._cache_misses += 1
            return None
        cached_at, result = record
        if time.time() - cached_at > self.db_manager.settings.query_cache_ttl_seconds:
            self._cache.pop(cache_key, None)
            self._cache_misses += 1
            return None
        self._cache_hits += 1
        cached_result = self._clone_result(result)
        cached_result.telemetry.cache_hit = True
        cached_result.telemetry.cache_key = cache_key
        cache_step = AgentStep(
            step_type="sql_execution",
            content="Cache hit: reused previous result.",
            metadata={"cache": True, "cache_key": cache_key},
        )
        cached_result.steps.append(cache_step)
        if step_handler is not None:
            maybe = step_handler(
                cache_step
            )
            if inspect.isawaitable(maybe):
                await maybe
        return cached_result

    def _cache_put(self, cache_key, result: QueryResult) -> None:
        self._cache[cache_key] = (time.time(), self._clone_result(result))

    def _resolve_follow_up_context(
        self,
        *,
        connection_id: str,
        question: str,
        previous_query_id: str | None,
        follow_up_instruction: str | None,
    ) -> tuple[str | None, str]:
        if not previous_query_id and not follow_up_instruction:
            return None, question

        history_repo = QueryHistoryRepository(self.db_manager.metadata_db)
        histories = history_repo.list_history(limit=1000)
        previous: QueryHistoryItem | None = None

        if previous_query_id:
            previous = next((item for item in histories if item.id == previous_query_id and item.connection_id == connection_id), None)
        if previous is None and (follow_up_instruction or previous_query_id):
            previous = next((item for item in histories if item.connection_id == connection_id), None)

        if previous is None:
            return None, question

        context_text = build_follow_up_prompt_context(
            question=question,
            previous_question=previous.question,
            previous_sql=previous.sql,
            previous_tables=previous.retrieved_tables,
            instruction=follow_up_instruction,
        )
        return previous.id, context_text
