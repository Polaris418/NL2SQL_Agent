from __future__ import annotations

from collections import Counter
from typing import Any

from app.agent.utils import categorize_error_message
from app.db.repositories.base import RepositoryBase
from app.db.repositories.rag_query_log_repo import RAGQueryLogRepository
from app.db.repositories.rag_telemetry_repo import RAGTelemetryRepository
from app.schemas.query import (
    AgentStep,
    AnalyticsReport,
    AnalyticsSummary,
    ErrorDistributionItem,
    FollowUpContext,
    QueryHistoryDetail,
    QueryHistoryItem,
    QueryResult,
    QueryStatus,
    TopTableItem,
)


class QueryHistoryRepository(RepositoryBase):
    def save_query_result(self, connection_id: str, result: QueryResult) -> str:
        query_id = self.utcnow().replace(":", "").replace("-", "").replace(".", "")
        result_payload = self._serialize_payload(result.result)
        chart_payload = self._serialize_payload(result.chart)
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO query_history (
                    id, connection_id, question, rewritten_query, retrieved_tables_json,
                    retrieved_table_details_json, sql_text, result_json, chart_json, telemetry_json,
                    result_row_count, status, retry_count,
                    llm_latency_ms, db_latency_ms, error_message, error_type, error_suggestion,
                    context_source_query_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    query_id,
                    connection_id,
                    result.question,
                    result.rewritten_query,
                    self.dumps([table.name for table in result.retrieved_tables]),
                    self.dumps([table.model_dump(mode="json") for table in result.retrieved_tables]),
                    result.sql,
                    self.dumps(result_payload) if result_payload is not None else None,
                    self.dumps(chart_payload) if chart_payload is not None else None,
                    self.dumps(result.telemetry.model_dump(mode="json")) if result.telemetry is not None else None,
                    result.result.row_count if result.result else None,
                    result.status.value if hasattr(result.status, "value") else result.status,
                    result.retry_count,
                    result.llm_latency_ms,
                    result.db_latency_ms,
                    result.error_message,
                    result.error_type,
                    result.error_suggestion,
                    result.context_source_query_id,
                    self.utcnow(),
                ),
            )
            for step in result.steps:
                conn.execute(
                    """
                    INSERT INTO agent_steps (query_history_id, step_type, content, timestamp, metadata_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        query_id,
                        step.step_type,
                        step.content,
                        step.timestamp.isoformat() if hasattr(step.timestamp, "isoformat") else str(step.timestamp),
                        self.dumps(step.metadata),
                    ),
                )
        return query_id

    def list_history(self, limit: int = 50, offset: int = 0) -> list[QueryHistoryItem]:
        rows = self.fetch_all(
            "SELECT * FROM query_history ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        return [self._row_to_item(row) for row in rows]

    def get_history(self, query_id: str) -> QueryHistoryDetail | None:
        row = self.fetch_one("SELECT * FROM query_history WHERE id = ?", (query_id,))
        if row is None:
            return None
        return self._row_to_detail(row)

    def list_steps(self, query_id: str) -> list[AgentStep]:
        rows = self.fetch_all(
            "SELECT * FROM agent_steps WHERE query_history_id = ? ORDER BY timestamp ASC",
            (query_id,),
        )
        return [
            AgentStep(
                step_type=row["step_type"],
                content=row["content"],
                timestamp=row["timestamp"],
                metadata=self.loads(row.get("metadata_json"), default={}),
            )
            for row in rows
        ]

    def build_follow_up_context(self, query_id: str) -> FollowUpContext | None:
        detail = self.get_history(query_id)
        if detail is None:
            return None
        context_text = self._compose_context_text(detail)
        return FollowUpContext(
            history_id=detail.id,
            question=detail.question,
            connection_id=detail.connection_id,
            rewritten_query=detail.rewritten_query,
            retrieved_tables=detail.retrieved_tables,
            final_sql=detail.sql,
            steps=detail.steps,
            context_text=context_text,
        )

    def top_tables(self, limit: int = 10, window: int = 100) -> list[TopTableItem]:
        rows = self.fetch_all("SELECT * FROM query_history ORDER BY created_at DESC LIMIT ?", (window,))
        counts = Counter(table for row in rows for table in self.loads(row.get("retrieved_tables_json"), default=[]))
        return [TopTableItem(table=table, count=count) for table, count in counts.most_common(limit)]

    def error_distribution(self, window: int = 100) -> list[ErrorDistributionItem]:
        rows = self.fetch_all("SELECT * FROM query_history ORDER BY created_at DESC LIMIT ?", (window,))
        counts = Counter(
            row.get("error_type") or self._error_type(row.get("error_message"))
            for row in rows
            if row.get("error_message") or row.get("error_type")
        )
        total = sum(counts.values()) or 1
        return [
            ErrorDistributionItem(type=error_type, count=count, percentage=round(count / total, 4))
            for error_type, count in counts.items()
        ]

    def analytics(self, window: int = 100) -> AnalyticsSummary:
        history = self.list_history(limit=window)
        if not history:
            return AnalyticsSummary()
        success_rate = sum(1 for item in history if item.status == QueryStatus.SUCCESS) / len(history)
        llm_values = [item.llm_latency_ms for item in history if item.llm_latency_ms is not None]
        db_values = [item.db_latency_ms for item in history if item.db_latency_ms is not None]
        return AnalyticsSummary(
            recent_success_rate=round(success_rate, 4),
            average_llm_latency_ms=round(sum(llm_values) / len(llm_values), 2) if llm_values else 0.0,
            average_db_latency_ms=round(sum(db_values) / len(db_values), 2) if db_values else 0.0,
            top_tables=[item.model_dump(mode="json") for item in self.top_tables(limit=10, window=window)],
            error_distribution=[item.model_dump(mode="json") for item in self.error_distribution(window=window)],
        )

    def analytics_report(self, window: int = 100) -> AnalyticsReport:
        return AnalyticsReport(
            summary=self.analytics(window=window),
            errors=self.error_distribution(window=window),
            top_tables=self.top_tables(limit=10, window=window),
        )

    def delete_history(self, query_id: str) -> bool:
        """删除单条查询历史记录及其关联的步骤"""
        with self.metadata_db.connect() as conn:
            # 先删除关联的 agent_steps
            conn.execute("DELETE FROM agent_steps WHERE query_history_id = ?", (query_id,))
            # 删除查询历史记录
            cursor = conn.execute("DELETE FROM query_history WHERE id = ?", (query_id,))
            return cursor.rowcount > 0

    def delete_all_history(self) -> int:
        """删除所有查询历史记录"""
        with self.metadata_db.connect() as conn:
            # 先删除所有 agent_steps
            conn.execute("DELETE FROM agent_steps")
            # 删除所有查询历史记录
            cursor = conn.execute("DELETE FROM query_history")
            return cursor.rowcount

    @staticmethod
    def _error_type(message: str | None) -> str:
        return categorize_error_message(message)

    def _row_to_item(self, row: dict[str, Any]) -> QueryHistoryItem:
        return QueryHistoryItem(
            id=row["id"],
            connection_id=row["connection_id"],
            question=row["question"],
            rewritten_query=row.get("rewritten_query"),
            retrieved_tables=self.loads(row.get("retrieved_tables_json"), default=[]),
            sql=row.get("sql_text"),
            status=row.get("status", QueryStatus.PARTIAL),
            retry_count=row.get("retry_count", 0),
            llm_latency_ms=row.get("llm_latency_ms"),
            db_latency_ms=row.get("db_latency_ms"),
            error_message=row.get("error_message"),
            error_type=row.get("error_type"),
            error_suggestion=row.get("error_suggestion"),
            result_row_count=row.get("result_row_count") or self._result_row_count(row),
            context_source_query_id=row.get("context_source_query_id"),
            created_at=row["created_at"],
        )

    def _row_to_detail(self, row: dict[str, Any]) -> QueryHistoryDetail:
        item = self._row_to_item(row)
        steps = self.list_steps(item.id)
        result_payload = self.loads(row.get("result_json"), default=None)
        chart_payload = self.loads(row.get("chart_json"), default=None)
        retrieved_detail_payload = self.loads(row.get("retrieved_table_details_json"), default=[])
        telemetry_payload = self.loads(row.get("telemetry_json"), default=None)
        if telemetry_payload is None:
            telemetry_payload = self._telemetry_fallback(item.id)
        follow_up_context = self._compose_context_text(item, steps=steps)
        return QueryHistoryDetail(
            **item.model_dump(mode="python"),
            steps=steps,
            result=result_payload,
            chart=chart_payload,
            retrieved_table_details=retrieved_detail_payload or [],
            telemetry=telemetry_payload,
            follow_up_context=follow_up_context,
        )

    def _compose_context_text(
        self,
        item: QueryHistoryItem,
        *,
        steps: list[AgentStep] | None = None,
    ) -> str:
        step_lines = []
        for step in steps or []:
            step_lines.append(f"- {step.step_type}: {step.content}")
        tables = ", ".join(item.retrieved_tables) if item.retrieved_tables else "none"
        pieces = [
            f"Question: {item.question}",
            f"Rewritten query: {item.rewritten_query or 'n/a'}",
            f"Retrieved tables: {tables}",
            f"Previous SQL: {item.sql or 'n/a'}",
            f"Final SQL: {item.sql or 'n/a'}",
            f"Status: {item.status.value if hasattr(item.status, 'value') else item.status}",
            f"Retry count: {item.retry_count}",
        ]
        if item.result_row_count is not None:
            pieces.append(f"Result rows: {item.result_row_count}")
        result_preview = self._result_preview(item.id)
        if result_preview:
            pieces.append("Result preview:\n" + result_preview)
        if step_lines:
            pieces.append("Steps:\n" + "\n".join(step_lines))
        return "\n".join(pieces)

    @staticmethod
    def _serialize_payload(payload: Any) -> Any:
        if payload is None:
            return None
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        if isinstance(payload, dict):
            return payload
        if hasattr(payload, "__dict__"):
            return dict(vars(payload))
        return payload

    def _result_row_count(self, row: dict[str, Any]) -> int | None:
        result_payload = self.loads(row.get("result_json"), default=None)
        if isinstance(result_payload, dict):
            row_count = result_payload.get("row_count")
            if isinstance(row_count, int):
                return row_count
        return None

    def _result_preview(self, query_id: str) -> str | None:
        row = self.fetch_one("SELECT result_json FROM query_history WHERE id = ?", (query_id,))
        if row is None:
            return None
        result_payload = self.loads(row.get("result_json"), default=None)
        if not isinstance(result_payload, dict):
            return None
        rows = result_payload.get("rows")
        if not isinstance(rows, list) or not rows:
            return None
        preview_lines = []
        for index, item in enumerate(rows[:5], start=1):
            preview_lines.append(f"{index}. {self.dumps(item)}")
        if len(rows) > 5:
            preview_lines.append(f"... and {len(rows) - 5} more row(s)")
        return "\n".join(preview_lines)

    def _telemetry_fallback(self, query_id: str) -> dict[str, Any] | None:
        event = RAGTelemetryRepository(self.metadata_db).get_event(query_id)
        log = RAGQueryLogRepository(self.metadata_db).get(query_id)
        if event is None and log is None:
            return None

        payload = dict(event.get("payload") or {}) if event else {}
        context_limit_events = RAGTelemetryRepository(self.metadata_db).list_context_limits(query_id=query_id, limit=1)
        context_limit = context_limit_events[0] if context_limit_events else None

        telemetry: dict[str, Any] = {
            "cache_hit": bool(event.get("cache_hit")) if event else bool(log.get("cache_hit")) if log else False,
            "retrieval_backend": payload.get("retrieval_backend") if payload else (log.get("degradation_mode") if log else None),
            "embedding_backend": payload.get("embedding_backend") if payload else None,
            "retrieval_latency_ms": event.get("retrieval_latency_ms") if event else log.get("retrieval_latency_ms") if log else None,
            "retrieval_candidates": payload.get("retrieval_candidates") if payload else None,
            "retrieval_selected": len(log.get("selected_tables") or []) if log else payload.get("retrieval_selected"),
            "retrieval_top_score": payload.get("retrieval_top_score") if payload else None,
            "relationship_count": payload.get("relationship_count") if payload else None,
            "relationship_tables": payload.get("relationship_tables") if payload else [],
            "column_annotation_count": payload.get("column_annotation_count") if payload else None,
            "packed_context_tables": payload.get("packed_context_tables") if payload else None,
            "packed_context_chars": payload.get("packed_context_chars") if payload else None,
            "packed_context_tokens": payload.get("packed_context_tokens") if payload else None,
            "packed_context_truncated": bool(context_limit.get("truncated")) if context_limit else bool(payload.get("packed_context_truncated")) if payload else False,
            "packed_context_budget": context_limit.get("budget") if context_limit else payload.get("packed_context_budget", {}) if payload else {},
            "packed_context_limit_reason": context_limit.get("limit_reason") if context_limit else payload.get("packed_context_limit_reason") if payload else None,
            "packed_context_dropped_tables": context_limit.get("dropped_tables") if context_limit else payload.get("packed_context_dropped_tables", []) if payload else [],
            "packed_context_dropped_columns": context_limit.get("dropped_columns") if context_limit else payload.get("packed_context_dropped_columns", {}) if payload else {},
            "packed_context_dropped_relationship_clues": context_limit.get("dropped_relationship_clues") if context_limit else payload.get("packed_context_dropped_relationship_clues", []) if payload else [],
            "few_shot_example_ids": payload.get("few_shot_example_ids") if payload else [],
            "audit_id": log.get("query_id") if log else None,
        }
        return telemetry
