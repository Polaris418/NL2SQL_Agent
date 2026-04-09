from __future__ import annotations

import asyncio
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator
from uuid import uuid4

from app.core.contracts import (
    AnalyticsReport,
    AnalyticsSummary,
    ChartSuggestion,
    ChartType,
    ConnectionCreateRequest,
    ConnectionRecord,
    ConnectionTestResult,
    ErrorDistributionItem,
    QueryHistoryDetail,
    QueryHistoryItem,
    QueryLatency,
    QueryRequest,
    QueryResponse,
    QueryResultRowSet,
    QueryStatus,
    QueryStep,
    SchemaColumn,
    SchemaRefreshResult,
    SchemaTable,
    StreamEvent,
    TopTableItem,
)


def _now() -> datetime:
    return datetime.utcnow()


def _looks_like_time_field(name: str) -> bool:
    lowered = name.lower()
    return any(token in lowered for token in ("date", "time", "created", "updated", "day", "at"))


def _looks_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


@dataclass
class InMemoryState:
    connections: dict[str, ConnectionRecord] = field(default_factory=dict)
    schemas: dict[str, list[SchemaTable]] = field(default_factory=dict)
    histories: dict[str, QueryHistoryDetail] = field(default_factory=dict)
    steps: defaultdict[str, list[QueryStep]] = field(default_factory=lambda: defaultdict(list))


class FallbackConnectionService:
    def __init__(self, state: InMemoryState) -> None:
        self._state = state

    async def create_connection(self, request: ConnectionCreateRequest) -> ConnectionRecord:
        warning = "Fallback mode: connection was stored locally without probing a live database."
        record = ConnectionRecord(
            name=request.name,
            db_type=request.db_type,
            host=request.host,
            port=request.port,
            username=request.username,
            database=request.database,
            status="online",
            description=request.description,
            warning=warning,
        )
        self._state.connections[record.id] = record
        self._state.schemas[record.id] = self._default_schema(record)
        return record.model_copy(update={"updated_at": _now()})

    async def list_connections(self) -> list[ConnectionRecord]:
        return list(self._state.connections.values())

    async def delete_connection(self, connection_id: str) -> bool:
        removed = self._state.connections.pop(connection_id, None) is not None
        self._state.schemas.pop(connection_id, None)
        return removed

    async def test_connection(self, connection_id: str) -> ConnectionTestResult:
        exists = connection_id in self._state.connections
        return ConnectionTestResult(
            connection_id=connection_id,
            success=exists,
            message=(
                "Fallback connectivity check passed."
                if exists
                else "Connection was not found in the local registry."
            ),
            latency_ms=1,
        )

    async def refresh_schema(self, connection_id: str) -> SchemaRefreshResult:
        connection = self._state.connections.get(connection_id)
        if connection is None:
            return SchemaRefreshResult(
                connection_id=connection_id,
                success=False,
                message="Connection was not found in the local registry.",
            )
        tables = self._default_schema(connection)
        self._state.schemas[connection_id] = tables
        return SchemaRefreshResult(connection_id=connection_id, success=True, tables=tables)

    async def get_schema(self, connection_id: str) -> list[SchemaTable]:
        return self._state.schemas.get(connection_id, [])

    def _default_schema(self, connection: ConnectionRecord) -> list[SchemaTable]:
        if connection.db_type == "sqlite":
            return [
                SchemaTable(
                    table_name="notes",
                    table_comment="Local fallback note table",
                    description="Stores local note records for fallback development mode.",
                    columns=[
                        SchemaColumn(name="id", type="INTEGER", comment="Primary key", primary_key=True),
                        SchemaColumn(name="title", type="TEXT", comment="Note title"),
                        SchemaColumn(name="body", type="TEXT", comment="Note content"),
                        SchemaColumn(name="created_at", type="DATETIME", comment="Creation time"),
                    ],
                )
            ]
        return [
            SchemaTable(
                table_name="users",
                table_comment="User dimension table",
                description="Stores user profiles and basic demographic attributes.",
                columns=[
                    SchemaColumn(name="id", type="INT", comment="Primary key", primary_key=True),
                    SchemaColumn(name="name", type="VARCHAR", comment="User name"),
                    SchemaColumn(name="city", type="VARCHAR", comment="City"),
                    SchemaColumn(name="created_at", type="TIMESTAMP", comment="Creation time"),
                ],
            ),
            SchemaTable(
                table_name="orders",
                table_comment="Order fact table",
                description="Stores purchase orders and transaction amounts.",
                columns=[
                    SchemaColumn(name="id", type="INT", comment="Primary key", primary_key=True),
                    SchemaColumn(name="user_id", type="INT", comment="User id", foreign_key="users.id"),
                    SchemaColumn(name="city", type="VARCHAR", comment="Order city"),
                    SchemaColumn(name="total_amount", type="DECIMAL", comment="Order amount"),
                    SchemaColumn(name="created_at", type="TIMESTAMP", comment="Creation time"),
                ],
            ),
        ]


class FallbackHistoryService:
    def __init__(self, state: InMemoryState) -> None:
        self._state = state

    async def list_history(self, limit: int = 20, offset: int = 0) -> list[QueryHistoryItem]:
        items = sorted(self._state.histories.values(), key=lambda item: item.created_at, reverse=True)
        return items[offset : offset + limit]

    async def get_history(self, query_id: str) -> QueryHistoryDetail | None:
        return self._state.histories.get(query_id)

    async def save_history(self, record: QueryHistoryDetail) -> QueryHistoryDetail:
        self._state.histories[record.query_id] = record
        return record

    async def log_steps(self, query_id: str, steps: list[QueryStep]) -> None:
        self._state.steps[query_id] = steps
        if query_id in self._state.histories:
            self._state.histories[query_id] = self._state.histories[query_id].model_copy(update={"steps": steps})


class FallbackAnalyticsService:
    def __init__(self, history_service: FallbackHistoryService, connection_service: FallbackConnectionService) -> None:
        self._history_service = history_service
        self._connection_service = connection_service

    async def get_summary(self) -> AnalyticsReport:
        histories = await self._history_service.list_history(limit=1000, offset=0)
        recent = histories[:100]
        total = len(histories)
        success_count = sum(1 for item in recent if item.status == QueryStatus.success)
        avg_llm = sum(item.llm_latency_ms for item in recent) / len(recent) if recent else 0.0
        avg_db = sum(item.db_latency_ms for item in recent) / len(recent) if recent else 0.0
        summary = AnalyticsSummary(
            total_queries=total,
            recent_query_count=len(recent),
            success_rate=(success_count / len(recent) * 100.0) if recent else 0.0,
            average_llm_latency_ms=round(avg_llm, 2),
            average_db_latency_ms=round(avg_db, 2),
        )
        return AnalyticsReport(
            summary=summary,
            errors=await self.get_errors(),
            top_tables=await self.get_top_tables(),
        )

    async def get_errors(self) -> list[ErrorDistributionItem]:
        histories = await self._history_service.list_history(limit=1000, offset=0)
        error_counts: Counter[str] = Counter()
        for item in histories:
            if item.status != QueryStatus.success:
                error_counts["execution_error" if item.error_message else "unknown_error"] += 1
        total_errors = sum(error_counts.values())
        if total_errors == 0:
            return []
        return [
            ErrorDistributionItem(
                error_type=name,
                count=count,
                percentage=round(count / total_errors * 100.0, 2),
            )
            for name, count in error_counts.most_common()
        ]

    async def get_top_tables(self) -> list[TopTableItem]:
        histories = await self._history_service.list_history(limit=1000, offset=0)
        counts: Counter[str] = Counter()
        for item in histories:
            counts.update(item.retrieved_tables)
        return [TopTableItem(table_name=table, query_count=count) for table, count in counts.most_common(10)]


class FallbackQueryService:
    def __init__(
        self,
        state: InMemoryState,
        connection_service: FallbackConnectionService,
        history_service: FallbackHistoryService,
    ) -> None:
        self._state = state
        self._connection_service = connection_service
        self._history_service = history_service
        self._default_limit = 1000

    async def process_query(self, request: QueryRequest) -> QueryResponse:
        started = datetime.utcnow()
        steps: list[QueryStep] = []
        query_id = str(uuid4())

        rewritten = self._rewrite_question(request.question)
        steps.append(QueryStep(type="rewrite", content=rewritten))

        tables = await self._retrieve_tables(request.connection_id, rewritten)
        steps.append(QueryStep(type="retrieve", content=", ".join(tables) if tables else "No tables matched"))

        sql = self._generate_sql(tables, request.question)
        steps.append(QueryStep(type="generate", content=sql))

        result_rows = await self._execute_sql(request.connection_id, sql, tables, request.question)
        execute_content = f"Execution succeeded with {len(result_rows.rows)} row(s)"
        steps.append(QueryStep(type="execute", content=execute_content))

        latency_ms = max(int((datetime.utcnow() - started).total_seconds() * 1000), 1)
        response = QueryResponse(
            query_id=query_id,
            status=QueryStatus.success,
            sql=sql,
            sql_attempts=1,
            results=result_rows,
            chart_suggestion=self._suggest_chart(result_rows, request.question),
            steps=steps,
            latency=QueryLatency(llm_ms=max(latency_ms // 2, 1), db_ms=max(latency_ms // 3, 1)),
            warning="Fallback mode: query execution used synthetic data.",
        )
        await self._history_service.save_history(
            QueryHistoryDetail(
                query_id=query_id,
                connection_id=request.connection_id,
                user_question=request.question,
                rewritten_query=rewritten,
                retrieved_tables=tables,
                final_sql=sql,
                status=QueryStatus.success,
                sql_attempts=1,
                llm_latency_ms=response.latency.llm_ms,
                db_latency_ms=response.latency.db_ms,
                row_count=result_rows.row_count,
                results=result_rows,
                steps=steps,
                chart_suggestion=response.chart_suggestion,
            )
        )
        await self._history_service.log_steps(query_id, steps)
        return response

    async def stream_query(self, request: QueryRequest) -> AsyncIterator[StreamEvent]:
        started = datetime.utcnow()
        query_id = str(uuid4())
        rewritten = self._rewrite_question(request.question)
        yield StreamEvent(event="step", data={"type": "rewrite", "content": rewritten}).model_dump()
        await asyncio.sleep(0)

        tables = await self._retrieve_tables(request.connection_id, rewritten)
        yield StreamEvent(
            event="step",
            data={"type": "retrieve", "content": ", ".join(tables) if tables else "No tables matched"},
        ).model_dump()
        await asyncio.sleep(0)

        sql = self._generate_sql(tables, request.question)
        yield StreamEvent(event="step", data={"type": "generate", "content": sql}).model_dump()
        await asyncio.sleep(0)

        result_rows = await self._execute_sql(request.connection_id, sql, tables, request.question)
        yield StreamEvent(
            event="step",
            data={"type": "execute", "content": f"Execution succeeded with {len(result_rows.rows)} row(s)"},
        ).model_dump()
        await asyncio.sleep(0)

        chart = self._suggest_chart(result_rows, request.question)
        latency_ms = max(int((datetime.utcnow() - started).total_seconds() * 1000), 1)
        query_record = QueryHistoryDetail(
            query_id=query_id,
            connection_id=request.connection_id,
            user_question=request.question,
            rewritten_query=rewritten,
            retrieved_tables=tables,
            final_sql=sql,
            status=QueryStatus.success,
            sql_attempts=1,
            llm_latency_ms=max(latency_ms // 2, 1),
            db_latency_ms=max(latency_ms // 3, 1),
            row_count=result_rows.row_count,
            results=result_rows,
            steps=[
                QueryStep(type="rewrite", content=rewritten),
                QueryStep(type="retrieve", content=", ".join(tables) if tables else "No tables matched"),
                QueryStep(type="generate", content=sql),
                QueryStep(type="execute", content=f"Execution succeeded with {len(result_rows.rows)} row(s)"),
            ],
            chart_suggestion=chart,
        )
        await self._history_service.save_history(query_record)
        yield StreamEvent(
            event="result",
            data={"query_id": query_id, "results": result_rows.model_dump(), "chart_suggestion": chart.model_dump()},
        ).model_dump()
        yield StreamEvent(event="done", data={"query_id": query_id}).model_dump()

    async def _retrieve_tables(self, connection_id: str, rewritten_query: str) -> list[str]:
        tables = await self._connection_service.get_schema(connection_id)
        scored: list[tuple[int, str]] = []
        lowered_query = rewritten_query.lower()
        for table in tables:
            score = 0
            haystack = " ".join(
                [
                    table.table_name,
                    table.table_comment or "",
                    table.description or "",
                    " ".join(column.name for column in table.columns),
                ]
            ).lower()
            for token in re.findall(r"[a-zA-Z0-9_]+", lowered_query):
                if token in haystack:
                    score += 1
            for token in re.findall(r"[\u4e00-\u9fff]+", rewritten_query):
                if token and token in haystack:
                    score += 1
            if score:
                scored.append((score, table.table_name))
        if scored:
            scored.sort(key=lambda item: (-item[0], item[1]))
            return [name for _, name in scored[:10]]
        return [table.table_name for table in tables[:5]]

    def _rewrite_question(self, question: str) -> str:
        cleaned = re.sub(r"\s+", " ", question).strip()
        tokens = re.findall(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]{2,}", cleaned)
        if not tokens:
            return cleaned
        return " ".join(tokens[:20])

    def _generate_sql(self, tables: list[str], question: str) -> str:
        if not tables:
            return f"SELECT 1 AS result LIMIT {self._default_limit}"
        table = tables[0]
        lowered = question.lower()
        if any(token in lowered for token in ("count", "数量", "多少", "总数", "几")):
            return f"SELECT COUNT(*) AS count FROM {table} LIMIT {self._default_limit}"
        return f"SELECT * FROM {table} LIMIT {self._default_limit}"

    async def _execute_sql(
        self,
        connection_id: str,
        sql: str,
        tables: list[str],
        question: str,
    ) -> QueryResultRowSet:
        schema = await self._connection_service.get_schema(connection_id)
        selected_table = tables[0] if tables else (schema[0].table_name if schema else "result")
        table_schema = next((item for item in schema if item.table_name == selected_table), None)
        if table_schema and table_schema.columns:
            columns = [column.name for column in table_schema.columns[:4]]
        else:
            columns = ["result"]
        rows = self._sample_rows(selected_table, columns, question)
        return QueryResultRowSet(columns=columns, rows=rows, row_count=len(rows))

    def _sample_rows(self, table_name: str, columns: list[str], question: str) -> list[list[Any]]:
        base_rows = {
            "users": [
                {"id": 1, "name": "Alice", "city": "Shanghai", "created_at": "2025-03-01"},
                {"id": 2, "name": "Bob", "city": "Beijing", "created_at": "2025-03-03"},
                {"id": 3, "name": "Cathy", "city": "Shenzhen", "created_at": "2025-03-05"},
            ],
            "orders": [
                {"id": 101, "user_id": 1, "city": "Shanghai", "total_amount": 189.5, "created_at": "2025-03-11"},
                {"id": 102, "user_id": 2, "city": "Beijing", "total_amount": 268.0, "created_at": "2025-03-12"},
                {"id": 103, "user_id": 1, "city": "Shanghai", "total_amount": 98.0, "created_at": "2025-03-14"},
            ],
            "notes": [
                {"id": 1, "title": "Fallback note", "body": question[:60], "created_at": "2025-03-01"},
            ],
        }
        source_rows = base_rows.get(table_name, [{"result": question}])
        rows: list[list[Any]] = []
        for row in source_rows[:5]:
            rows.append([_json_safe(row.get(column)) for column in columns])
        if not rows:
            rows = [[question]]
        return rows

    def _suggest_chart(self, result: QueryResultRowSet, question: str) -> ChartSuggestion:
        if not result.columns:
            return ChartSuggestion(type=ChartType.table)
        lower_question = question.lower()
        if any(word in lower_question for word in ("trend", "trend", "时间", "趋势", "最近", "daily", "month")):
            return ChartSuggestion(type=ChartType.line, x_field=result.columns[0], y_field=result.columns[-1])
        if len(result.columns) >= 2 and any(_looks_numeric(row[-1]) for row in result.rows if row):
            return ChartSuggestion(type=ChartType.bar, x_field=result.columns[0], y_field=result.columns[-1])
        if "ratio" in lower_question or "占比" in lower_question:
            return ChartSuggestion(type=ChartType.pie, label_field=result.columns[0], y_field=result.columns[-1])
        return ChartSuggestion(type=ChartType.table)


class FallbackServiceRegistry:
    def __init__(self) -> None:
        self.state = InMemoryState()
        self.connection_service = FallbackConnectionService(self.state)
        self.history_service = FallbackHistoryService(self.state)
        self.analytics_service = FallbackAnalyticsService(self.history_service, self.connection_service)
        self.query_service = FallbackQueryService(self.state, self.connection_service, self.history_service)

    async def seed_demo_connection(self) -> ConnectionRecord:
        if self.state.connections:
            return next(iter(self.state.connections.values()))
        return await self.connection_service.create_connection(
            ConnectionCreateRequest(
                name="Demo Connection",
                db_type="sqlite",
                database="demo",
                sqlite_path="./data/demo.sqlite",
            )
        )


def format_sse_event(event: dict[str, Any]) -> str:
    payload = json.dumps(event, ensure_ascii=False, default=_json_safe)
    event_type = event.get("event", "message")
    return f"event: {event_type}\ndata: {payload}\n\n"
