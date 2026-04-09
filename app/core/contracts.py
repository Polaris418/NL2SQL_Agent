from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


class DBType(str, Enum):
    postgresql = "postgresql"
    mysql = "mysql"
    sqlite = "sqlite"


class QueryStatus(str, Enum):
    success = "success"
    failed = "failed"
    partial = "partial"


class ChartType(str, Enum):
    bar = "bar"
    line = "line"
    pie = "pie"
    table = "table"


class ConnectionCreateRequest(BaseModel):
    name: str = Field(min_length=1)
    db_type: DBType
    host: str | None = None
    port: int | None = None
    username: str | None = None
    password: str | None = None
    database: str = Field(min_length=1)
    sqlite_path: str | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _validate_fields(self) -> "ConnectionCreateRequest":
        if self.db_type in {DBType.postgresql, DBType.mysql}:
            missing = [
                field
                for field in ("host", "port", "username", "password")
                if getattr(self, field) in (None, "")
            ]
            if missing:
                raise ValueError(f"Missing required connection fields: {', '.join(missing)}")
        if self.db_type == DBType.sqlite and not self.sqlite_path:
            raise ValueError("sqlite_path is required for SQLite connections")
        return self


class ConnectionRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    db_type: DBType
    host: str | None = None
    port: int | None = None
    username: str | None = None
    database: str
    status: Literal["online", "offline", "unknown"] = "unknown"
    is_active: bool = True
    description: str | None = None
    warning: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ConnectionTestResult(BaseModel):
    connection_id: str
    success: bool
    message: str
    latency_ms: int = 0
    tested_at: datetime = Field(default_factory=datetime.utcnow)


class SchemaColumn(BaseModel):
    name: str
    type: str
    comment: str | None = None
    primary_key: bool = False
    foreign_key: str | None = None


class SchemaTable(BaseModel):
    table_name: str
    table_comment: str | None = None
    columns: list[SchemaColumn] = Field(default_factory=list)
    description: str | None = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class SchemaRefreshResult(BaseModel):
    connection_id: str
    success: bool
    tables: list[SchemaTable] = Field(default_factory=list)
    message: str | None = None


class QueryRequest(BaseModel):
    connection_id: str
    question: str = Field(min_length=1)
    context_history: list[str] = Field(default_factory=list)


class QueryStep(BaseModel):
    type: Literal["rewrite", "retrieve", "generate", "execute", "reflect"]
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class QueryResultRowSet(BaseModel):
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0


class ChartSuggestion(BaseModel):
    type: ChartType = ChartType.table
    x_field: str | None = None
    y_field: str | None = None
    label_field: str | None = None


class QueryLatency(BaseModel):
    llm_ms: int = 0
    db_ms: int = 0


class QueryResponse(BaseModel):
    query_id: str
    status: QueryStatus
    sql: str
    sql_attempts: int = 1
    results: QueryResultRowSet
    chart_suggestion: ChartSuggestion = Field(default_factory=ChartSuggestion)
    steps: list[QueryStep] = Field(default_factory=list)
    latency: QueryLatency = Field(default_factory=QueryLatency)
    warning: str | None = None


class QueryHistoryItem(BaseModel):
    query_id: str
    connection_id: str
    user_question: str
    rewritten_query: str | None = None
    retrieved_tables: list[str] = Field(default_factory=list)
    final_sql: str | None = None
    status: QueryStatus
    sql_attempts: int = 1
    llm_latency_ms: int = 0
    db_latency_ms: int = 0
    row_count: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)


class QueryHistoryDetail(QueryHistoryItem):
    results: QueryResultRowSet = Field(default_factory=QueryResultRowSet)
    steps: list[QueryStep] = Field(default_factory=list)
    error_message: str | None = None
    chart_suggestion: ChartSuggestion = Field(default_factory=ChartSuggestion)


class ErrorDistributionItem(BaseModel):
    error_type: str
    count: int
    percentage: float


class TopTableItem(BaseModel):
    table_name: str
    query_count: int


class AnalyticsSummary(BaseModel):
    total_queries: int
    recent_query_count: int
    success_rate: float
    average_llm_latency_ms: float
    average_db_latency_ms: float


class AnalyticsReport(BaseModel):
    summary: AnalyticsSummary
    errors: list[ErrorDistributionItem] = Field(default_factory=list)
    top_tables: list[TopTableItem] = Field(default_factory=list)


class StreamEvent(BaseModel):
    event: Literal["step", "result", "error", "done"]
    data: dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
