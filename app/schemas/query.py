from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .base import SchemaModel
from .connection import TableSchema
from .pydantic_compat import Field


class QueryStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class ChartType(str, Enum):
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    TABLE = "table"


class QueryRequest(SchemaModel):
    question: str
    connection_id: str
    stream: bool = False
    page_number: int = 1
    page_size: int = 1000
    previous_query_id: str | None = None
    follow_up_instruction: str | None = None
    include_total_count: bool = False


class SQLExecutionRequest(SchemaModel):
    connection_id: str
    sql: str
    page_number: int = 1
    page_size: int = 1000
    include_total_count: bool = False


class PaginationInfo(SchemaModel):
    page_number: int = 1
    page_size: int = 1000
    offset: int = 0
    has_more: bool = False
    total_row_count: int | None = None
    applied_limit: int | None = None


class ExecutionResult(SchemaModel):
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    db_latency_ms: float | None = None
    total_row_count: int | None = None
    pagination: PaginationInfo | None = None
    source_sql: str | None = None


class ChartSuggestion(SchemaModel):
    chart_type: ChartType = ChartType.TABLE
    x_axis: str | None = None
    y_axis: str | None = None
    reason: str | None = None


class AgentStep(SchemaModel):
    step_type: str
    content: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)


class QueryHistoryItem(SchemaModel):
    id: str
    connection_id: str
    question: str
    rewritten_query: str | None = None
    retrieved_tables: list[str] = Field(default_factory=list)
    sql: str | None = None
    status: QueryStatus
    retry_count: int = 0
    llm_latency_ms: float | None = None
    db_latency_ms: float | None = None
    error_message: str | None = None
    error_type: str | None = None
    error_suggestion: str | None = None
    result_row_count: int | None = None
    context_source_query_id: str | None = None
    created_at: datetime


class QueryHistoryDetail(QueryHistoryItem):
    steps: list[AgentStep] = Field(default_factory=list)
    result_row_count: int | None = None
    result: ExecutionResult | None = None
    chart: ChartSuggestion | None = None
    retrieved_table_details: list[TableSchema] = Field(default_factory=list)
    telemetry: QueryTelemetry | None = None
    follow_up_context: str | None = None


class QueryTelemetry(SchemaModel):
    cache_hit: bool = False
    cache_key: str | None = None
    queue_wait_ms: float | None = None
    retrieval_latency_ms: float | None = None
    active_concurrency: int | None = None
    peak_concurrency: int | None = None
    schema_fingerprint: str | None = None
    schema_table_count: int | None = None
    llm_cache_hit: bool = False
    retrieval_backend: str | None = None
    embedding_backend: str | None = None
    retrieval_scope_count: int | None = None
    retrieval_candidates: int | None = None
    retrieval_selected: int | None = None
    retrieval_lexical_count: int | None = None
    retrieval_vector_count: int | None = None
    retrieval_top_score: float | None = None
    relationship_count: int | None = None
    relationship_tables: list[str] = Field(default_factory=list)
    column_annotation_count: int | None = None
    packed_context_tables: int | None = None
    packed_context_chars: int | None = None
    packed_context_truncated: bool = False
    packed_context_tokens: int | None = None
    packed_context_budget: dict[str, Any] = Field(default_factory=dict)
    packed_context_limit_reason: str | None = None
    packed_context_dropped_tables: list[str] = Field(default_factory=list)
    packed_context_dropped_columns: dict[str, list[str]] = Field(default_factory=dict)
    packed_context_dropped_relationship_clues: list[dict[str, Any]] = Field(default_factory=list)
    few_shot_example_ids: list[str] = Field(default_factory=list)
    tenant_isolation_key: str | None = None
    audit_id: str | None = None


class FollowUpContext(SchemaModel):
    history_id: str
    question: str
    connection_id: str
    rewritten_query: str | None = None
    retrieved_tables: list[str] = Field(default_factory=list)
    final_sql: str | None = None
    steps: list[AgentStep] = Field(default_factory=list)
    context_text: str


class QueryResult(SchemaModel):
    question: str
    rewritten_query: str
    retrieved_tables: list[TableSchema] = Field(default_factory=list)
    sql: str
    result: ExecutionResult | None = None
    chart: ChartSuggestion | None = None
    summary: str | None = None
    steps: list[AgentStep] = Field(default_factory=list)
    status: QueryStatus
    retry_count: int = 0
    llm_latency_ms: float | None = None
    db_latency_ms: float | None = None
    error_message: str | None = None
    error_type: str | None = None
    error_suggestion: str | None = None
    context_source_query_id: str | None = None
    telemetry: QueryTelemetry = Field(default_factory=QueryTelemetry)


class AnalyticsSummary(SchemaModel):
    recent_success_rate: float = 0.0
    average_llm_latency_ms: float = 0.0
    average_db_latency_ms: float = 0.0
    top_tables: list[dict[str, Any]] = Field(default_factory=list)
    error_distribution: list[dict[str, Any]] = Field(default_factory=list)


class TopTableItem(SchemaModel):
    table: str
    count: int


class ErrorDistributionItem(SchemaModel):
    type: str
    count: int
    percentage: float


class AnalyticsReport(SchemaModel):
    summary: AnalyticsSummary
    errors: list[ErrorDistributionItem] = Field(default_factory=list)
    top_tables: list[TopTableItem] = Field(default_factory=list)
