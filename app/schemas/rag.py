from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .base import SchemaModel
from .pydantic_compat import Field


class RAGIndexStatus(str, Enum):
    PENDING = "pending"
    INDEXING = "indexing"
    READY = "ready"
    FAILED = "failed"


class RAGHealthStatus(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class RAGIndexMode(str, Enum):
    HYBRID = "hybrid"
    BM25_ONLY = "bm25_only"
    VECTOR = "vector"


class SchemaVersionResponse(SchemaModel):
    connection_id: str
    schema_version: str | None = None
    table_count: int = 0
    updated_at: str | None = None


class SchemaVersionRecordResponse(SchemaModel):
    connection_id: str
    version: str
    schema_fingerprint: str
    table_count: int = 0
    table_fingerprints: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class SchemaVersionDiffResponse(SchemaModel):
    connection_id: str
    left_version: str | None = None
    right_version: str | None = None
    added_tables: list[str] = Field(default_factory=list)
    removed_tables: list[str] = Field(default_factory=list)
    changed_tables: list[str] = Field(default_factory=list)
    unchanged_tables: list[str] = Field(default_factory=list)


class RAGIndexState(SchemaModel):
    connection_id: str
    schema_version: str | None = None
    index_status: RAGIndexStatus = RAGIndexStatus.PENDING
    health_status: RAGHealthStatus = RAGHealthStatus.UNKNOWN
    index_mode: RAGIndexMode = RAGIndexMode.HYBRID
    is_indexed: bool = False
    table_count: int = 0
    vector_count: int = 0
    last_started_at: str | None = None
    last_completed_at: str | None = None
    last_success_at: str | None = None
    last_error: str | None = None
    last_forced_rebuild: bool = False
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class RAGIndexJob(SchemaModel):
    id: str
    connection_id: str
    schema_version: str | None = None
    action: str
    status: str
    force_full_rebuild: bool = False
    started_at: str
    completed_at: str | None = None
    duration_ms: float | None = None
    error_message: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class RAGIndexHealthDetail(SchemaModel):
    state: RAGIndexState
    current_job: RAGIndexJob | None = None
    latest_jobs: list[RAGIndexJob] = Field(default_factory=list)
    vector_store_available: bool = False
    bm25_enabled: bool = False
    degradation: dict[str, Any] = Field(default_factory=dict)
    async_snapshot: dict[str, Any] = Field(default_factory=dict)


class RAGDegradationSnapshot(SchemaModel):
    connection_id: str | None = None
    current_mode: str | None = None
    degradation_count: int = 0
    recovery_count: int = 0
    observation_count: int = 0
    degradation_rate: float = 0.0
    event_count: int = 0
    degraded_connections: int = 0
    total_connections: int = 0
    last_transition_at: str | None = None
    last_observed_at: str | None = None


class RAGDegradationEventRecord(SchemaModel):
    connection_id: str
    event_type: str
    previous_mode: str
    current_mode: str
    reason: str | None = None
    observed_healthy: bool | None = None
    created_at: str | None = None


class RAGQueryLogRecord(SchemaModel):
    query_id: str
    connection_id: str
    original_query: str
    rewritten_query: str | None = None
    expanded_query: str | None = None
    selected_tables: list[str] = Field(default_factory=list)
    candidate_scores: list[dict[str, Any]] = Field(default_factory=list)
    reranked_tables: list[str] = Field(default_factory=list)
    prompt_schema: str | None = None
    final_sql: str | None = None
    cache_hit: bool = False
    used_fallback: bool = False
    degradation_mode: str | None = None
    failure_category: str | None = None
    failure_stage: str | None = None
    retrieval_latency_ms: float | None = None
    stage_latencies: dict[str, float] = Field(default_factory=dict)
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class RAGIndexMetrics(SchemaModel):
    total_connections: int = 0
    indexed_connections: int = 0
    healthy_connections: int = 0
    unhealthy_connections: int = 0
    indexing_connections: int = 0
    pending_connections: int = 0
    failed_connections: int = 0
    average_table_count: float = 0.0
    average_vector_count: float = 0.0
    cache_entries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_evictions: int = 0
    cache_invalidations: int = 0
    cache_hit_rate: float = 0.0
    logged_queries: int = 0
    retrieval_p50_ms: float = 0.0
    retrieval_p95_ms: float = 0.0
    retrieval_p99_ms: float = 0.0
    embedding_p50_ms: float = 0.0
    embedding_p95_ms: float = 0.0
    embedding_p99_ms: float = 0.0
    vector_hit_rate: float = 0.0
    bm25_hit_rate: float = 0.0
    fallback_rate: float = 0.0
    table_not_found_rate: float = 0.0
    timeout_rate: float = 0.0
    concurrency_rejection_rate: float = 0.0
    degraded_connections: int = 0
    degradation_count: int = 0
    recovery_count: int = 0
    retrieval_timeout_count: int = 0
    queue_timeout_count: int = 0
    rejected_requests: int = 0
    peak_active_requests: int = 0
    current_degradation_mode: str | None = None
    failure_categories: dict[str, int] = Field(default_factory=dict)
    last_updated_at: str | None = None


class RAGHealthReport(SchemaModel):
    metrics: RAGIndexMetrics = Field(default_factory=RAGIndexMetrics)
    connections: list[RAGIndexState] = Field(default_factory=list)


class RAGIndexBuildRequest(SchemaModel):
    force_full_rebuild: bool = False


class RAGDebugSection(SchemaModel):
    title: str
    items: dict[str, Any] = Field(default_factory=dict)


class RAGDebugView(SchemaModel):
    query_id: str
    connection_id: str | None = None
    summary: dict[str, Any] = Field(default_factory=dict)
    sections: list[RAGDebugSection] = Field(default_factory=list)


class RAGAcceptanceThresholds(SchemaModel):
    minimum_recall_at_5: float = 0.7
    minimum_mrr: float = 0.5
    maximum_table_not_found_rate: float = 0.2


class RAGAcceptanceResult(SchemaModel):
    passed: bool = False
    checks: dict[str, bool] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    failures: list[str] = Field(default_factory=list)


class RAGStabilityResult(SchemaModel):
    passed: bool = False
    checks: dict[str, bool] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    failures: list[str] = Field(default_factory=list)


class RAGTelemetrySnapshot(SchemaModel):
    id: str | None = None
    logged_queries: int = 0
    metrics: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class RAGTelemetryDashboard(SchemaModel):
    current: dict[str, Any] = Field(default_factory=dict)
    latest_snapshot: RAGTelemetrySnapshot | None = None
    recent_events: list[dict[str, Any]] = Field(default_factory=list)
    history_count: int = 0
    store: dict[str, Any] = Field(default_factory=dict)
    context_limit_events: list[dict[str, Any]] = Field(default_factory=list)


class RAGTelemetryEventRecord(SchemaModel):
    id: str | None = None
    query_id: str
    connection_id: str
    retrieval_latency_ms: float = 0.0
    embedding_latency_ms: float = 0.0
    lexical_count: int = 0
    vector_count: int = 0
    cache_hit: bool = False
    used_fallback: bool = False
    error_type: str | None = None
    failure_stage: str | None = None
    selected_tables: list[str] = Field(default_factory=list)
    degradation_mode: str | None = None
    error_message: str | None = None
    retrieval_backend: str | None = None
    embedding_backend: str | None = None
    created_at: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class RAGTelemetrySummary(SchemaModel):
    metrics: dict[str, Any] = Field(default_factory=dict)
    recent_events: list[RAGTelemetryEventRecord] = Field(default_factory=list)


class RAGDebugCandidateScore(SchemaModel):
    table_name: str
    score: float = 0.0
    source: str | None = None
    rank: int | None = None
    source_scores: dict[str, float] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RAGDebugTiming(SchemaModel):
    retrieval_latency_ms: float | None = None
    embedding_latency_ms: float | None = None
    stage_latencies: dict[str, float] = Field(default_factory=dict)


class RAGDebugArtifacts(SchemaModel):
    prompt_schema: str | None = None
    final_sql: str | None = None


class RAGDebugQueryContext(SchemaModel):
    query_id: str
    connection_id: str | None = None
    original_query: str | None = None
    rewritten_query: str | None = None
    expanded_query: str | None = None
    applied_synonyms: list[tuple[str, str]] = Field(default_factory=list)
    selected_tables: list[str] = Field(default_factory=list)
    reranked_tables: list[str] = Field(default_factory=list)
    candidate_count: int = 0
    selected_count: int = 0


class RAGQueryDebugView(SchemaModel):
    query_id: str
    connection_id: str | None = None
    summary: str = ""
    query: RAGDebugQueryContext = Field(default_factory=lambda: RAGDebugQueryContext(query_id=""))
    candidates: list[RAGDebugCandidateScore] = Field(default_factory=list)
    timings: RAGDebugTiming = Field(default_factory=RAGDebugTiming)
    artifacts: RAGDebugArtifacts = Field(default_factory=RAGDebugArtifacts)
    failure_category: str | None = None
    failure_stage: str | None = None
    degradation_mode: str | None = None
    cache_hit: bool = False
    used_fallback: bool = False
    index_state: dict[str, Any] = Field(default_factory=dict)
    runtime_metrics: dict[str, Any] = Field(default_factory=dict)
    access_metadata: dict[str, Any] = Field(default_factory=dict)
    input_validation: dict[str, Any] = Field(default_factory=dict)
    raw_log: dict[str, Any] = Field(default_factory=dict)
