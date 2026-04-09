from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from app.schemas.rag import (
    RAGHealthReport,
    RAGHealthStatus,
    RAGIndexMetrics,
    RAGIndexMode,
    RAGIndexState,
    RAGIndexStatus,
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class IndexHealthSnapshot:
    connection_id: str
    schema_version: str | None = None
    table_count: int = 0
    vector_count: int = 0
    index_mode: RAGIndexMode = RAGIndexMode.HYBRID
    is_indexed: bool = False
    updated_at: str = field(default_factory=_utcnow)
    created_at: str = field(default_factory=_utcnow)


@dataclass(slots=True)
class IndexJobSnapshot:
    connection_id: str
    status: str
    started_at: str
    force_full_rebuild: bool = False
    schema_version: str | None = None
    completed_at: str | None = None
    error_message: str | None = None


class IndexHealthBuilder:
    """Build connection-level health snapshots and aggregate reports."""

    def build_state(
        self,
        connection_id: str,
        *,
        snapshot: IndexHealthSnapshot | dict[str, Any] | None = None,
        job: IndexJobSnapshot | dict[str, Any] | None = None,
        vector_store_available: bool = True,
        bm25_enabled: bool = True,
        last_error: str | None = None,
        index_mode: RAGIndexMode | str = RAGIndexMode.HYBRID,
        health_status: RAGHealthStatus | str | None = None,
    ) -> RAGIndexState:
        snapshot_data = self._normalize_snapshot(snapshot)
        job_data = self._normalize_job(job)
        job_status = str(job_data.get("status") or "").lower()
        has_snapshot = snapshot_data is not None
        is_indexed = bool(snapshot_data and snapshot_data.get("is_indexed"))

        inferred_index_status = RAGIndexStatus.PENDING
        inferred_health_status = RAGHealthStatus.UNKNOWN
        if job_status in {"running", "pending"}:
            inferred_index_status = RAGIndexStatus.INDEXING
            inferred_health_status = RAGHealthStatus.DEGRADED if has_snapshot else RAGHealthStatus.UNKNOWN
        elif job_status == "failed":
            inferred_index_status = RAGIndexStatus.FAILED
            inferred_health_status = RAGHealthStatus.DEGRADED if has_snapshot else RAGHealthStatus.UNHEALTHY
        elif has_snapshot:
            inferred_index_status = RAGIndexStatus.READY
            inferred_health_status = RAGHealthStatus.HEALTHY if (vector_store_available and bm25_enabled) else RAGHealthStatus.DEGRADED

        if health_status is not None:
            inferred_health_status = health_status if isinstance(health_status, RAGHealthStatus) else RAGHealthStatus(str(health_status))

        last_started_at = job_data.get("started_at") or snapshot_data.get("last_started_at")
        last_completed_at = job_data.get("completed_at") or snapshot_data.get("last_completed_at")
        last_forced_rebuild = bool(job_data.get("force_full_rebuild") or snapshot_data.get("last_forced_rebuild"))

        return RAGIndexState(
            connection_id=connection_id,
            schema_version=snapshot_data.get("schema_version"),
            index_status=inferred_index_status,
            health_status=inferred_health_status,
            index_mode=index_mode if isinstance(index_mode, RAGIndexMode) else RAGIndexMode(str(index_mode)),
            is_indexed=is_indexed,
            table_count=int(snapshot_data.get("table_count") or 0),
            vector_count=int(snapshot_data.get("vector_count") or 0),
            last_started_at=last_started_at,
            last_completed_at=last_completed_at,
            last_success_at=last_completed_at if inferred_index_status == RAGIndexStatus.READY else snapshot_data.get("last_success_at"),
            last_error=last_error or job_data.get("error_message") or snapshot_data.get("last_error"),
            last_forced_rebuild=last_forced_rebuild,
            updated_at=snapshot_data.get("updated_at") or _utcnow(),
            created_at=snapshot_data.get("created_at") or _utcnow(),
        )

    def build_report(
        self,
        states: Iterable[RAGIndexState | dict[str, Any]],
        *,
        metrics: RAGIndexMetrics | dict[str, Any] | None = None,
    ) -> RAGHealthReport:
        normalized_states = [self._normalize_state(state) for state in states]
        report_metrics = self._normalize_metrics(metrics) if metrics is not None else self._derive_metrics(normalized_states)
        return RAGHealthReport(metrics=report_metrics, connections=normalized_states)

    def _derive_metrics(self, states: list[RAGIndexState]) -> RAGIndexMetrics:
        if not states:
            return RAGIndexMetrics()
        return RAGIndexMetrics(
            total_connections=len(states),
            indexed_connections=sum(1 for state in states if state.is_indexed),
            healthy_connections=sum(1 for state in states if state.health_status == RAGHealthStatus.HEALTHY),
            unhealthy_connections=sum(1 for state in states if state.health_status == RAGHealthStatus.UNHEALTHY),
            indexing_connections=sum(1 for state in states if state.index_status == RAGIndexStatus.INDEXING),
            pending_connections=sum(1 for state in states if state.index_status == RAGIndexStatus.PENDING),
            failed_connections=sum(1 for state in states if state.index_status == RAGIndexStatus.FAILED),
            average_table_count=sum(state.table_count for state in states) / len(states),
            average_vector_count=sum(state.vector_count for state in states) / len(states),
            last_updated_at=max((state.updated_at for state in states), default=None),
        )

    @staticmethod
    def _normalize_snapshot(snapshot: IndexHealthSnapshot | dict[str, Any] | None) -> dict[str, Any]:
        if snapshot is None:
            return {}
        if isinstance(snapshot, IndexHealthSnapshot):
            return {
                "connection_id": snapshot.connection_id,
                "schema_version": snapshot.schema_version,
                "table_count": snapshot.table_count,
                "vector_count": snapshot.vector_count,
                "index_mode": snapshot.index_mode,
                "is_indexed": snapshot.is_indexed,
                "updated_at": snapshot.updated_at,
                "created_at": snapshot.created_at,
            }
        if isinstance(snapshot, dict):
            return dict(snapshot)
        return {
            "connection_id": getattr(snapshot, "connection_id", None),
            "schema_version": getattr(snapshot, "schema_version", None),
            "table_count": getattr(snapshot, "table_count", 0),
            "vector_count": getattr(snapshot, "vector_count", 0),
            "index_mode": getattr(snapshot, "index_mode", RAGIndexMode.HYBRID),
            "is_indexed": getattr(snapshot, "is_indexed", False),
            "updated_at": getattr(snapshot, "updated_at", _utcnow()),
            "created_at": getattr(snapshot, "created_at", _utcnow()),
            "last_started_at": getattr(snapshot, "last_started_at", None),
            "last_completed_at": getattr(snapshot, "last_completed_at", None),
            "last_success_at": getattr(snapshot, "last_success_at", None),
            "last_error": getattr(snapshot, "last_error", None),
            "last_forced_rebuild": getattr(snapshot, "last_forced_rebuild", False),
        }

    @staticmethod
    def _normalize_job(job: IndexJobSnapshot | dict[str, Any] | None) -> dict[str, Any]:
        if job is None:
            return {}
        if isinstance(job, IndexJobSnapshot):
            return {
                "connection_id": job.connection_id,
                "status": job.status,
                "started_at": job.started_at,
                "force_full_rebuild": job.force_full_rebuild,
                "schema_version": job.schema_version,
                "completed_at": job.completed_at,
                "error_message": job.error_message,
            }
        if isinstance(job, dict):
            return dict(job)
        return {
            "connection_id": getattr(job, "connection_id", None),
            "status": getattr(job, "status", None),
            "started_at": getattr(job, "started_at", None),
            "force_full_rebuild": getattr(job, "force_full_rebuild", False),
            "schema_version": getattr(job, "schema_version", None),
            "completed_at": getattr(job, "completed_at", None),
            "error_message": getattr(job, "error_message", None),
        }

    @staticmethod
    def _normalize_state(state: RAGIndexState | dict[str, Any]) -> RAGIndexState:
        if isinstance(state, RAGIndexState):
            return state
        return RAGIndexState(**dict(state))

    @staticmethod
    def _normalize_metrics(metrics: RAGIndexMetrics | dict[str, Any]) -> RAGIndexMetrics:
        if isinstance(metrics, RAGIndexMetrics):
            return metrics
        return RAGIndexMetrics(**dict(metrics))
