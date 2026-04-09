from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

from app.agent.schema_retriever import SchemaRetriever
from app.db.manager import DBManager
from app.db.metadata import MetadataDB
from app.db.repositories.rag_repo import RAGIndexRepository
from app.rag.async_indexing import AsyncIndexingManager, IndexBuildArtifact
from app.rag.debug_view import build_debug_view_from_manager
from app.rag.index_health import IndexHealthBuilder
from app.rag.indexing import IndexingSystem
from app.rag.schema_version import SchemaVersionManager
from app.schemas.connection import TableSchema
from app.schemas.rag import (
    RAGAcceptanceResult,
    RAGAcceptanceThresholds,
    RAGDegradationEventRecord,
    RAGDegradationSnapshot,
    RAGHealthStatus,
    RAGIndexBuildRequest,
    RAGIndexHealthDetail,
    RAGIndexJob,
    RAGIndexMode,
    RAGIndexState,
    RAGIndexStatus,
    RAGIndexMetrics,
    SchemaVersionDiffResponse,
    SchemaVersionRecordResponse,
    RAGStabilityResult,
    SchemaVersionResponse,
)


class RAGIndexManager:
    def __init__(
        self,
        metadata_db: MetadataDB,
        db_manager: DBManager,
        schema_retriever: SchemaRetriever | None = None,
        max_workers: int = 2,
    ):
        self.metadata_db = metadata_db
        self.db_manager = db_manager
        self.schema_retriever = schema_retriever
        self.repo = RAGIndexRepository(metadata_db)
        version_storage = Path(getattr(metadata_db, "path", Path("./metadata.sqlite3"))).with_name("rag_schema_versions.json")
        self.version_manager = SchemaVersionManager(version_storage)
        self.indexing_system = IndexingSystem(self.version_manager)
        self.health_builder = IndexHealthBuilder()
        self.async_indexing = AsyncIndexingManager(max_workers=max_workers, health_builder=self.health_builder)
        self._job_sync_marker: dict[str, str] = {}
        self._lock = threading.Lock()

    def compute_schema_version(self, tables: list[TableSchema]) -> str:
        return self.version_manager.compute_version(tables)

    def get_status(self, connection_id: str) -> RAGIndexState:
        self._sync_async_state(connection_id)
        state = self.repo.get_state(connection_id)
        if state is not None:
            return state
        schema_cache = self.db_manager.get_schema_cache(connection_id)
        if schema_cache is None:
            return RAGIndexState(connection_id=connection_id)
        return self.repo.upsert_state(
            connection_id=connection_id,
            schema_version=self.compute_schema_version(schema_cache.tables),
            index_status=RAGIndexStatus.PENDING,
            health_status=RAGHealthStatus.UNKNOWN,
            index_mode=RAGIndexMode.HYBRID,
            is_indexed=False,
            table_count=len(schema_cache.tables),
            vector_count=len(schema_cache.tables),
        )

    def list_statuses(self) -> list[RAGIndexState]:
        states = []
        for state in self.repo.list_states():
            self._sync_async_state(state.connection_id)
            states.append(self.repo.get_state(state.connection_id) or state)
        return states

    def get_metrics(self) -> RAGIndexMetrics:
        metrics = self.repo.get_metrics()
        cache_stats = self._retrieval_cache_stats()
        telemetry_stats = self._retrieval_telemetry_stats()
        degradation_stats = self._retrieval_degradation_stats()
        concurrency_stats = self._retrieval_concurrency_stats()
        payload = metrics.model_dump(mode="json") if hasattr(metrics, "model_dump") else dict(metrics.__dict__)
        payload.update(
            {
                "cache_entries": int(cache_stats.get("entries", 0) or 0),
                "cache_hits": int(cache_stats.get("hits", 0) or 0),
                "cache_misses": int(cache_stats.get("misses", 0) or 0),
                "cache_evictions": int(cache_stats.get("evictions", 0) or 0),
                "cache_invalidations": int(cache_stats.get("invalidations", 0) or 0),
                "cache_hit_rate": float(cache_stats.get("hit_rate", 0.0) or 0.0),
                "logged_queries": int(telemetry_stats.get("logged_queries", 0) or 0),
                "retrieval_p50_ms": float(telemetry_stats.get("retrieval_p50_ms", 0.0) or 0.0),
                "retrieval_p95_ms": float(telemetry_stats.get("retrieval_p95_ms", 0.0) or 0.0),
                "retrieval_p99_ms": float(telemetry_stats.get("retrieval_p99_ms", 0.0) or 0.0),
                "embedding_p50_ms": float(telemetry_stats.get("embedding_p50_ms", 0.0) or 0.0),
                "embedding_p95_ms": float(telemetry_stats.get("embedding_p95_ms", 0.0) or 0.0),
                "embedding_p99_ms": float(telemetry_stats.get("embedding_p99_ms", 0.0) or 0.0),
                "vector_hit_rate": float(telemetry_stats.get("vector_hit_rate", 0.0) or 0.0),
                "bm25_hit_rate": float(telemetry_stats.get("bm25_hit_rate", 0.0) or 0.0),
                "fallback_rate": float(telemetry_stats.get("fallback_rate", 0.0) or 0.0),
                "table_not_found_rate": float(telemetry_stats.get("table_not_found_rate", 0.0) or 0.0),
                "timeout_rate": float(telemetry_stats.get("timeout_rate", 0.0) or 0.0),
                "concurrency_rejection_rate": float(telemetry_stats.get("concurrency_rejection_rate", 0.0) or 0.0),
                "failure_categories": dict(telemetry_stats.get("failure_categories") or {}),
                "degraded_connections": int(degradation_stats.get("degraded_connections", 0) or 0),
                "degradation_count": int(degradation_stats.get("degradation_count", 0) or 0),
                "recovery_count": int(degradation_stats.get("recovery_count", 0) or 0),
                "current_degradation_mode": degradation_stats.get("current_mode"),
                "retrieval_timeout_count": int(concurrency_stats.get("retrieval_timeout_count", 0) or 0),
                "queue_timeout_count": int(concurrency_stats.get("queue_timeout_count", 0) or 0),
                "rejected_requests": int(concurrency_stats.get("rejected_requests", 0) or 0),
                "peak_active_requests": int(concurrency_stats.get("peak_active_requests", 0) or 0),
            }
        )
        return RAGIndexMetrics(**payload)

    def get_schema_version(self, connection_id: str) -> SchemaVersionResponse:
        current = self.version_manager.get_current_version(connection_id)
        state = self.get_status(connection_id)
        return SchemaVersionResponse(
            connection_id=connection_id,
            schema_version=current.version if current is not None else state.schema_version,
            table_count=current.table_count if current is not None else state.table_count,
            updated_at=current.created_at.isoformat() if current is not None else state.updated_at,
        )

    def list_schema_versions(self, connection_id: str) -> list[SchemaVersionRecordResponse]:
        return [
            SchemaVersionRecordResponse(
                connection_id=record.connection_id,
                version=record.version,
                schema_fingerprint=record.schema_fingerprint,
                table_count=record.table_count,
                table_fingerprints=dict(record.table_fingerprints),
                metadata=dict(record.metadata),
                created_at=record.created_at.isoformat(),
            )
            for record in reversed(self.version_manager.get_version_history(connection_id))
        ]

    def get_schema_version_detail(self, connection_id: str, version: str) -> SchemaVersionRecordResponse | None:
        record = self.version_manager.get_version(connection_id, version)
        if record is None:
            return None
        return SchemaVersionRecordResponse(
            connection_id=record.connection_id,
            version=record.version,
            schema_fingerprint=record.schema_fingerprint,
            table_count=record.table_count,
            table_fingerprints=dict(record.table_fingerprints),
            metadata=dict(record.metadata),
            created_at=record.created_at.isoformat(),
        )

    def diff_schema_versions(
        self,
        connection_id: str,
        *,
        left_version: str | None,
        right_version: str | None = None,
    ) -> SchemaVersionDiffResponse:
        return SchemaVersionDiffResponse(
            **self.version_manager.diff_versions(
                connection_id,
                left_version=left_version,
                right_version=right_version,
            )
        )

    def schedule_rebuild(self, connection_id: str, payload: RAGIndexBuildRequest | None = None) -> RAGIndexState:
        payload = payload or RAGIndexBuildRequest()
        schema_cache = self.db_manager.get_schema_cache(connection_id)
        if schema_cache is None:
            return self.repo.upsert_state(
                connection_id=connection_id,
                schema_version=None,
                index_status=RAGIndexStatus.FAILED,
                health_status=RAGHealthStatus.UNHEALTHY,
                index_mode=RAGIndexMode.HYBRID,
                is_indexed=False,
                table_count=0,
                vector_count=0,
                last_error="Schema cache not found. Refresh the schema first.",
                last_forced_rebuild=payload.force_full_rebuild,
            )

        plan = self.indexing_system.detect_changes(connection_id, schema_cache.tables, force=payload.force_full_rebuild)
        schema_version = plan.schema_fingerprint
        existing_state = self.repo.get_state(connection_id)
        if not payload.force_full_rebuild and not plan.changed_tables and existing_state and existing_state.is_indexed:
            return existing_state
        self._invalidate_retrieval_cache(connection_id)
        if existing_state and existing_state.is_indexed:
            self.async_indexing.register_snapshot(
                connection_id,
                schema_version=existing_state.schema_version,
                table_count=existing_state.table_count,
                vector_count=existing_state.vector_count,
                is_indexed=existing_state.is_indexed,
            )
        started_at = self.repo.utcnow()
        state = self.repo.upsert_state(
            connection_id=connection_id,
            schema_version=schema_version,
            index_status=RAGIndexStatus.INDEXING,
            health_status=RAGHealthStatus.DEGRADED,
            index_mode=RAGIndexMode.HYBRID,
            is_indexed=False,
            table_count=len(schema_cache.tables),
            vector_count=len(schema_cache.tables),
            last_started_at=started_at,
            last_error=None,
            last_forced_rebuild=payload.force_full_rebuild,
        )
        self.repo.add_job(
            connection_id=connection_id,
            action="rebuild",
            status="running",
            schema_version=schema_version,
            force_full_rebuild=payload.force_full_rebuild,
            started_at=started_at,
            payload={
                "table_count": len(schema_cache.tables),
                "force_full_rebuild": payload.force_full_rebuild,
                "changed_tables": plan.changed_tables,
                "tables_to_index": plan.tables_to_index,
            },
        )
        self.async_indexing.schedule_rebuild(
            connection_id,
            self._build_index_artifact,
            force_full_rebuild=payload.force_full_rebuild,
            payload={"table_count": len(schema_cache.tables)},
        )
        return self.get_status(connection_id)

    def get_job_state(self, connection_id: str) -> str | None:
        return self.async_indexing.get_job_state(connection_id)

    def list_index_jobs(self, connection_id: str | None = None, limit: int = 20) -> list[RAGIndexJob]:
        jobs = self.repo.list_jobs(connection_id)
        return jobs[: max(1, int(limit))]

    def get_index_health_detail(self, connection_id: str) -> RAGIndexHealthDetail:
        state = self.get_status(connection_id)
        runtime = self.get_runtime_metrics()
        vector_store_available = bool(runtime.get("vector_store_available", True))
        bm25_enabled = bool(runtime.get("bm25_enabled", True))
        current_job = self.async_indexing.get_job(connection_id)
        return RAGIndexHealthDetail(
            state=state,
            current_job=RAGIndexJob(
                id=current_job.id,
                connection_id=current_job.connection_id,
                schema_version=current_job.schema_version,
                action="rebuild",
                status=current_job.status,
                force_full_rebuild=current_job.force_full_rebuild,
                started_at=current_job.started_at or self.repo.utcnow(),
                completed_at=current_job.completed_at,
                duration_ms=current_job.duration_ms,
                error_message=current_job.error_message,
                payload=dict(current_job.payload),
            ) if current_job is not None else None,
            latest_jobs=self.list_index_jobs(connection_id=connection_id, limit=10),
            vector_store_available=vector_store_available,
            bm25_enabled=bm25_enabled,
            degradation=self.get_degradation_snapshot(connection_id).model_dump(mode="json"),
            async_snapshot=self.async_indexing.build_health_report(
                connection_id,
                vector_store_available=vector_store_available,
                bm25_enabled=bm25_enabled,
            ).model_dump(mode="json"),
        )

    def delete_connection_state(self, connection_id: str) -> None:
        self.async_indexing.cancel_job(connection_id)
        self._invalidate_retrieval_cache(connection_id)
        self.repo.delete_state(connection_id)

    def health_snapshot(self) -> dict[str, Any]:
        metrics = self.get_metrics()
        report = self.health_builder.build_report(self.list_statuses(), metrics=metrics)
        return {
            "metrics": report.metrics.model_dump(mode="json") if hasattr(report.metrics, "model_dump") else report.metrics.__dict__,
            "connections": [state.model_dump(mode="json") for state in report.connections],
        }

    def get_degradation_snapshot(self, connection_id: str | None = None) -> RAGDegradationSnapshot:
        runtime = self.get_runtime_metrics()
        degradation = dict(runtime.get("degradation") or {})
        if self.schema_retriever is not None:
            getter = getattr(self.schema_retriever, "get_degradation_snapshot", None)
            if callable(getter):
                payload = getter(connection_id=connection_id)
                if payload:
                    return RAGDegradationSnapshot(**payload)
        if connection_id:
            degradation.setdefault("connection_id", connection_id)
        return RAGDegradationSnapshot(**degradation)

    def list_degradation_events(self, connection_id: str | None = None, limit: int = 50) -> list[RAGDegradationEventRecord]:
        if self.schema_retriever is not None:
            getter = getattr(self.schema_retriever, "list_degradation_events", None)
            if callable(getter):
                return [RAGDegradationEventRecord(**item) for item in getter(connection_id=connection_id, limit=limit)]
        return []

    def get_runtime_metrics(self) -> dict[str, Any]:
        if self.schema_retriever is None:
            return {}
        runtime_metrics = getattr(self.schema_retriever, "runtime_metrics", None)
        if isinstance(runtime_metrics, dict):
            return dict(runtime_metrics)
        if callable(runtime_metrics):
            try:
                payload = runtime_metrics()
            except Exception:  # pragma: no cover
                return {}
            return dict(payload) if isinstance(payload, dict) else {}
        return {}

    def get_query_details(self, query_id: str) -> dict[str, Any] | None:
        if self.schema_retriever is None:
            return None
        getter = getattr(self.schema_retriever, "get_query_details", None)
        if callable(getter):
            return getter(query_id)
        return None

    def get_telemetry_dashboard(self) -> dict[str, Any]:
        if self.schema_retriever is None:
            return {}
        getter = getattr(self.schema_retriever, "get_telemetry_dashboard", None)
        if callable(getter):
            return getter()
        runtime = self.get_runtime_metrics()
        return dict(runtime.get("telemetry_snapshot") or {})

    def list_telemetry_history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if self.schema_retriever is None:
            return []
        getter = getattr(self.schema_retriever, "list_telemetry_history", None)
        if callable(getter):
            return getter(limit=limit)
        return []

    def list_telemetry_events(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if self.schema_retriever is None:
            return []
        getter = getattr(self.schema_retriever, "get_telemetry_events", None)
        if callable(getter):
            return getter(connection_id=connection_id, query_id=query_id, limit=limit)
        return []

    def get_telemetry_event(self, query_id: str) -> dict[str, Any] | None:
        if self.schema_retriever is None:
            return None
        getter = getattr(self.schema_retriever, "get_telemetry_event", None)
        if callable(getter):
            return getter(query_id)
        return None

    def get_telemetry_events(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return self.list_telemetry_events(connection_id=connection_id, query_id=query_id, limit=limit)

    def get_debug_view(self, query_id: str) -> dict[str, Any] | None:
        payload = build_debug_view_from_manager(self, query_id)
        if payload is None:
            return None
        if hasattr(payload, "model_dump"):
            return payload.model_dump(mode="json")
        if hasattr(payload, "to_dict"):
            return payload.to_dict()
        return dict(payload)

    def list_query_logs(self, *, connection_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if self.schema_retriever is None:
            return []
        getter = getattr(self.schema_retriever, "list_query_logs", None)
        if callable(getter):
            return getter(connection_id=connection_id, limit=limit)
        return []

    def evaluate_acceptance(
        self,
        metrics: dict[str, Any],
        thresholds: RAGAcceptanceThresholds | None = None,
    ) -> RAGAcceptanceResult:
        thresholds = thresholds or RAGAcceptanceThresholds()
        recall_at_5 = float(metrics.get("recall_at_5", 0.0) or 0.0)
        mrr = float(metrics.get("mrr", 0.0) or 0.0)
        table_not_found_rate = float(metrics.get("table_not_found_rate", 1.0) or 1.0)
        checks = {
            "recall_at_5": recall_at_5 >= thresholds.minimum_recall_at_5,
            "mrr": mrr >= thresholds.minimum_mrr,
            "table_not_found_rate": table_not_found_rate <= thresholds.maximum_table_not_found_rate,
        }
        failures = [name for name, passed in checks.items() if not passed]
        return RAGAcceptanceResult(
            passed=all(checks.values()),
            checks=checks,
            metrics={
                "recall_at_5": recall_at_5,
                "mrr": mrr,
                "table_not_found_rate": table_not_found_rate,
            },
            failures=failures,
        )

    def evaluate_stability(self) -> RAGStabilityResult:
        runtime = self.get_runtime_metrics()
        telemetry = dict(runtime.get("telemetry") or {})
        concurrency = dict(runtime.get("concurrency") or {})
        degradation = dict(runtime.get("degradation") or {})
        timeout_rate = float(telemetry.get("timeout_rate", 0.0) or 0.0)
        fallback_rate = float(telemetry.get("fallback_rate", 0.0) or 0.0)
        rejection_rate = float(telemetry.get("concurrency_rejection_rate", 0.0) or 0.0)
        retrieval_timeout_count = int(concurrency.get("retrieval_timeout_count", 0) or 0)
        degraded_connections = int(degradation.get("degraded_connections", 0) or 0)
        checks = {
            "timeout_rate": timeout_rate <= 0.2,
            "fallback_rate": fallback_rate <= 0.5,
            "concurrency_rejection_rate": rejection_rate <= 0.2,
            "retrieval_timeout_count": retrieval_timeout_count <= 5,
            "degraded_connections": degraded_connections <= 3,
        }
        failures = [name for name, passed in checks.items() if not passed]
        return RAGStabilityResult(
            passed=all(checks.values()),
            checks=checks,
            metrics={
                "timeout_rate": timeout_rate,
                "fallback_rate": fallback_rate,
                "concurrency_rejection_rate": rejection_rate,
                "retrieval_timeout_count": retrieval_timeout_count,
                "degraded_connections": degraded_connections,
            },
            failures=failures,
        )

    def _retrieval_cache_stats(self) -> dict[str, Any]:
        if self.schema_retriever is None:
            return {}
        runtime_metrics = getattr(self.schema_retriever, "runtime_metrics", None)
        if isinstance(runtime_metrics, dict):
            return dict(runtime_metrics.get("cache") or {})
        if callable(runtime_metrics):
            try:
                payload = runtime_metrics()
            except Exception:  # pragma: no cover
                return {}
            return dict(payload.get("cache") or {}) if isinstance(payload, dict) else {}
        return {}

    def _retrieval_telemetry_stats(self) -> dict[str, Any]:
        if self.schema_retriever is None:
            return {}
        runtime_metrics = getattr(self.schema_retriever, "runtime_metrics", None)
        if isinstance(runtime_metrics, dict):
            return dict(runtime_metrics.get("telemetry") or {})
        if callable(runtime_metrics):
            try:
                payload = runtime_metrics()
            except Exception:  # pragma: no cover
                return {}
            return dict(payload.get("telemetry") or {}) if isinstance(payload, dict) else {}
        return {}

    def _retrieval_degradation_stats(self) -> dict[str, Any]:
        if self.schema_retriever is None:
            return {}
        runtime_metrics = getattr(self.schema_retriever, "runtime_metrics", None)
        if isinstance(runtime_metrics, dict):
            return dict(runtime_metrics.get("degradation") or {})
        if callable(runtime_metrics):
            try:
                payload = runtime_metrics()
            except Exception:  # pragma: no cover
                return {}
            return dict(payload.get("degradation") or {}) if isinstance(payload, dict) else {}
        return {}

    def _retrieval_concurrency_stats(self) -> dict[str, Any]:
        if self.schema_retriever is None:
            return {}
        runtime_metrics = getattr(self.schema_retriever, "runtime_metrics", None)
        if isinstance(runtime_metrics, dict):
            return dict(runtime_metrics.get("concurrency") or {})
        if callable(runtime_metrics):
            try:
                payload = runtime_metrics()
            except Exception:  # pragma: no cover
                return {}
            return dict(payload.get("concurrency") or {}) if isinstance(payload, dict) else {}
        return {}

    def _invalidate_retrieval_cache(self, connection_id: str) -> None:
        if self.schema_retriever is None:
            return
        invalidate = getattr(self.schema_retriever, "invalidate_connection_cache", None)
        if callable(invalidate):
            try:
                invalidate(connection_id)
            except Exception:  # pragma: no cover
                return

    def _build_index_artifact(self, connection_id: str, force_full_rebuild: bool, _payload: dict[str, Any]) -> IndexBuildArtifact:
        schema_cache = self.db_manager.get_schema_cache(connection_id) or self.db_manager.refresh_schema_cache(connection_id)
        indexing_result = self.indexing_system.incremental_update(connection_id, schema_cache.tables, force=force_full_rebuild)
        connection = self.db_manager.get_connection_status(connection_id)
        if self.schema_retriever is not None:
            asyncio.run(
                self.schema_retriever.index_schema(
                    connection_id,
                    schema_cache.tables,
                    database_name=getattr(connection, "database", None),
                    force=bool(force_full_rebuild or indexing_result.changed_tables),
                )
            )
        return IndexBuildArtifact(
            schema_version=indexing_result.schema_version,
            table_count=len(schema_cache.tables),
            vector_count=len(schema_cache.tables),
            metadata={
                "changed_tables": indexing_result.changed_tables,
                "indexed_tables": indexing_result.indexed_tables,
                "skipped_tables": indexing_result.skipped_tables,
                "force_full_rebuild": force_full_rebuild,
            },
        )

    def _sync_async_state(self, connection_id: str) -> None:
        snapshot = self.async_indexing.get_snapshot(connection_id)
        job = self.async_indexing.get_job(connection_id)
        if snapshot is None and job is None:
            return
        state = self.async_indexing.get_connection_state(connection_id)
        self.repo.upsert_state(
            connection_id=connection_id,
            schema_version=state.schema_version,
            index_status=state.index_status,
            health_status=state.health_status,
            index_mode=state.index_mode,
            is_indexed=state.is_indexed,
            table_count=state.table_count,
            vector_count=state.vector_count,
            last_started_at=state.last_started_at,
            last_completed_at=state.last_completed_at,
            last_success_at=state.last_success_at,
            last_error=state.last_error,
            last_forced_rebuild=state.last_forced_rebuild,
        )
        marker = None
        if job is not None:
            marker = f"{job.id}:{job.status}:{job.completed_at or ''}"
        if marker and self._job_sync_marker.get(connection_id) != marker:
            self.repo.add_job(
                connection_id=connection_id,
                action="rebuild",
                status=job.status,
                schema_version=job.schema_version or state.schema_version,
                force_full_rebuild=job.force_full_rebuild,
                started_at=job.started_at or self.repo.utcnow(),
                completed_at=job.completed_at,
                duration_ms=job.duration_ms,
                error_message=job.error_message,
                payload=dict(job.payload),
            )
            self._job_sync_marker[connection_id] = marker

    def _refresh_runtime_state(self, state: RAGIndexState) -> RAGIndexState:
        job_state = self.get_job_state(state.connection_id)
        if job_state == "running" and state.index_status != RAGIndexStatus.INDEXING:
            return self.repo.upsert_state(
                connection_id=state.connection_id,
                schema_version=state.schema_version,
                index_status=RAGIndexStatus.INDEXING,
                health_status=RAGHealthStatus.DEGRADED,
                index_mode=state.index_mode,
                is_indexed=state.is_indexed,
                table_count=state.table_count,
                vector_count=state.vector_count,
                last_started_at=state.last_started_at,
                last_completed_at=state.last_completed_at,
                last_success_at=state.last_success_at,
                last_error=state.last_error,
                last_forced_rebuild=state.last_forced_rebuild,
            )
        if job_state == "failed" and state.index_status != RAGIndexStatus.FAILED:
            return self.repo.upsert_state(
                connection_id=state.connection_id,
                schema_version=state.schema_version,
                index_status=RAGIndexStatus.FAILED,
                health_status=RAGHealthStatus.UNHEALTHY,
                index_mode=state.index_mode,
                is_indexed=state.is_indexed,
                table_count=state.table_count,
                vector_count=state.vector_count,
                last_started_at=state.last_started_at,
                last_completed_at=state.last_completed_at,
                last_success_at=state.last_success_at,
                last_error=state.last_error,
                last_forced_rebuild=state.last_forced_rebuild,
            )
        return state
