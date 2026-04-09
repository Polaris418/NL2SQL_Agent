from __future__ import annotations

import inspect
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Callable

from app.rag.index_health import IndexHealthBuilder, IndexHealthSnapshot, IndexJobSnapshot


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class IndexBuildArtifact:
    schema_version: str | None = None
    table_count: int = 0
    vector_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    updated_at: str = field(default_factory=_utcnow)


@dataclass(slots=True)
class AsyncIndexJob:
    id: str
    connection_id: str
    status: str = "pending"
    force_full_rebuild: bool = False
    schema_version: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    duration_ms: float | None = None
    error_message: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AsyncIndexConnectionState:
    connection_id: str
    committed: IndexHealthSnapshot | None = None
    active_job: AsyncIndexJob | None = None
    last_error: str | None = None
    last_job_status: str | None = None


class AsyncIndexingManager:
    """Lightweight async indexing manager with atomic commit semantics."""

    def __init__(self, *, max_workers: int = 2, health_builder: IndexHealthBuilder | None = None):
        self._executor = ThreadPoolExecutor(max_workers=max(1, int(max_workers)), thread_name_prefix="rag-async-index")
        self._lock = threading.RLock()
        self._jobs: dict[str, AsyncIndexJob] = {}
        self._futures: dict[str, Future] = {}
        self._states: dict[str, AsyncIndexConnectionState] = {}
        self.health_builder = health_builder or IndexHealthBuilder()

    def register_snapshot(
        self,
        connection_id: str,
        *,
        schema_version: str | None = None,
        table_count: int = 0,
        vector_count: int = 0,
        metadata: dict[str, Any] | None = None,
        is_indexed: bool = True,
    ) -> IndexHealthSnapshot:
        snapshot = IndexHealthSnapshot(
            connection_id=connection_id,
            schema_version=schema_version,
            table_count=table_count,
            vector_count=vector_count,
            is_indexed=is_indexed,
            updated_at=_utcnow(),
            created_at=self._states.get(connection_id).committed.created_at if connection_id in self._states and self._states[connection_id].committed else _utcnow(),
        )
        with self._lock:
            state = self._states.setdefault(connection_id, AsyncIndexConnectionState(connection_id=connection_id))
            state.committed = snapshot
            if metadata:
                state.last_error = metadata.get("last_error") or state.last_error
        return snapshot

    def schedule_rebuild(
        self,
        connection_id: str,
        build_fn: Callable[..., Any],
        *,
        force_full_rebuild: bool = False,
        payload: dict[str, Any] | None = None,
    ) -> AsyncIndexJob:
        payload = dict(payload or {})
        job = AsyncIndexJob(
            id=f"rag_async_{connection_id}_{int(perf_counter() * 1000)}",
            connection_id=connection_id,
            status="running",
            force_full_rebuild=force_full_rebuild,
            started_at=_utcnow(),
            payload=payload,
        )
        with self._lock:
            state = self._states.setdefault(connection_id, AsyncIndexConnectionState(connection_id=connection_id))
            state.active_job = job
            state.last_job_status = job.status
            self._jobs[connection_id] = job
            future = self._executor.submit(self._run_job, connection_id, build_fn, force_full_rebuild, payload, job.started_at)
            self._futures[connection_id] = future
        return job

    def get_job(self, connection_id: str) -> AsyncIndexJob | None:
        with self._lock:
            job = self._jobs.get(connection_id)
            return self._clone_job(job)

    def get_snapshot(self, connection_id: str) -> IndexHealthSnapshot | None:
        with self._lock:
            state = self._states.get(connection_id)
            if state is None or state.committed is None:
                return None
            return self._clone_snapshot(state.committed)

    def get_job_state(self, connection_id: str) -> str | None:
        job = self.get_job(connection_id)
        return job.status if job else None

    def wait_for_job(self, connection_id: str, timeout: float | None = None) -> bool:
        with self._lock:
            future = self._futures.get(connection_id)
        if future is None:
            return True
        future.result(timeout=timeout)
        return True

    def cancel_job(self, connection_id: str) -> bool:
        with self._lock:
            future = self._futures.get(connection_id)
            if future is None:
                return False
            cancelled = future.cancel()
            if cancelled and connection_id in self._jobs:
                self._jobs[connection_id].status = "cancelled"
            return cancelled

    def build_health_report(self, connection_id: str, *, vector_store_available: bool = True, bm25_enabled: bool = True):
        state = self.get_connection_state(connection_id, vector_store_available=vector_store_available, bm25_enabled=bm25_enabled)
        return self.health_builder.build_report([state])

    def get_connection_state(
        self,
        connection_id: str,
        *,
        vector_store_available: bool = True,
        bm25_enabled: bool = True,
    ):
        with self._lock:
            state = self._states.get(connection_id)
            committed = self._clone_snapshot(state.committed) if state and state.committed else None
            job = self._clone_job(state.active_job) if state and state.active_job else None
            last_error = state.last_error if state else None
        return self.health_builder.build_state(
            connection_id,
            snapshot=committed,
            job=job,
            vector_store_available=vector_store_available,
            bm25_enabled=bm25_enabled,
            last_error=last_error,
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connections": len(self._states),
                "jobs": {connection_id: self._clone_job(job) for connection_id, job in self._jobs.items()},
                "snapshots": {
                    connection_id: self._clone_snapshot(state.committed)
                    for connection_id, state in self._states.items()
                    if state.committed is not None
                },
            }

    def _run_job(
        self,
        connection_id: str,
        build_fn: Callable[..., Any],
        force_full_rebuild: bool,
        payload: dict[str, Any],
        started_at: str,
    ) -> None:
        started = perf_counter()
        try:
            result = self._invoke_builder(build_fn, connection_id, force_full_rebuild, payload)
            artifact = self._normalize_artifact(result, connection_id)
            with self._lock:
                state = self._states.setdefault(connection_id, AsyncIndexConnectionState(connection_id=connection_id))
                state.committed = IndexHealthSnapshot(
                    connection_id=connection_id,
                    schema_version=artifact.schema_version,
                    table_count=artifact.table_count,
                    vector_count=artifact.vector_count,
                    is_indexed=True,
                    updated_at=artifact.updated_at,
                    created_at=state.committed.created_at if state.committed else artifact.updated_at,
                )
                state.last_error = None
                if state.active_job is not None:
                    state.active_job.status = "completed"
                    state.active_job.schema_version = artifact.schema_version
                    state.active_job.completed_at = _utcnow()
                    state.active_job.duration_ms = round((perf_counter() - started) * 1000.0, 2)
                    state.active_job.error_message = None
                    state.last_job_status = state.active_job.status
                    self._jobs[connection_id] = state.active_job
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                state = self._states.setdefault(connection_id, AsyncIndexConnectionState(connection_id=connection_id))
                state.last_error = str(exc)
                if state.active_job is not None:
                    state.active_job.status = "failed"
                    state.active_job.completed_at = _utcnow()
                    state.active_job.duration_ms = round((perf_counter() - started) * 1000.0, 2)
                    state.active_job.error_message = str(exc)
                    state.last_job_status = state.active_job.status
                    self._jobs[connection_id] = state.active_job
            raise

    @staticmethod
    def _invoke_builder(build_fn: Callable[..., Any], connection_id: str, force_full_rebuild: bool, payload: dict[str, Any]) -> Any:
        signature = inspect.signature(build_fn)
        params = list(signature.parameters)
        if len(params) >= 3:
            return build_fn(connection_id, force_full_rebuild, payload)
        if len(params) == 2:
            return build_fn(connection_id, force_full_rebuild)
        if len(params) == 1:
            return build_fn(connection_id)
        return build_fn()

    @staticmethod
    def _normalize_artifact(result: Any, connection_id: str) -> IndexBuildArtifact:
        if isinstance(result, IndexBuildArtifact):
            return result
        if isinstance(result, dict):
            return IndexBuildArtifact(
                schema_version=result.get("schema_version"),
                table_count=int(result.get("table_count") or 0),
                vector_count=int(result.get("vector_count") or 0),
                metadata=dict(result.get("metadata") or {}),
                updated_at=str(result.get("updated_at") or _utcnow()),
            )
        schema_version = getattr(result, "schema_version", None)
        table_count = int(getattr(result, "table_count", 0) or 0)
        vector_count = int(getattr(result, "vector_count", table_count) or 0)
        metadata = dict(getattr(result, "metadata", {}) or {})
        updated_at = str(getattr(result, "updated_at", _utcnow()) or _utcnow())
        if schema_version is None and metadata:
            schema_version = metadata.get("schema_version")
        return IndexBuildArtifact(
            schema_version=schema_version,
            table_count=table_count,
            vector_count=vector_count,
            metadata=metadata,
            updated_at=updated_at,
        )

    @staticmethod
    def _clone_snapshot(snapshot: IndexHealthSnapshot) -> IndexHealthSnapshot:
        return IndexHealthSnapshot(
            connection_id=snapshot.connection_id,
            schema_version=snapshot.schema_version,
            table_count=snapshot.table_count,
            vector_count=snapshot.vector_count,
            index_mode=snapshot.index_mode,
            is_indexed=snapshot.is_indexed,
            updated_at=snapshot.updated_at,
            created_at=snapshot.created_at,
        )

    @staticmethod
    def _clone_job(job: AsyncIndexJob | None) -> AsyncIndexJob | None:
        if job is None:
            return None
        return AsyncIndexJob(
            id=job.id,
            connection_id=job.connection_id,
            status=job.status,
            force_full_rebuild=job.force_full_rebuild,
            schema_version=job.schema_version,
            started_at=job.started_at,
            completed_at=job.completed_at,
            duration_ms=job.duration_ms,
            error_message=job.error_message,
            payload=dict(job.payload),
        )
