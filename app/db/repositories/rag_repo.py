from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from app.db.repositories.base import RepositoryBase
from app.schemas.rag import (
    RAGHealthStatus,
    RAGIndexJob,
    RAGIndexMetrics,
    RAGIndexMode,
    RAGIndexState,
    RAGIndexStatus,
)


class RAGIndexRepository(RepositoryBase):
    @staticmethod
    def _enum_value(value: Any, enum_cls: type[RAGIndexStatus] | type[RAGHealthStatus] | type[RAGIndexMode]) -> str:
        if isinstance(value, enum_cls):
            return value.value
        if isinstance(value, str):
            return value
        return enum_cls(value).value if value is not None else list(enum_cls)[0].value

    def list_states(self) -> list[RAGIndexState]:
        rows = self.fetch_all("SELECT * FROM rag_index_state ORDER BY updated_at DESC, connection_id ASC")
        return [self._row_to_state(row) for row in rows]

    def get_state(self, connection_id: str) -> RAGIndexState | None:
        row = self.fetch_one("SELECT * FROM rag_index_state WHERE connection_id = ?", (connection_id,))
        return self._row_to_state(row) if row else None

    def delete_state(self, connection_id: str) -> None:
        with self.metadata_db.connect() as conn:
            conn.execute("DELETE FROM rag_index_jobs WHERE connection_id = ?", (connection_id,))
            conn.execute("DELETE FROM rag_index_state WHERE connection_id = ?", (connection_id,))

    def upsert_state(
        self,
        *,
        connection_id: str,
        schema_version: str | None,
        index_status: RAGIndexStatus,
        health_status: RAGHealthStatus,
        index_mode: RAGIndexMode = RAGIndexMode.HYBRID,
        is_indexed: bool,
        table_count: int,
        vector_count: int,
        last_started_at: str | None = None,
        last_completed_at: str | None = None,
        last_success_at: str | None = None,
        last_error: str | None = None,
        last_forced_rebuild: bool = False,
    ) -> RAGIndexState:
        now = self.utcnow()
        existing = self.get_state(connection_id)
        created_at = existing.created_at if existing else now
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO rag_index_state (
                    connection_id, schema_version, index_status, health_status, index_mode,
                    is_indexed, table_count, vector_count, last_started_at, last_completed_at,
                    last_success_at, last_error, last_forced_rebuild, updated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connection_id) DO UPDATE SET
                    schema_version = excluded.schema_version,
                    index_status = excluded.index_status,
                    health_status = excluded.health_status,
                    index_mode = excluded.index_mode,
                    is_indexed = excluded.is_indexed,
                    table_count = excluded.table_count,
                    vector_count = excluded.vector_count,
                    last_started_at = excluded.last_started_at,
                    last_completed_at = excluded.last_completed_at,
                    last_success_at = excluded.last_success_at,
                    last_error = excluded.last_error,
                    last_forced_rebuild = excluded.last_forced_rebuild,
                    updated_at = excluded.updated_at
                """,
                (
                    connection_id,
                    schema_version,
                    self._enum_value(index_status, RAGIndexStatus),
                    self._enum_value(health_status, RAGHealthStatus),
                    self._enum_value(index_mode, RAGIndexMode),
                    int(is_indexed),
                    int(table_count),
                    int(vector_count),
                    last_started_at,
                    last_completed_at,
                    last_success_at,
                    last_error,
                    int(last_forced_rebuild),
                    now,
                    created_at,
                ),
            )
        return self.get_state(connection_id) or RAGIndexState(connection_id=connection_id)

    def add_job(
        self,
        *,
        connection_id: str,
        action: str,
        status: str,
        schema_version: str | None,
        force_full_rebuild: bool = False,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_ms: float | None = None,
        error_message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> RAGIndexJob:
        job = RAGIndexJob(
            id=f"rag_job_{uuid4().hex[:12]}",
            connection_id=connection_id,
            schema_version=schema_version,
            action=action,
            status=status,
            force_full_rebuild=force_full_rebuild,
            started_at=started_at or self.utcnow(),
            completed_at=completed_at,
            duration_ms=duration_ms,
            error_message=error_message,
            payload=payload or {},
        )
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO rag_index_jobs (
                    id, connection_id, schema_version, action, status, force_full_rebuild,
                    started_at, completed_at, duration_ms, error_message, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id,
                    job.connection_id,
                    job.schema_version,
                    job.action,
                    job.status,
                    int(job.force_full_rebuild),
                    job.started_at,
                    job.completed_at,
                    job.duration_ms,
                    job.error_message,
                    self.dumps(job.payload),
                ),
            )
        return job

    def list_jobs(self, connection_id: str | None = None) -> list[RAGIndexJob]:
        if connection_id:
            rows = self.fetch_all(
                "SELECT * FROM rag_index_jobs WHERE connection_id = ? ORDER BY started_at DESC",
                (connection_id,),
            )
        else:
            rows = self.fetch_all("SELECT * FROM rag_index_jobs ORDER BY started_at DESC")
        return [self._row_to_job(row) for row in rows]

    def get_metrics(self) -> RAGIndexMetrics:
        rows = self.fetch_all("SELECT * FROM rag_index_state")
        if not rows:
            return RAGIndexMetrics()
        table_count_values = [int(row.get("table_count") or 0) for row in rows]
        vector_count_values = [int(row.get("vector_count") or 0) for row in rows]
        statuses = [str(row.get("health_status") or "unknown") for row in rows]
        build_statuses = [str(row.get("index_status") or "pending") for row in rows]
        return RAGIndexMetrics(
            total_connections=len(rows),
            indexed_connections=sum(1 for row in rows if int(row.get("is_indexed") or 0)),
            healthy_connections=sum(1 for value in statuses if value == RAGHealthStatus.HEALTHY.value),
            unhealthy_connections=sum(1 for value in statuses if value == RAGHealthStatus.UNHEALTHY.value),
            indexing_connections=sum(1 for value in build_statuses if value == RAGIndexStatus.INDEXING.value),
            pending_connections=sum(1 for value in build_statuses if value == RAGIndexStatus.PENDING.value),
            failed_connections=sum(1 for value in build_statuses if value == RAGIndexStatus.FAILED.value),
            average_table_count=sum(table_count_values) / len(table_count_values),
            average_vector_count=sum(vector_count_values) / len(vector_count_values),
            last_updated_at=max((str(row.get("updated_at")) for row in rows if row.get("updated_at")), default=None),
        )

    @staticmethod
    def _row_to_state(row: dict | None) -> RAGIndexState | None:
        if row is None:
            return None
        return RAGIndexState(
            connection_id=row["connection_id"],
            schema_version=row.get("schema_version"),
            index_status=RAGIndexStatus(row.get("index_status") or RAGIndexStatus.PENDING.value),
            health_status=RAGHealthStatus(row.get("health_status") or RAGHealthStatus.UNKNOWN.value),
            index_mode=RAGIndexMode(row.get("index_mode") or RAGIndexMode.HYBRID.value),
            is_indexed=bool(row.get("is_indexed")),
            table_count=int(row.get("table_count") or 0),
            vector_count=int(row.get("vector_count") or 0),
            last_started_at=row.get("last_started_at"),
            last_completed_at=row.get("last_completed_at"),
            last_success_at=row.get("last_success_at"),
            last_error=row.get("last_error"),
            last_forced_rebuild=bool(row.get("last_forced_rebuild")),
            updated_at=row.get("updated_at") or row.get("created_at") or RepositoryBase.utcnow(),
            created_at=row.get("created_at") or row.get("updated_at") or RepositoryBase.utcnow(),
        )

    @staticmethod
    def _row_to_job(row: dict) -> RAGIndexJob:
        return RAGIndexJob(
            id=row["id"],
            connection_id=row["connection_id"],
            schema_version=row.get("schema_version"),
            action=row["action"],
            status=row["status"],
            force_full_rebuild=bool(row.get("force_full_rebuild")),
            started_at=row["started_at"],
            completed_at=row.get("completed_at"),
            duration_ms=row.get("duration_ms"),
            error_message=row.get("error_message"),
            payload=json.loads(row.get("payload_json") or "{}"),
        )
