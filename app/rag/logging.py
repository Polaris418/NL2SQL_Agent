from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.db.repositories.rag_query_log_repo import RAGQueryLogRepository


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class RetrievalLogEntry:
    query_id: str
    connection_id: str
    original_query: str
    rewritten_query: str | None = None
    expanded_query: str | None = None
    selected_tables: list[str] = field(default_factory=list)
    candidate_scores: list[dict[str, Any]] = field(default_factory=list)
    reranked_tables: list[str] = field(default_factory=list)
    prompt_schema: str | None = None
    final_sql: str | None = None
    cache_hit: bool = False
    used_fallback: bool = False
    degradation_mode: str | None = None
    failure_category: str | None = None
    failure_stage: str | None = None
    retrieval_latency_ms: float | None = None
    stage_latencies: dict[str, float] = field(default_factory=dict)
    error_message: str | None = None
    created_at: str = field(default_factory=_utcnow)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RetrievalLogger:
    def __init__(self, max_entries: int = 500, repository: RAGQueryLogRepository | None = None):
        self.max_entries = max(10, int(max_entries))
        self._entries: deque[RetrievalLogEntry] = deque(maxlen=self.max_entries)
        self.repository = repository

    def record(self, entry: RetrievalLogEntry) -> None:
        self._entries.append(entry)
        if self.repository is not None:
            self.repository.upsert(**entry.to_dict())

    def attach_generation(
        self,
        query_id: str,
        *,
        prompt_schema: str | None = None,
        final_sql: str | None = None,
    ) -> None:
        for entry in reversed(self._entries):
            if entry.query_id != query_id:
                continue
            if prompt_schema:
                entry.prompt_schema = prompt_schema
            if final_sql:
                entry.final_sql = final_sql
            break
        if self.repository is not None:
            self.repository.attach_generation(query_id, prompt_schema=prompt_schema, final_sql=final_sql)

    def get(self, query_id: str) -> dict[str, Any] | None:
        if self.repository is not None:
            payload = self.repository.get(query_id)
            if payload is not None:
                return payload
        for entry in reversed(self._entries):
            if entry.query_id == query_id:
                return entry.to_dict()
        return None

    def list(self, *, connection_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if self.repository is not None:
            payload = self.repository.list(connection_id=connection_id, limit=limit)
            if payload:
                return payload
        items: list[dict[str, Any]] = []
        for entry in reversed(self._entries):
            if connection_id and entry.connection_id != connection_id:
                continue
            items.append(entry.to_dict())
            if len(items) >= max(1, limit):
                break
        return items

    def size(self) -> int:
        return len(self._entries)
