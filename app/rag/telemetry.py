from __future__ import annotations

from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.db.repositories.rag_telemetry_repo import RAGTelemetryRepository
from app.rag.telemetry_store import TelemetryEventRecord, TelemetryStore


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class RetrievalTelemetryEvent:
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
    selected_tables: list[str] = field(default_factory=list)
    degradation_mode: str | None = None
    error_message: str | None = None
    retrieval_backend: str | None = None
    embedding_backend: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_utcnow)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ContextLimitTelemetryEvent:
    query_id: str | None
    connection_id: str | None
    limit_reason: str | None = None
    truncated: bool = False
    budget: dict[str, Any] = field(default_factory=dict)
    original_char_count: int = 0
    original_token_count: int = 0
    final_char_count: int = 0
    final_token_count: int = 0
    dropped_tables: list[str] = field(default_factory=list)
    dropped_columns: dict[str, list[str]] = field(default_factory=dict)
    dropped_relationship_clues: list[dict[str, Any]] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_utcnow)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class TelemetrySystem:
    def __init__(
        self,
        max_events: int = 5000,
        *,
        snapshot_interval: int = 25,
        repository: RAGTelemetryRepository | None = None,
        persist_path: str | None = None,
    ):
        self.max_events = max(100, int(max_events))
        self.snapshot_interval = max(1, int(snapshot_interval))
        self.repository = repository
        self.store = TelemetryStore(max_events=self.max_events, persist_path=persist_path)
        self._events: deque[RetrievalTelemetryEvent] = deque(maxlen=self.max_events)
        self._context_limit_events: deque[ContextLimitTelemetryEvent] = deque(maxlen=self.max_events)
        self._error_counter: Counter[str] = Counter()
        self._since_last_snapshot = 0
        self._last_snapshot_at: str | None = None

    def record_retrieval(self, event: RetrievalTelemetryEvent) -> None:
        self._events.append(event)
        self.store.record(
            TelemetryEventRecord(
                event_type="retrieval",
                connection_id=event.connection_id,
                query_id=event.query_id,
                category=event.error_type,
                payload=event.to_dict(),
            )
        )
        if event.error_type and self.repository is None:
            self._error_counter[event.error_type] += 1
        if self.repository is not None:
            payload = dict(event.payload or {})
            if event.degradation_mode:
                payload.setdefault("degradation_mode", event.degradation_mode)
            if event.error_message:
                payload.setdefault("error_message", event.error_message)
            if event.retrieval_backend:
                payload.setdefault("retrieval_backend", event.retrieval_backend)
            if event.embedding_backend:
                payload.setdefault("embedding_backend", event.embedding_backend)
            self.repository.add_event(
                query_id=event.query_id,
                connection_id=event.connection_id,
                retrieval_latency_ms=event.retrieval_latency_ms,
                embedding_latency_ms=event.embedding_latency_ms,
                lexical_count=event.lexical_count,
                vector_count=event.vector_count,
                cache_hit=event.cache_hit,
                used_fallback=event.used_fallback,
                error_type=event.error_type,
                failure_stage=event.failure_stage,
                selected_tables=event.selected_tables,
                payload=payload,
            )
        self._since_last_snapshot += 1
        if self._since_last_snapshot >= self.snapshot_interval:
            self.flush_snapshot()

    def record_context_limit(self, event: ContextLimitTelemetryEvent) -> None:
        self._context_limit_events.append(event)
        self.store.record(
            TelemetryEventRecord(
                event_type="context_limit",
                connection_id=event.connection_id,
                query_id=event.query_id,
                category=event.limit_reason,
                payload=event.to_dict(),
            )
        )
        if self.repository is not None:
            add_context_limit = getattr(self.repository, "add_context_limit", None)
            if callable(add_context_limit):
                add_context_limit(
                    query_id=event.query_id,
                    connection_id=event.connection_id,
                    limit_reason=event.limit_reason,
                    truncated=event.truncated,
                    budget=event.budget,
                    original_char_count=event.original_char_count,
                    original_token_count=event.original_token_count,
                    final_char_count=event.final_char_count,
                    final_token_count=event.final_token_count,
                    dropped_tables=event.dropped_tables,
                    dropped_columns=event.dropped_columns,
                    dropped_relationship_clues=event.dropped_relationship_clues,
                    payload=event.payload,
                )
        elif event.limit_reason:
            self._error_counter[f"context_limit:{event.limit_reason}"] += 1

    def record_error(self, error_type: str) -> None:
        if error_type:
            self._error_counter[error_type] += 1
            self.store.record(
                TelemetryEventRecord(
                    event_type="error",
                    category=error_type,
                    payload={"error_type": error_type},
                )
            )

    def get_metrics(self) -> dict[str, Any]:
        if self.repository is not None:
            metrics = dict(self.repository.get_metrics())
            if self._error_counter:
                failure_categories = Counter(metrics.get("failure_categories") or {})
                for key, value in self._error_counter.items():
                    failure_categories[key] += int(value)
                metrics["failure_categories"] = dict(failure_categories)
            metrics["last_snapshot_at"] = metrics.get("last_snapshot_at") or self._last_snapshot_at
            return metrics

        def _value(event: Any, name: str, default: Any = 0) -> Any:
            if isinstance(event, dict):
                return event.get(name, default)
            return getattr(event, name, default)

        if self._events:
            events: list[Any] = list(self._events)
            failure_categories = Counter(self._error_counter)
            context_limit_count = len(self._context_limit_events)
        else:
            events = self.store.list(event_type="retrieval", limit=self.max_events)
            failure_categories = Counter({str(key): int(value) for key, value in self.store.snapshot().get("category_counts", {}).items()})
            context_limit_count = len(self.store.list(event_type="context_limit", limit=self.max_events))

        if not events:
            return {
                "logged_queries": 0,
                "retrieval_p50_ms": 0.0,
                "retrieval_p95_ms": 0.0,
                "retrieval_p99_ms": 0.0,
                "embedding_p50_ms": 0.0,
                "embedding_p95_ms": 0.0,
                "embedding_p99_ms": 0.0,
                "vector_hit_rate": 0.0,
                "bm25_hit_rate": 0.0,
                "fallback_rate": 0.0,
                "table_not_found_rate": 0.0,
                "timeout_rate": 0.0,
                "concurrency_rejection_rate": 0.0,
                "failure_categories": dict(failure_categories),
                "context_limit_events": context_limit_count,
                "context_limit_rate": 0.0,
                "last_snapshot_at": self._last_snapshot_at,
            }

        retrieval_latencies = [max(0.0, float(_value(event, "retrieval_latency_ms", 0.0) or 0.0)) for event in events]
        embedding_latencies = [max(0.0, float(_value(event, "embedding_latency_ms", 0.0) or 0.0)) for event in events]
        total = len(events)
        table_not_found = int(failure_categories.get("table_not_found", 0) or 0) + int(failure_categories.get("database_mismatch", 0) or 0)
        timeout_count = sum(int(failure_categories.get(key, 0) or 0) for key in ("timeout_error", "retrieval_timeout", "embedding_timeout", "reranker_timeout"))
        return {
            "logged_queries": total,
            "retrieval_p50_ms": self._percentile(retrieval_latencies, 50),
            "retrieval_p95_ms": self._percentile(retrieval_latencies, 95),
            "retrieval_p99_ms": self._percentile(retrieval_latencies, 99),
            "embedding_p50_ms": self._percentile(embedding_latencies, 50),
            "embedding_p95_ms": self._percentile(embedding_latencies, 95),
            "embedding_p99_ms": self._percentile(embedding_latencies, 99),
            "vector_hit_rate": round(sum(1 for event in events if int(_value(event, "vector_count", 0) or 0) > 0) / total, 4),
            "bm25_hit_rate": round(sum(1 for event in events if int(_value(event, "lexical_count", 0) or 0) > 0) / total, 4),
            "fallback_rate": round(sum(1 for event in events if bool(_value(event, "used_fallback", False))) / total, 4),
            "table_not_found_rate": round(table_not_found / total, 4),
            "timeout_rate": round(timeout_count / total, 4),
            "concurrency_rejection_rate": round(int(failure_categories.get("concurrency_limit", 0) or 0) / total, 4),
            "failure_categories": dict(failure_categories),
            "context_limit_events": context_limit_count,
            "context_limit_rate": round(context_limit_count / total, 4),
            "last_snapshot_at": self._last_snapshot_at,
        }

    def flush_snapshot(self, *, force: bool = False) -> dict[str, Any]:
        metrics = self.get_metrics()
        if self.repository is not None and (force or self._since_last_snapshot > 0):
            self.repository.add_snapshot(metrics)
            self._last_snapshot_at = _utcnow()
            self._since_last_snapshot = 0
            metrics["last_snapshot_at"] = self._last_snapshot_at
        return metrics

    def list_recent_events(self, *, limit: int = 25, connection_id: str | None = None) -> list[dict[str, Any]]:
        if self.repository is not None:
            return self.repository.list_events(connection_id=connection_id, limit=limit)
        items: list[dict[str, Any]] = []
        for event in reversed(self._events):
            if connection_id and event.connection_id != connection_id:
                continue
            items.append(event.to_dict())
            if len(items) >= max(1, int(limit)):
                break
        return items

    def list_snapshot_history(self, *, limit: int = 25) -> list[dict[str, Any]]:
        if self.repository is not None:
            return self.repository.list_snapshots(limit=limit)
        return []

    def dashboard(self) -> dict[str, Any]:
        latest_snapshot = self.repository.latest_snapshot() if self.repository is not None else None
        context_limits = self.list_context_limit_events(limit=10)
        return {
            "current": self.get_metrics(),
            "latest_snapshot": latest_snapshot,
            "recent_events": self.list_recent_events(limit=10),
            "history_count": len(self.list_snapshot_history(limit=100)),
            "store": self.store.snapshot(),
            "context_limit_events": context_limits,
        }

    def snapshot(self) -> dict[str, Any]:
        latest_snapshot = self.repository.latest_snapshot() if self.repository is not None else None
        context_limits = self.list_context_limit_events(limit=10)
        return {
            "current": self.get_metrics(),
            "latest_snapshot": latest_snapshot,
            "last_snapshot_at": self._last_snapshot_at,
            "store": self.store.snapshot(),
            "context_limit_events": context_limits,
        }

    def list_events(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if self.repository is not None:
            return self.repository.list_events(connection_id=connection_id, query_id=query_id, limit=limit)
        events = self.list_recent_events(limit=limit, connection_id=connection_id)
        if query_id:
            events = [item for item in events if item.get("query_id") == query_id]
        return events[: max(1, int(limit))]

    def get_event(self, query_id: str) -> dict[str, Any] | None:
        if self.repository is not None:
            return self.repository.get_event(query_id)
        items = self.list_events(query_id=query_id, limit=1)
        return items[0] if items else None

    def list_context_limit_events(self, *, limit: int = 25, connection_id: str | None = None) -> list[dict[str, Any]]:
        if self.repository is not None:
            getter = getattr(self.repository, "list_context_limits", None)
            if callable(getter):
                return getter(connection_id=connection_id, limit=limit)
        items: list[dict[str, Any]] = []
        for event in reversed(self._context_limit_events):
            if connection_id and event.connection_id != connection_id:
                continue
            items.append(event.to_dict())
            if len(items) >= max(1, int(limit)):
                break
        return items

    @staticmethod
    def _percentile(values: list[float], percentile: int) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = max(0, min(len(ordered) - 1, round((percentile / 100) * (len(ordered) - 1))))
        return round(float(ordered[index]), 2)
