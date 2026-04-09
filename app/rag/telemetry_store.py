from __future__ import annotations

from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class TelemetryEventRecord:
    event_type: str
    connection_id: str | None = None
    query_id: str | None = None
    category: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_utcnow)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class TelemetryStore:
    def __init__(self, *, max_events: int = 5000, persist_path: str | None = None):
        self.max_events = max(100, int(max_events))
        self.persist_path = Path(persist_path) if persist_path else None
        self._events: deque[TelemetryEventRecord] = deque(maxlen=self.max_events)
        self._counts: Counter[str] = Counter()
        self._categories: Counter[str] = Counter()
        if self.persist_path is not None:
            self._load()

    def record(self, record: TelemetryEventRecord) -> None:
        self._events.append(record)
        self._counts[record.event_type] += 1
        if record.category:
            self._categories[record.category] += 1
        if self.persist_path is not None:
            self.persist_path.parent.mkdir(parents=True, exist_ok=True)
            with self.persist_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")

    def list(
        self,
        *,
        event_type: str | None = None,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        items = list(self._events)
        if event_type:
            items = [item for item in items if item.event_type == event_type]
        if connection_id:
            items = [item for item in items if item.connection_id == connection_id]
        if query_id:
            items = [item for item in items if item.query_id == query_id]
        items = list(reversed(items))
        return [item.to_dict() for item in items[: max(1, limit)]]

    def snapshot(self) -> dict[str, Any]:
        events = list(self._events)
        return {
            "total_events": len(events),
            "event_counts": dict(self._counts),
            "category_counts": dict(self._categories),
            "recent_events": [event.to_dict() for event in list(reversed(events))[:20]],
        }

    def _load(self) -> None:
        if self.persist_path is None or not self.persist_path.exists():
            return
        try:
            for line in self.persist_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                payload = json.loads(line)
                record = TelemetryEventRecord(
                    event_type=str(payload.get("event_type") or "unknown"),
                    connection_id=payload.get("connection_id"),
                    query_id=payload.get("query_id"),
                    category=payload.get("category"),
                    payload=dict(payload.get("payload") or {}),
                    created_at=str(payload.get("created_at") or _utcnow()),
                )
                self._events.append(record)
                self._counts[record.event_type] += 1
                if record.category:
                    self._categories[record.category] += 1
        except Exception:
            self._events.clear()
            self._counts.clear()
            self._categories.clear()

