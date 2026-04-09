from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.db.metadata import MetadataDB


class RepositoryBase:
    def __init__(self, metadata_db: MetadataDB):
        self.metadata_db = metadata_db
        self.metadata_db.initialize()

    @staticmethod
    def utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def dumps(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, default=str)

    @staticmethod
    def loads(payload: str | None, default: Any = None) -> Any:
        if not payload:
            return default
        return json.loads(payload)

    def fetch_one(self, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.metadata_db.connect() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

    def fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.metadata_db.connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
