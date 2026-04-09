from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from app.agent.utils import table_name
from app.schemas.connection import TableSchema


def _normalize_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _column_payload(column: Any) -> dict[str, Any]:
    return {
        "name": _normalize_text(getattr(column, "name", "")),
        "type": _normalize_text(getattr(column, "type", "")),
        "nullable": bool(getattr(column, "nullable", True)),
        "default": _normalize_text(getattr(column, "default", "")),
        "comment": _normalize_text(getattr(column, "comment", "")),
        "is_primary_key": bool(getattr(column, "is_primary_key", False)),
        "is_foreign_key": bool(getattr(column, "is_foreign_key", False)),
        "foreign_table": _normalize_text(getattr(column, "foreign_table", "")),
    }


def _table_payload(table: Any) -> dict[str, Any]:
    columns = [_column_payload(column) for column in getattr(table, "columns", []) or []]
    columns.sort(key=lambda item: item["name"])
    return {
        "name": table_name(table),
        "comment": _normalize_text(getattr(table, "comment", None)),
        "description": _normalize_text(getattr(table, "description", None)),
        "columns": columns,
    }


def compute_schema_fingerprint(tables: Iterable[Any]) -> str:
    payload = [_table_payload(table) for table in tables]
    payload.sort(key=lambda item: item["name"])
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def compute_table_fingerprint(table: Any) -> str:
    payload = _table_payload(table)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class SchemaVersionRecord:
    connection_id: str
    version: str
    schema_fingerprint: str
    table_count: int
    table_fingerprints: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = self.created_at.isoformat()
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SchemaVersionRecord":
        payload = dict(data)
        created_at = payload.get("created_at")
        if isinstance(created_at, str):
            payload["created_at"] = datetime.fromisoformat(created_at)
        elif created_at is None:
            payload["created_at"] = datetime.now(timezone.utc)
        return cls(**payload)


class SchemaVersionManager:
    def __init__(self, storage_path: str | Path | None = None):
        self.storage_path = self._resolve_storage_path(storage_path)
        self._records: dict[str, list[SchemaVersionRecord]] = {}
        self._load()

    def compute_version(self, tables: Iterable[Any]) -> str:
        return compute_schema_fingerprint(tables)

    def save_version(
        self,
        connection_id: str,
        tables: Iterable[Any],
        *,
        version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SchemaVersionRecord:
        table_list = list(tables)
        schema_fingerprint = compute_schema_fingerprint(table_list)
        version = version or schema_fingerprint
        record = SchemaVersionRecord(
            connection_id=connection_id,
            version=version,
            schema_fingerprint=schema_fingerprint,
            table_count=len(table_list),
            table_fingerprints={table_name(table): compute_table_fingerprint(table) for table in table_list},
            metadata=dict(metadata or {}),
        )
        self._records.setdefault(connection_id, []).append(record)
        self._persist()
        return record

    def get_current_version(self, connection_id: str) -> SchemaVersionRecord | None:
        records = self._records.get(connection_id, [])
        return records[-1] if records else None

    def get_version_history(self, connection_id: str) -> list[SchemaVersionRecord]:
        return list(self._records.get(connection_id, []))

    def get_version(self, connection_id: str, version: str) -> SchemaVersionRecord | None:
        for record in self._records.get(connection_id, []):
            if record.version == version:
                return record
        return None

    def diff_versions(
        self,
        connection_id: str,
        *,
        left_version: str | None,
        right_version: str | None = None,
    ) -> dict[str, Any]:
        left = self.get_version(connection_id, left_version) if left_version else None
        right = self.get_version(connection_id, right_version) if right_version else self.get_current_version(connection_id)
        left_tables = dict(left.table_fingerprints) if left else {}
        right_tables = dict(right.table_fingerprints) if right else {}
        left_names = set(left_tables)
        right_names = set(right_tables)
        added = sorted(right_names - left_names)
        removed = sorted(left_names - right_names)
        changed = sorted(name for name in (left_names & right_names) if left_tables[name] != right_tables[name])
        unchanged = sorted(name for name in (left_names & right_names) if left_tables[name] == right_tables[name])
        return {
            "connection_id": connection_id,
            "left_version": left.version if left else None,
            "right_version": right.version if right else None,
            "added_tables": added,
            "removed_tables": removed,
            "changed_tables": changed,
            "unchanged_tables": unchanged,
        }

    def list_connections(self) -> list[str]:
        return sorted(self._records)

    def _resolve_storage_path(self, storage_path: str | Path | None) -> Path | None:
        if storage_path is None:
            return None
        path = Path(storage_path)
        if path.suffix:
            return path
        return path / "schema_versions.json"

    def _load(self) -> None:
        if self.storage_path is None or not self.storage_path.exists():
            return
        try:
            data = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except Exception:  # pragma: no cover - corrupted persistence falls back to empty state
            return
        for connection_id, items in (data or {}).items():
            self._records[connection_id] = [SchemaVersionRecord.from_dict(item) for item in items or []]

    def _persist(self) -> None:
        if self.storage_path is None:
            return
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            connection_id: [record.to_dict() for record in records]
            for connection_id, records in self._records.items()
        }
        tmp_path = self.storage_path.with_suffix(self.storage_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.storage_path)
