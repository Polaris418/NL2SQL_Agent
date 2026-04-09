from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping, Sequence


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class ColumnProfile:
    """Lightweight profiling summary for a single column.

    The structure is intentionally simple so it can be embedded in schema
    documentation, context packing, or telemetry without pulling in extra
    dependencies. Every statistic is optional so callers can populate only the
    fields they actually have.
    """

    column_name: str
    data_type: str | None = None
    sample_values: list[str] = field(default_factory=list)
    distinct_count: int | None = None
    null_ratio: float | None = None
    min_value: str | None = None
    max_value: str | None = None
    value_count: int | None = None
    comment: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def has_numeric_bounds(self) -> bool:
        return self.min_value is not None or self.max_value is not None

    @classmethod
    def from_samples(
        cls,
        column_name: str,
        samples: Iterable[Any],
        *,
        data_type: str | None = None,
        comment: str | None = None,
        max_samples: int = 8,
        treat_as_numeric: bool | None = None,
    ) -> "ColumnProfile":
        raw_samples = list(samples)
        sample_values = _normalize_samples(raw_samples, max_samples=max_samples)
        distinct_count = len(set(sample_values)) if sample_values else 0
        value_count = len(sample_values)
        null_count = sum(1 for value in raw_samples if value is None)
        null_ratio = (null_count / len(raw_samples)) if raw_samples else None

        min_value: str | None = None
        max_value: str | None = None
        numeric_candidates = [_coerce_float(item) for item in sample_values]
        numeric_candidates = [item for item in numeric_candidates if item is not None]
        looks_numeric = treat_as_numeric if treat_as_numeric is not None else bool(numeric_candidates)

        if looks_numeric:
            if numeric_candidates:
                min_value = _stringify_number(min(numeric_candidates))
                max_value = _stringify_number(max(numeric_candidates))
        else:
            if sample_values:
                min_value = min(sample_values)
                max_value = max(sample_values)

        return cls(
            column_name=column_name,
            data_type=data_type,
            sample_values=sample_values,
            distinct_count=distinct_count,
            null_ratio=null_ratio,
            min_value=min_value,
            max_value=max_value,
            value_count=value_count,
            comment=comment,
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ColumnProfile":
        return cls(
            column_name=_normalize_text(payload.get("column_name") or payload.get("name")),
            data_type=payload.get("data_type") or payload.get("type"),
            sample_values=_normalize_samples(payload.get("sample_values") or payload.get("samples") or []),
            distinct_count=_coerce_int(payload.get("distinct_count")),
            null_ratio=_coerce_float(payload.get("null_ratio")),
            min_value=_normalize_text(payload.get("min_value")) or None,
            max_value=_normalize_text(payload.get("max_value")) or None,
            value_count=_coerce_int(payload.get("value_count")),
            comment=_normalize_text(payload.get("comment")) or None,
            extra=dict(payload.get("extra") or {}),
        )


@dataclass(slots=True)
class TableProfile:
    """Profiling summary for a table."""

    table_name: str
    columns: list[ColumnProfile] = field(default_factory=list)
    row_count: int | None = None
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["columns"] = [column.to_dict() for column in self.columns]
        return payload

    def column(self, column_name: str) -> ColumnProfile | None:
        normalized = _normalize_text(column_name).lower()
        for column in self.columns:
            if column.column_name.lower() == normalized:
                return column
        return None


@dataclass(slots=True)
class ProfilingSnapshot:
    connection_id: str
    database_name: str | None = None
    schema_name: str | None = None
    tables: list[TableProfile] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tables"] = [table.to_dict() for table in self.tables]
        return payload

    def table(self, table_name: str) -> TableProfile | None:
        normalized = _normalize_text(table_name).lower()
        for table in self.tables:
            if table.table_name.lower() == normalized:
                return table
        return None


class ProfilingStore:
    """In-memory store for profiling snapshots.

    The store is intentionally minimal and suitable for plugging into schema
    documentation generation or telemetry collection. It can be replaced by a
    persistent backend later without changing the profiling model.
    """

    def __init__(self) -> None:
        self._snapshots: dict[str, ProfilingSnapshot] = {}

    def upsert(self, snapshot: ProfilingSnapshot) -> ProfilingSnapshot:
        self._snapshots[snapshot.connection_id] = snapshot
        return snapshot

    def get(self, connection_id: str) -> ProfilingSnapshot | None:
        return self._snapshots.get(connection_id)

    def delete(self, connection_id: str) -> bool:
        return self._snapshots.pop(connection_id, None) is not None

    def list(self) -> list[ProfilingSnapshot]:
        return list(self._snapshots.values())

    def query(
        self,
        *,
        connection_id: str | None = None,
        table_name: str | None = None,
    ) -> list[TableProfile]:
        tables: list[TableProfile] = []
        connection_ids = [connection_id] if connection_id else list(self._snapshots)
        target_table = _normalize_text(table_name).lower() if table_name else None
        for item in connection_ids:
            snapshot = self._snapshots.get(item)
            if snapshot is None:
                continue
            for table in snapshot.tables:
                if target_table and table.table_name.lower() != target_table:
                    continue
                tables.append(table)
        return tables


def _normalize_samples(samples: Iterable[Any], *, max_samples: int = 8) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in samples:
        text = _normalize_text(value)
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
        if len(normalized) >= max(1, int(max_samples)):
            break
    return normalized


def _stringify_number(value: float | int) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


__all__ = [
    "ColumnProfile",
    "ProfilingSnapshot",
    "ProfilingStore",
    "TableProfile",
]
