from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional
import json
import re
import uuid


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_id(prefix: str = "") -> str:
    value = uuid.uuid4().hex
    return f"{prefix}{value}" if prefix else value


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def strip_code_fences(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_+-]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def safe_json_dumps(value: Any, *, ensure_ascii: bool = False) -> str:
    def default(obj: Any) -> Any:
        if is_dataclass(obj):
            return dataclass_to_dict(obj)
        if isinstance(obj, set):
            return sorted(obj)
        if isinstance(obj, tuple):
            return list(obj)
        if hasattr(obj, "to_dict") and callable(obj.to_dict):
            return obj.to_dict()
        return str(obj)

    return json.dumps(value, ensure_ascii=ensure_ascii, default=default)


def dataclass_to_dict(value: Any) -> Any:
    if is_dataclass(value):
        return {
            key: dataclass_to_dict(item)
            for key, item in asdict(value).items()
        }
    if isinstance(value, dict):
        return {key: dataclass_to_dict(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [dataclass_to_dict(item) for item in value]
    return value


def tokenize_text(text: str) -> list[str]:
    text = (text or "").lower()
    text = re.sub(r"[\u3000-\u303f\uff00-\uffef]", " ", text)
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text)
    tokens = [token for token in text.split() if token]
    return tokens


def contains_any(text: str, keywords: Iterable[str]) -> bool:
    lower = (text or "").lower()
    return any(keyword.lower() in lower for keyword in keywords)


def best_text_sample(values: Iterable[Any], limit: int = 3) -> list[str]:
    samples: list[str] = []
    for value in values:
        if value is None:
            continue
        sample = str(value).strip()
        if sample and sample not in samples:
            samples.append(sample)
        if len(samples) >= limit:
            break
    return samples


@dataclass(slots=True)
class Message:
    role: str
    content: str

    def to_dict(self) -> dict[str, str]:
        return {"role": self.role, "content": self.content}


@dataclass(slots=True)
class ColumnInfo:
    name: str
    type: str = "TEXT"
    comment: str = ""
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_table: Optional[str] = None
    default: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclass_to_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnInfo":
        return cls(
            name=data.get("name", ""),
            type=data.get("type", "TEXT"),
            comment=data.get("comment", ""),
            nullable=bool(data.get("nullable", True)),
            is_primary_key=bool(data.get("is_primary_key", False)),
            is_foreign_key=bool(data.get("is_foreign_key", False)),
            foreign_table=data.get("foreign_table"),
            default=data.get("default"),
            metadata=dict(data.get("metadata") or {}),
        )

    @classmethod
    def from_any(cls, value: Any) -> "ColumnInfo":
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls.from_dict(value)
        return cls(
            name=getattr(value, "name", "") or "",
            type=getattr(value, "type", None) or getattr(value, "data_type", None) or "TEXT",
            comment=getattr(value, "comment", None) or "",
            nullable=bool(getattr(value, "nullable", True)),
            is_primary_key=bool(getattr(value, "is_primary_key", False) or getattr(value, "primary_key", False)),
            is_foreign_key=bool(getattr(value, "is_foreign_key", False) or getattr(value, "foreign_key", None) or getattr(value, "foreign_key_reference", None)),
            foreign_table=getattr(value, "foreign_table", None) or getattr(value, "foreign_key_reference", None),
            default=getattr(value, "default", None),
            metadata=dict(getattr(value, "metadata", {}) or {}),
        )


@dataclass(slots=True)
class TableSchema:
    connection_id: str
    table_name: str
    table_comment: str = ""
    columns: list[ColumnInfo] = field(default_factory=list)
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclass_to_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TableSchema":
        columns = [ColumnInfo.from_any(item) for item in data.get("columns") or []]
        return cls(
            connection_id=data.get("connection_id", ""),
            table_name=data.get("table_name", ""),
            table_comment=data.get("table_comment", ""),
            columns=columns,
            description=data.get("description", ""),
            metadata=dict(data.get("metadata") or {}),
        )

    @classmethod
    def from_any(cls, value: Any) -> "TableSchema":
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls.from_dict(value)
        columns = [ColumnInfo.from_any(item) for item in getattr(value, "columns", []) or []]
        return cls(
            connection_id=getattr(value, "connection_id", "") or "",
            table_name=getattr(value, "table_name", "") or getattr(value, "name", "") or "",
            table_comment=getattr(value, "table_comment", None) or getattr(value, "comment", "") or "",
            columns=columns,
            description=getattr(value, "description", None) or "",
            metadata=dict(getattr(value, "metadata", {}) or {}),
        )

    @property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]

    def compact_description(self) -> str:
        parts = [
            f"table={self.table_name}",
            f"comment={self.table_comment}" if self.table_comment else "",
            f"description={self.description}" if self.description else "",
        ]
        column_bits = []
        for column in self.columns:
            flags = []
            if column.is_primary_key:
                flags.append("pk")
            if column.is_foreign_key:
                target = f"->{column.foreign_table}" if column.foreign_table else ""
                flags.append(f"fk{target}")
            flags_text = f" ({', '.join(flags)})" if flags else ""
            detail = f"{column.name}:{column.type}{flags_text}"
            if column.comment:
                detail = f"{detail} - {column.comment}"
            column_bits.append(detail)
        parts.append("columns=" + "; ".join(column_bits))
        return normalize_whitespace(" | ".join(bit for bit in parts if bit))


@dataclass(slots=True)
class QueryStep:
    step_type: str
    content: str
    timestamp: str = field(default_factory=utc_now_iso)
    input: str = ""
    output: str = ""
    order: int = 0

    def to_dict(self) -> dict[str, Any]:
        return dataclass_to_dict(self)

    @property
    def type(self) -> str:
        return self.step_type


@dataclass(slots=True)
class ChartSuggestion:
    type: str = "table"
    x_field: Optional[str] = None
    y_field: Optional[str] = None
    reason: str = ""
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return dataclass_to_dict(self)

    @property
    def chart_type(self) -> str:
        return self.type

    @property
    def x_axis(self) -> Optional[str]:
        return self.x_field

    @property
    def y_axis(self) -> Optional[str]:
        return self.y_field


@dataclass(slots=True)
class ExecutionResult:
    success: bool
    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    row_count: int = 0
    error_message: str = ""
    execution_ms: int = 0
    truncated: bool = False
    sanitized_sql: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclass_to_dict(self)


@dataclass(slots=True)
class QueryResult:
    query_id: str
    status: str
    sql: str = ""
    sql_attempts: int = 1
    results: dict[str, Any] = field(default_factory=dict)
    chart_suggestion: ChartSuggestion = field(default_factory=ChartSuggestion)
    steps: list[QueryStep] = field(default_factory=list)
    latency: dict[str, int] = field(default_factory=dict)
    error_message: str = ""
    rewritten_query: str = ""
    retrieved_tables: list[str] = field(default_factory=list)
    connection_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return dataclass_to_dict(self)


def coerce_table_schema_list(tables: Iterable[Any]) -> list[TableSchema]:
    result: list[TableSchema] = []
    for table in tables:
        try:
            result.append(TableSchema.from_any(table))
        except Exception:
            continue
    return result


def render_schema_context(tables: Iterable[TableSchema]) -> str:
    rendered = []
    for table in tables:
        rendered.append(table.compact_description())
    return "\n".join(rendered)
