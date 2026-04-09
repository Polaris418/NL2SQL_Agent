from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
import json
import re

from app.agent.contracts import best_text_sample, dataclass_to_dict
from app.agent.utils import (
    column_comment,
    column_foreign_table,
    column_type,
    table_comment,
    table_description,
    table_name,
    tokenize_text,
)
from app.rag.business_knowledge import BusinessKnowledgeRepository
from app.rag.profiling import ProfilingStore


@dataclass(slots=True)
class FieldDocumentation:
    name: str
    type: str
    comment: str = ""
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_table: str | None = None
    is_time_field: bool = False
    is_metric_field: bool = False
    enum_values: list[str] = field(default_factory=list)
    sample_values: list[str] = field(default_factory=list)
    distinct_count: int | None = None
    null_ratio: float | None = None
    min_value: str | None = None
    max_value: str | None = None
    business_meaning: str | None = None


@dataclass(slots=True)
class JoinPath:
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    confidence: float = 0.5
    reason: str | None = None


@dataclass(slots=True)
class TableDocumentation:
    table_name: str
    business_summary: str
    columns: list[FieldDocumentation] = field(default_factory=list)
    join_paths: list[JoinPath] = field(default_factory=list)
    sample_values: list[str] = field(default_factory=list)
    domain_tags: list[str] = field(default_factory=list)
    table_category: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    version: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclass_to_dict(self)


class SchemaDocumentationManager:
    """Generate and maintain structured schema documentation."""

    def __init__(
        self,
        *,
        default_domain_tags: Iterable[str] | None = None,
        profiling_store: ProfilingStore | None = None,
        business_knowledge_repository: BusinessKnowledgeRepository | None = None,
    ):
        self.default_domain_tags = list(default_domain_tags or [])
        self.profiling_store = profiling_store or ProfilingStore()
        self.business_knowledge_repository = business_knowledge_repository or BusinessKnowledgeRepository()
        self._docs: dict[str, TableDocumentation] = {}

    def generate_documentation(
        self,
        table: Any,
        *,
        related_tables: Iterable[Any] | None = None,
        sample_values: Iterable[Any] | None = None,
        connection_id: str | None = None,
        database_name: str | None = None,
        schema_name: str | None = None,
        version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TableDocumentation:
        related_tables = list(related_tables or [])
        table_profile = self.profiling_store.get(connection_id).table(table_name(table)) if connection_id and self.profiling_store.get(connection_id) else None
        field_docs = [
            self._document_field(column, profile=table_profile.column(str(getattr(column, "name", ""))) if table_profile else None)
            for column in getattr(table, "columns", []) or []
        ]
        join_paths = self._infer_join_paths(table, related_tables)
        summary = self._build_summary(table, field_docs)
        sample_text = best_text_sample(sample_values or getattr(table, "sample_values", []) or [], limit=5)
        knowledge_items = self.business_knowledge_repository.query(
            connection_id=connection_id,
            table_name=table_name(table),
            limit=3,
        )
        knowledge_snippets = [item.content for item in knowledge_items]
        doc = TableDocumentation(
            table_name=table_name(table),
            business_summary=summary if not knowledge_snippets else f"{summary}. " + " ".join(knowledge_snippets[:2]),
            columns=field_docs,
            join_paths=join_paths,
            sample_values=sample_text,
            domain_tags=self._infer_domain_tags(table, field_docs),
            table_category=self._infer_table_category(table, field_docs),
            database_name=database_name,
            schema_name=schema_name,
            version=version,
            metadata={
                "table_object": table,
                "connection_id": connection_id,
                "knowledge_snippets": knowledge_snippets,
                "profile_row_count": getattr(table_profile, "row_count", None) if table_profile else None,
                **dict(metadata or {}),
            },
        )
        self._docs[doc.table_name] = doc
        return doc

    def generate_collection(self, tables: Iterable[Any], **kwargs: Any) -> list[TableDocumentation]:
        tables = list(tables)
        return [
            self.generate_documentation(
                table,
                related_tables=[candidate for candidate in tables if table_name(candidate) != table_name(table)],
                **kwargs,
            )
            for table in tables
        ]

    def update_documentation(self, table_name_value: str, **updates: Any) -> TableDocumentation | None:
        doc = self._docs.get(table_name_value)
        if doc is None:
            return None
        for key, value in updates.items():
            if hasattr(doc, key):
                setattr(doc, key, value)
        doc.updated_at = datetime.now(timezone.utc)
        self._docs[table_name_value] = doc
        return doc

    def get_documentation(self, table_name_value: str) -> TableDocumentation | None:
        return self._docs.get(table_name_value)

    def list_documentation(self) -> list[TableDocumentation]:
        return list(self._docs.values())

    def save(self, path: str) -> None:
        payload = [doc.to_dict() for doc in self._docs.values()]
        Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def load(self, path: str) -> list[TableDocumentation]:
        content = Path(path).read_text(encoding="utf-8")
        raw_items = json.loads(content)
        docs: list[TableDocumentation] = []
        self._docs.clear()
        for item in raw_items:
            doc = self._from_dict(item)
            self._docs[doc.table_name] = doc
            docs.append(doc)
        return docs

    def to_context_text(self, table_name_value: str) -> str:
        doc = self._docs.get(table_name_value)
        if doc is None:
            return ""
        parts = [f"Table: {doc.table_name}", f"Summary: {doc.business_summary}"]
        if doc.sample_values:
            parts.append("Samples: " + ", ".join(doc.sample_values[:5]))
        if doc.metadata.get("profile_row_count") is not None:
            parts.append(f"Profile: row_count={doc.metadata.get('profile_row_count')}")
        if doc.domain_tags:
            parts.append("Domains: " + ", ".join(doc.domain_tags))
        if doc.metadata.get("knowledge_snippets"):
            parts.append("Business Knowledge: " + " | ".join(doc.metadata.get("knowledge_snippets", [])[:2]))
        if doc.join_paths:
            joins = "; ".join(
                f"{path.source_table}.{path.source_column} -> {path.target_table}.{path.target_column}"
                for path in doc.join_paths[:5]
            )
            parts.append("Joins: " + joins)
        for column in doc.columns:
            column_bits = [f"{column.name} ({column.type})"]
            if column.business_meaning:
                column_bits.append(column.business_meaning)
            if column.enum_values:
                column_bits.append("enum=" + ",".join(column.enum_values[:5]))
            if column.sample_values:
                column_bits.append("samples=" + ",".join(column.sample_values[:3]))
            if column.distinct_count is not None:
                column_bits.append(f"distinct={column.distinct_count}")
            if column.null_ratio is not None:
                column_bits.append(f"null_ratio={round(column.null_ratio, 4)}")
            if column.min_value is not None or column.max_value is not None:
                column_bits.append(f"range={column.min_value or '?'}..{column.max_value or '?'}")
            parts.append(" | ".join(column_bits))
        return "\n".join(parts)

    def _document_field(self, column: Any, *, profile: Any | None = None) -> FieldDocumentation:
        name = str(getattr(column, "name", "") or "")
        declared_type = str(column_type(column) or "TEXT")
        comment = str(column_comment(column) or "")
        lower = f"{name} {comment} {declared_type}".lower()
        is_time_field = any(token in lower for token in ("date", "time", "created_at", "updated_at", "timestamp"))
        is_metric_field = any(token in lower for token in ("amount", "count", "total", "ratio", "rate", "score", "price"))
        enum_values = list(dict.fromkeys(best_text_sample(getattr(column, "enum_values", []) or [], limit=8)))
        profile_samples = list(getattr(profile, "sample_values", []) or []) if profile is not None else []
        sample_values = list(dict.fromkeys(best_text_sample((getattr(column, "sample_values", []) or []) or profile_samples, limit=5)))
        if not enum_values and sample_values and len(sample_values) <= 10:
            enum_values = sample_values[:5]
        business_meaning = comment or self._infer_meaning(name)
        return FieldDocumentation(
            name=name,
            type=declared_type,
            comment=comment,
            nullable=bool(getattr(column, "nullable", True)),
            is_primary_key=bool(getattr(column, "is_primary_key", False)),
            is_foreign_key=bool(getattr(column, "is_foreign_key", False)),
            foreign_table=column_foreign_table(column),
            is_time_field=is_time_field,
            is_metric_field=is_metric_field,
            enum_values=enum_values,
            sample_values=sample_values,
            distinct_count=getattr(profile, "distinct_count", None) if profile is not None else None,
            null_ratio=getattr(profile, "null_ratio", None) if profile is not None else None,
            min_value=getattr(profile, "min_value", None) if profile is not None else None,
            max_value=getattr(profile, "max_value", None) if profile is not None else None,
            business_meaning=business_meaning,
        )

    def _infer_join_paths(self, table: Any, related_tables: list[Any]) -> list[JoinPath]:
        source_table = table_name(table)
        related_lookup = {table_name(candidate): candidate for candidate in related_tables}
        join_paths: list[JoinPath] = []
        for column in getattr(table, "columns", []) or []:
            target_table = column_foreign_table(column)
            if target_table and target_table in related_lookup:
                join_paths.append(
                    JoinPath(
                        source_table=source_table,
                        source_column=str(getattr(column, "name", "")),
                        target_table=target_table,
                        target_column=self._guess_target_pk(related_lookup[target_table]) or "id",
                        confidence=0.95,
                        reason="foreign_key",
                    )
                )
                continue
            if str(getattr(column, "name", "")).lower().endswith("_id"):
                guessed = self._match_related_table(str(getattr(column, "name", "")), related_lookup)
                if guessed:
                    join_paths.append(
                        JoinPath(
                            source_table=source_table,
                            source_column=str(getattr(column, "name", "")),
                            target_table=guessed,
                            target_column=self._guess_target_pk(related_lookup[guessed]) or "id",
                            confidence=0.7,
                            reason="name_similarity",
                        )
                    )
        return join_paths

    def _infer_domain_tags(self, table: Any, columns: list[FieldDocumentation]) -> list[str]:
        tokens = set(tokenize_text(table_name(table)))
        tokens.update(tokenize_text(table_comment(table)))
        for column in columns:
            tokens.update(tokenize_text(column.name))
            tokens.update(tokenize_text(column.comment))
        tags = set(self.default_domain_tags)
        for token in tokens:
            if token in {"user", "customer", "member", "会员", "用户", "client"}:
                tags.add("user")
            if token in {"order", "orders", "purchase", "订单", "购买"}:
                tags.add("order")
            if token in {"sales", "sale", "revenue", "gmv", "销售", "金额"}:
                tags.add("sales")
            if token in {"dept", "department", "team", "organization", "部门", "组织"}:
                tags.add("organization")
            if token in {"product", "goods", "item", "商品", "产品"}:
                tags.add("product")
        return sorted(tags)

    def _infer_table_category(self, table: Any, columns: list[FieldDocumentation]) -> str:
        metrics = sum(1 for column in columns if column.is_metric_field)
        foreign_keys = sum(1 for column in columns if column.is_foreign_key)
        dimensions = sum(1 for column in columns if not column.is_metric_field and not column.is_foreign_key)
        if metrics >= max(1, dimensions) and foreign_keys >= 1:
            return "fact_table"
        if dimensions >= metrics and foreign_keys <= 1:
            return "dimension_table"
        return "mixed_table"

    def _build_summary(self, table: Any, columns: list[FieldDocumentation]) -> str:
        summary = table_comment(table) or table_description(table)
        if summary:
            return summary
        pieces = [f"Table {table_name(table)}"]
        field_names = ", ".join(column.name for column in columns[:8])
        if field_names:
            pieces.append(f"fields: {field_names}")
        return ". ".join(pieces)

    @staticmethod
    def _guess_target_pk(table: Any) -> str | None:
        for column in getattr(table, "columns", []) or []:
            if bool(getattr(column, "is_primary_key", False)) or str(getattr(column, "name", "")).lower() == "id":
                return str(getattr(column, "name", ""))
        return None

    @staticmethod
    def _match_related_table(column_name: str, related_lookup: dict[str, Any]) -> str | None:
        base = re.sub(r"_id$", "", column_name.lower()).strip("_")
        if not base:
            return None

        def variants(value: str) -> set[str]:
            items = {
                value,
                f"{value}s",
                f"{value}_info",
                f"t_{value}",
                f"t_{value}s",
                f"dim_{value}",
                f"fact_{value}",
                f"tbl_{value}",
            }
            return {item.strip("_") for item in items if item}

        targets = variants(base)
        scored: list[tuple[int, str]] = []
        for candidate in related_lookup.keys():
            normalized = candidate.lower().strip("_")
            score = 0
            if normalized in targets:
                score = 100
            elif any(normalized.endswith(target) or normalized.startswith(target + '_') for target in targets):
                score = 80
            elif any(target in normalized for target in targets):
                score = 60
            elif normalized.replace('t_', '', 1) == base:
                score = 90
            if score > 0:
                scored.append((score, candidate))
        if not scored:
            return None
        scored.sort(key=lambda item: (-item[0], len(item[1]), item[1]))
        return scored[0][1]

    @staticmethod
    def _infer_meaning(name: str) -> str:
        lowered = name.lower()
        if lowered.endswith("_id"):
            return "identifier"
        if any(token in lowered for token in ("amount", "price", "total", "score")):
            return "metric"
        if any(token in lowered for token in ("date", "time", "timestamp")):
            return "time field"
        return "business field"

    @staticmethod
    def _from_dict(data: dict[str, Any]) -> TableDocumentation:
        columns = [FieldDocumentation(**item) for item in data.get("columns", [])]
        joins = [JoinPath(**item) for item in data.get("join_paths", [])]
        updated_at = data.get("updated_at")
        return TableDocumentation(
            table_name=data.get("table_name", ""),
            business_summary=data.get("business_summary", ""),
            columns=columns,
            join_paths=joins,
            sample_values=list(data.get("sample_values") or []),
            domain_tags=list(data.get("domain_tags") or []),
            table_category=data.get("table_category"),
            database_name=data.get("database_name"),
            schema_name=data.get("schema_name"),
            version=data.get("version"),
            updated_at=datetime.fromisoformat(updated_at) if isinstance(updated_at, str) else datetime.now(timezone.utc),
            metadata=dict(data.get("metadata") or {}),
        )
