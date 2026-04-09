from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from app.agent.contracts import tokenize_text
from app.agent.utils import (
    column_comment,
    column_foreign_table,
    table_comment,
    table_description,
    table_name,
)
from app.rag.context_limiter import SchemaContextLimiter
from app.schemas.connection import TableSchema
from app.rag.schema_doc import TableDocumentation


@dataclass(slots=True)
class PackedColumnContext:
    name: str
    score: float
    reason: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_time_field: bool = False
    is_metric_field: bool = False
    foreign_table: str | None = None
    comment: str = ""


@dataclass(slots=True)
class PackedTableContext:
    table_name: str
    table_type: str | None
    summary: str
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[str] = field(default_factory=list)
    columns: list[PackedColumnContext] = field(default_factory=list)
    join_hints: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class PackedSchemaContext:
    question: str
    db_type: str
    tables: list[PackedTableContext] = field(default_factory=list)
    relationship_clues: list[dict[str, Any]] = field(default_factory=list)
    column_annotations: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    packed_text: str = ""
    truncated: bool = False
    char_count: int = 0
    token_count: int = 0
    limit_reason: str | None = None
    budget: dict[str, int] = field(default_factory=dict)
    limiter_metadata: dict[str, Any] = field(default_factory=dict)
    dropped_tables: list[str] = field(default_factory=list)
    dropped_columns: dict[str, list[str]] = field(default_factory=dict)
    dropped_relationship_clues: list[dict[str, Any]] = field(default_factory=list)


class SchemaContextPacker:
    """Compress schema context for SQL generation."""

    def __init__(
        self,
        *,
        max_tables: int = 8,
        max_columns_per_table: int = 6,
        max_relationship_clues: int = 6,
        max_chars: int = 7000,
        max_tokens: int = 1800,
        limiter: SchemaContextLimiter | None = None,
    ):
        self.max_tables = max(1, int(max_tables))
        self.max_columns_per_table = max(1, int(max_columns_per_table))
        self.max_relationship_clues = max(1, int(max_relationship_clues))
        self.max_chars = max(1, int(max_chars))
        self.max_tokens = max(1, int(max_tokens))
        self.limiter = limiter or SchemaContextLimiter(
            max_tables=self.max_tables,
            max_columns_per_table=self.max_columns_per_table,
            max_relationship_clues=self.max_relationship_clues,
            max_chars=self.max_chars,
            max_tokens=self.max_tokens,
        )

    def pack(
        self,
        question: str,
        tables: list[TableSchema],
        *,
        db_type: str,
        relationship_clues: list[dict[str, Any]] | None = None,
        column_annotations: dict[str, list[dict[str, Any]]] | None = None,
        documents: Iterable[TableDocumentation] | dict[str, TableDocumentation] | None = None,
        max_tables: int | None = None,
        max_columns_per_table: int | None = None,
        max_relationship_clues: int | None = None,
        max_chars: int | None = None,
        max_tokens: int | None = None,
    ) -> PackedSchemaContext:
        relationship_clues = self._normalize_clues(relationship_clues)
        column_annotations = dict(column_annotations or {})
        document_lookup = self._normalize_documents(documents)
        table_lookup = {table_name(table): table for table in tables}
        ranked_tables = self._rank_tables(question, tables, relationship_clues, column_annotations, document_lookup)
        selected_tables = ranked_tables[: max(1, max_tables or self.max_tables)]
        selected_names = {table_name(table) for table in selected_tables}

        packed_tables: list[PackedTableContext] = []
        for table in selected_tables:
            table_id = table_name(table)
            doc = document_lookup.get(table_id)
            selected_columns = self._select_columns(
                question,
                table,
                annotations=column_annotations.get(table_id, []),
                document=doc,
                max_columns_per_table=max_columns_per_table or self.max_columns_per_table,
                relationship_clues=relationship_clues,
            )
            join_hints = self._select_join_hints(table_id, relationship_clues, table_lookup)
            packed_tables.append(
                PackedTableContext(
                    table_name=table_id,
                    table_type=self._table_type(table, doc),
                    summary=self._table_summary(table, doc),
                    primary_keys=[column.name for column in self._iter_columns(table, doc) if self._is_primary_key(column)],
                    foreign_keys=[
                        self._format_foreign_key(column)
                        for column in self._iter_columns(table, doc)
                        if self._is_foreign_key(column)
                    ],
                    columns=selected_columns,
                    join_hints=join_hints,
                )
            )

        relevant_relationships = [
            clue
            for clue in relationship_clues
            if clue.get("source_table") in selected_names or clue.get("target_table") in selected_names
        ]
        relevant_relationships = relevant_relationships[: max(1, max_relationship_clues or self.max_relationship_clues)]
        packed_text = self._render_packed_text(
            question=question,
            db_type=db_type,
            tables=packed_tables,
            relationship_clues=relevant_relationships,
            max_chars=max_chars or self.max_chars,
        )

        packed_context = PackedSchemaContext(
            question=question,
            db_type=db_type,
            tables=packed_tables,
            relationship_clues=relevant_relationships,
            column_annotations=column_annotations,
            packed_text=packed_text,
            truncated=False,
            char_count=len(packed_text),
            token_count=len(tokenize_text(packed_text)),
            budget={
                "max_tables": max_tables or self.max_tables,
                "max_columns_per_table": max_columns_per_table or self.max_columns_per_table,
                "max_relationship_clues": max_relationship_clues or self.max_relationship_clues,
                "max_chars": max_chars or self.max_chars,
                "max_tokens": max_tokens or self.max_tokens,
            },
        )
        limited = self.limiter.limit(
            packed_context,
            render_fn=lambda tables, clues: self._render_packed_text(
                question=question,
                db_type=db_type,
                tables=tables,
                relationship_clues=clues,
                max_chars=max(max_chars or self.max_chars, 1_000_000),
                truncate=False,
            ),
        )
        return limited

    def _rank_tables(
        self,
        question: str,
        tables: list[TableSchema],
        relationship_clues: list[dict[str, Any]],
        column_annotations: dict[str, list[dict[str, Any]]],
        document_lookup: dict[str, TableDocumentation],
    ) -> list[TableSchema]:
        query_tokens = set(tokenize_text(question))
        scored: list[tuple[float, TableSchema]] = []
        for table in tables:
            table_id = table_name(table)
            doc = document_lookup.get(table_id)
            score = 0.0
            score += self._table_structure_score(table, doc)
            score += self._table_text_score(question, table, doc)
            score += sum(
                2.5 + min(1.0, float(item.get("score") or 0.0))
                for item in column_annotations.get(table_id, [])
                if query_tokens & set(tokenize_text(str(item.get("column_name") or "")))
            )
            score += sum(
                1.2
                for clue in relationship_clues
                if clue.get("source_table") == table_id or clue.get("target_table") == table_id
            )
            scored.append((score, table))
        scored.sort(key=lambda item: (-item[0], table_name(item[1])))
        return [table for _, table in scored]

    def _select_columns(
        self,
        question: str,
        table: TableSchema,
        *,
        annotations: list[dict[str, Any]],
        document: TableDocumentation | None,
        max_columns_per_table: int,
        relationship_clues: list[dict[str, Any]],
    ) -> list[PackedColumnContext]:
        query_tokens = set(tokenize_text(question))
        ranked: list[tuple[float, PackedColumnContext]] = []
        seen: set[str] = set()
        annotation_lookup = {
            str(item.get("column_name") or "").lower(): item
            for item in annotations
            if str(item.get("column_name") or "").strip()
        }
        relation_columns = self._relation_columns_for_table(table_name(table), relationship_clues)

        for column in self._iter_columns(table, document):
            column_name = str(getattr(column, "name", "") or "")
            lower_name = column_name.lower()
            if not column_name:
                continue
            score, reason = self._score_column(question, column, annotation_lookup.get(lower_name), relation_columns)
            if query_tokens & set(tokenize_text(column_name)):
                score += 4.0
                reason = "column_name"
            if query_tokens & set(tokenize_text(column_comment(column) or "")):
                score += 2.0
                reason = "column_comment"
            if lower_name in annotation_lookup:
                score += 8.0
                reason = "column_annotation"
            if column_name in relation_columns:
                score += 6.0
                reason = "relationship_column"
            packed = PackedColumnContext(
                name=column_name,
                score=score,
                reason=reason,
                is_primary_key=self._is_primary_key(column),
                is_foreign_key=self._is_foreign_key(column),
                is_time_field=bool(getattr(column, "is_time_field", False)),
                is_metric_field=bool(getattr(column, "is_metric_field", False)),
                foreign_table=column_foreign_table(column),
                comment=column_comment(column),
            )
            if column_name not in seen:
                seen.add(column_name)
                ranked.append((score, packed))

        ranked.sort(key=lambda item: (-item[0], item[1].name))

        selected: list[PackedColumnContext] = []
        required_columns = [
            column
            for _, column in ranked
            if column.is_primary_key or column.is_foreign_key or column.name in relation_columns
        ]
        for column in required_columns:
            if column.name not in {item.name for item in selected}:
                selected.append(column)

        for _, column in ranked:
            if column.name in {item.name for item in selected}:
                continue
            if len(selected) >= max_columns_per_table:
                break
            selected.append(column)

        if not selected and ranked:
            selected = [column for _, column in ranked[:max_columns_per_table]]
        return selected[:max_columns_per_table]

    def _select_join_hints(
        self,
        table_id: str,
        relationship_clues: list[dict[str, Any]],
        table_lookup: dict[str, TableSchema],
    ) -> list[dict[str, Any]]:
        hints: list[dict[str, Any]] = []
        for clue in relationship_clues:
            if clue.get("source_table") != table_id and clue.get("target_table") != table_id:
                continue
            source_table = str(clue.get("source_table") or "")
            target_table = str(clue.get("target_table") or "")
            if source_table not in table_lookup or target_table not in table_lookup:
                continue
            hints.append(
                {
                    "source_table": source_table,
                    "target_table": target_table,
                    "source_column": str(clue.get("source_column") or ""),
                    "target_column": str(clue.get("target_column") or ""),
                    "confidence": float(clue.get("confidence") or 0.0),
                    "reason": str(clue.get("reason") or "relationship_aware"),
                }
            )
        return hints

    def _render_packed_text(
        self,
        *,
        question: str,
        db_type: str,
        tables: list[PackedTableContext],
        relationship_clues: list[dict[str, Any]],
        max_chars: int,
        truncate: bool = True,
    ) -> str:
        lines: list[str] = [
            "Packing Rules: Keep PK/FK, relationship columns, and annotated columns first. Do not invent tables or columns.",
            "Schema Context:",
        ]
        for table in tables:
            lines.extend(self._format_table_section(table))
            if len("\n".join(lines)) >= max_chars:
                break

        if relationship_clues:
            lines.append("Relationship Clues:")
            for clue in relationship_clues:
                lines.append(
                    f"- {clue.get('source_table')}.{clue.get('source_column')} -> {clue.get('target_table')}.{clue.get('target_column')}"
                    f" | confidence={float(clue.get('confidence') or 0.0):.2f}"
                    f" | reason={clue.get('reason') or 'relationship_aware'}"
                )
                if len("\n".join(lines)) >= max_chars:
                    break

        packed_text = "\n".join(lines).strip()
        if truncate and len(packed_text) > max_chars:
            packed_text = packed_text[:max_chars].rstrip()
        return packed_text

    def _format_table_section(self, table: PackedTableContext) -> list[str]:
        lines = [f"- Table: {table.table_name}"]
        if table.table_type:
            lines[-1] += f" [{table.table_type}]"
        lines.append(f"  Summary: {self._truncate(table.summary, 220)}")
        if table.primary_keys:
            lines.append("  PK: " + ", ".join(table.primary_keys[:5]))
        if table.foreign_keys:
            lines.append("  FK: " + ", ".join(table.foreign_keys[:5]))
        if table.columns:
            lines.append("  Columns: " + ", ".join(column.name for column in table.columns))
            focus_bits = []
            for column in table.columns:
                labels: list[str] = []
                if column.is_primary_key:
                    labels.append("pk")
                if column.is_foreign_key:
                    labels.append("fk")
                if column.is_time_field:
                    labels.append("time")
                if column.is_metric_field:
                    labels.append("metric")
                if column.reason in {"column_annotation", "relationship_column", "column_name", "column_comment"}:
                    labels.append(column.reason)
                if labels:
                    focus_bits.append(f"{column.name}({'/'.join(labels)})")
            if focus_bits:
                lines.append("  Focus: " + ", ".join(focus_bits[:5]))
        if table.join_hints:
            joins = ", ".join(
                f"{item['source_column']}->{item['target_table']}.{item['target_column']}"
                for item in table.join_hints[:4]
            )
            lines.append("  Joins: " + joins)
        return lines

    def _score_column(
        self,
        question: str,
        column: Any,
        annotation: dict[str, Any] | None,
        relation_columns: set[str],
    ) -> tuple[float, str]:
        score = 0.0
        reason = "semantic"
        column_name = str(getattr(column, "name", "") or "")
        query_tokens = set(tokenize_text(question))
        column_tokens = set(tokenize_text(column_name))
        comment_tokens = set(tokenize_text(column_comment(column)))
        sample_tokens = set(tokenize_text(" ".join(self._samples_from_column(column))))

        if self._is_primary_key(column):
            score += 10.0
            reason = "primary_key"
        if self._is_foreign_key(column):
            score += 8.0
            reason = "foreign_key"
        if column_name in relation_columns:
            score += 7.0
            reason = "relationship_column"
        if query_tokens & column_tokens:
            score += 5.0 * len(query_tokens & column_tokens)
            reason = "column_name"
        if query_tokens & comment_tokens:
            score += 2.0 * len(query_tokens & comment_tokens)
            reason = "column_comment"
        if annotation:
            score += 6.0 + float(annotation.get("score") or 0.0)
            reason = "column_annotation"
        if query_tokens & sample_tokens:
            score += 1.0
            reason = "sample_value"
        if getattr(column, "is_time_field", False):
            score += 1.5
        if getattr(column, "is_metric_field", False):
            score += 1.2
        if str(getattr(column, "foreign_table", "") or "").strip():
            score += 0.6
        return score, reason

    def _relation_columns_for_table(self, table_id: str, relationship_clues: list[dict[str, Any]]) -> set[str]:
        columns: set[str] = set()
        for clue in relationship_clues:
            if clue.get("source_table") == table_id:
                if clue.get("source_column"):
                    columns.add(str(clue.get("source_column")))
            if clue.get("target_table") == table_id:
                if clue.get("target_column"):
                    columns.add(str(clue.get("target_column")))
        return columns

    def _table_structure_score(self, table: TableSchema, document: TableDocumentation | None) -> float:
        score = 0.0
        columns = list(self._iter_columns(table, document))
        if any(self._is_primary_key(column) for column in columns):
            score += 2.0
        if any(self._is_foreign_key(column) for column in columns):
            score += 2.5
        if len(columns) <= 8:
            score += 1.0
        if document and document.table_category in {"fact_table", "dimension_table", "bridge_table", "event_table"}:
            score += 1.0
        return score

    def _table_text_score(self, question: str, table: TableSchema, document: TableDocumentation | None) -> float:
        query_tokens = set(tokenize_text(question))
        table_tokens = set(tokenize_text(table_name(table)))
        table_tokens.update(tokenize_text(table_comment(table)))
        table_tokens.update(tokenize_text(table_description(table)))
        if document:
            table_tokens.update(tokenize_text(document.business_summary))
            table_tokens.update(tokenize_text(" ".join(document.domain_tags)))
            for column in document.columns:
                table_tokens.update(tokenize_text(column.name))
                table_tokens.update(tokenize_text(column.comment))
                table_tokens.update(tokenize_text(column.business_meaning or ""))
        return float(len(query_tokens & table_tokens) * 2)

    @staticmethod
    def _iter_columns(table: TableSchema, document: TableDocumentation | None) -> list[Any]:
        if document and document.columns:
            # Prefer documented column objects because they carry richer metadata.
            return list(document.columns)
        return list(getattr(table, "columns", []) or [])

    @staticmethod
    def _is_primary_key(column: Any) -> bool:
        return bool(getattr(column, "is_primary_key", False))

    @staticmethod
    def _is_foreign_key(column: Any) -> bool:
        return bool(getattr(column, "is_foreign_key", False)) or bool(column_foreign_table(column))

    @staticmethod
    def _format_foreign_key(column: Any) -> str:
        target = column_foreign_table(column)
        if target:
            return f"{getattr(column, 'name', '')}->{target}"
        return str(getattr(column, "name", "") or "")

    @staticmethod
    def _samples_from_column(column: Any) -> list[str]:
        samples = getattr(column, "sample_values", None) or []
        return [str(item) for item in list(samples)[:5] if str(item).strip()]

    @staticmethod
    def _table_type(table: TableSchema, document: TableDocumentation | None) -> str | None:
        if document and document.table_category:
            return document.table_category
        columns = list(getattr(table, "columns", []) or [])
        if any(getattr(column, "is_foreign_key", False) for column in columns) and any(
            getattr(column, "is_metric_field", False) for column in columns
        ):
            return "fact_table"
        if any(getattr(column, "is_foreign_key", False) for column in columns):
            return "bridge_table"
        return "dimension_table"

    @staticmethod
    def _table_summary(table: TableSchema, document: TableDocumentation | None) -> str:
        if document and document.business_summary:
            return document.business_summary.strip()
        comment = table_comment(table) or table_description(table) or ""
        return comment.strip() or f"Table {table_name(table)}"

    @staticmethod
    def _truncate(text: str, limit: int) -> str:
        if len(text) <= limit:
            return text
        return text[: max(0, limit - 1)].rstrip() + "…"

    @staticmethod
    def _normalize_clues(relationship_clues: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for clue in relationship_clues or []:
            source_table = str(clue.get("source_table") or "").strip()
            target_table = str(clue.get("target_table") or "").strip()
            source_column = str(clue.get("source_column") or "").strip()
            target_column = str(clue.get("target_column") or "").strip()
            if not (source_table and target_table and source_column and target_column):
                continue
            signature = (source_table, source_column, target_table, target_column)
            if signature in seen:
                continue
            seen.add(signature)
            normalized.append(
                {
                    "source_table": source_table,
                    "target_table": target_table,
                    "source_column": source_column,
                    "target_column": target_column,
                    "confidence": float(clue.get("confidence") or 0.0),
                    "reason": str(clue.get("reason") or "relationship_aware"),
                }
            )
        normalized.sort(
            key=lambda item: (
                -float(item.get("confidence") or 0.0),
                str(item.get("source_table") or ""),
                str(item.get("target_table") or ""),
            )
        )
        return normalized

    @staticmethod
    def _normalize_documents(
        documents: Iterable[TableDocumentation] | dict[str, TableDocumentation] | None,
    ) -> dict[str, TableDocumentation]:
        if documents is None:
            return {}
        if isinstance(documents, dict):
            return {str(name): doc for name, doc in documents.items() if doc is not None}
        return {doc.table_name: doc for doc in documents if doc is not None}


ContextPacker = SchemaContextPacker
