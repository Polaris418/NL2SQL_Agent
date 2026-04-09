from __future__ import annotations

import re
from typing import Any

from app.llm.client import LLMClient
from app.prompts.few_shot_examples import EXAMPLES
from app.prompts.sql_generation import SYSTEM_PROMPT
from app.rag.context_packer import ContextPacker, PackedSchemaContext
from app.rag.fewshot_integration import FewShotIntegration
from app.rag.multi_tenant import TenantScope
from app.rag.telemetry import ContextLimitTelemetryEvent, TelemetrySystem
from app.schemas.connection import TableSchema
from app.agent.utils import (
    best_table_match,
    detect_aggregate,
    detect_limit,
    detect_time_filter,
    guess_datetime_columns,
    guess_dimension_columns,
    guess_numeric_columns,
    table_name,
)


class SQLGenerator:
    def __init__(
        self,
        llm_client: LLMClient,
        default_limit: int = 1000,
        few_shot_integration: FewShotIntegration | None = None,
        context_max_tables: int = 8,
        context_max_columns_per_table: int = 6,
        context_max_relationship_clues: int = 6,
        context_max_chars: int = 7000,
        context_max_tokens: int = 1800,
        telemetry_system: TelemetrySystem | None = None,
    ):
        self.llm_client = llm_client
        self.default_limit = default_limit
        self.last_cache_hit = False
        self.telemetry_system = telemetry_system
        self.context_packer = ContextPacker(
            max_tables=context_max_tables,
            max_columns_per_table=context_max_columns_per_table,
            max_relationship_clues=context_max_relationship_clues,
            max_chars=context_max_chars,
            max_tokens=context_max_tokens,
        )
        self.few_shot_integration = few_shot_integration or FewShotIntegration()
        self.last_prompt: str | None = None
        self.last_schema_context_text: str | None = None
        self.last_schema_context_metadata: dict[str, Any] = {}
        self.last_few_shot_examples: list[dict[str, Any]] = []

    async def generate(
        self,
        question: str,
        rewritten_query: str,
        schema_context: list[TableSchema],
        db_type: str,
        *,
        packed_schema_context: str | None = None,
        retrieval_metadata: dict[str, Any] | None = None,
        schema_context_details: dict[str, Any] | None = None,
        few_shot_scope: TenantScope | dict[str, Any] | None = None,
        few_shot_domain: str | None = None,
    ) -> tuple[str, float]:
        prompt = self._build_prompt(
            question,
            rewritten_query,
            schema_context,
            db_type,
            packed_schema_context=packed_schema_context,
            retrieval_metadata=retrieval_metadata,
            schema_context_details=schema_context_details,
            few_shot_scope=few_shot_scope,
            few_shot_domain=few_shot_domain,
        )
        self.last_prompt = prompt
        sql, latency = await self.llm_client.chat(SYSTEM_PROMPT, prompt)
        self.last_cache_hit = getattr(self.llm_client, "last_cache_hit", False)
        sql = self._clean_sql(sql) if sql else self._fallback(question, rewritten_query, schema_context, db_type)
        if "limit" not in sql.lower():
            sql = sql.rstrip(";") + f" LIMIT {self.default_limit};"
        return sql, latency

    def _build_prompt(
        self,
        question: str,
        rewritten_query: str,
        schema_context: list[TableSchema],
        db_type: str,
        *,
        packed_schema_context: str | None = None,
        retrieval_metadata: dict[str, Any] | None = None,
        schema_context_details: dict[str, Any] | None = None,
        few_shot_scope: TenantScope | dict[str, Any] | None = None,
        few_shot_domain: str | None = None,
    ) -> str:
        packed_context = self._pack_schema_context(
            question,
            schema_context,
            db_type=db_type,
            schema_context_details=schema_context_details,
        )
        if packed_context is not None:
            schema_text = packed_context.packed_text
            self.last_schema_context_metadata = dict(getattr(packed_context, "limiter_metadata", {}) or {})
            retrieval_metadata = None
        else:
            schema_text = packed_schema_context or self._render_fallback_schema(schema_context)
            self.last_schema_context_metadata = {}
        self.last_schema_context_text = schema_text
        available_tables = ", ".join(table_name(table) for table in schema_context) or "none"
        retrieval_sections: list[str] = []
        metadata = retrieval_metadata or {}
        relationship_clues = list(metadata.get("relationship_clues") or [])
        if relationship_clues:
            relationship_lines = [
                f"- {clue.get('source_table')}.{clue.get('source_column')} -> {clue.get('target_table')}.{clue.get('target_column')}"
                for clue in relationship_clues[:8]
            ]
            retrieval_sections.append("Join Relationships:\n" + "\n".join(relationship_lines))
        column_annotations = metadata.get("column_annotations") or {}
        if isinstance(column_annotations, dict) and column_annotations:
            annotation_lines: list[str] = []
            for table_label, items in list(column_annotations.items())[:8]:
                columns = ", ".join(
                    str(item.get("column_name"))
                    for item in items[:5]
                    if item.get("column_name")
                )
                if columns:
                    annotation_lines.append(f"- {table_label}: {columns}")
            if annotation_lines:
                retrieval_sections.append("Relevant Columns:\n" + "\n".join(annotation_lines))
        retrieval_text = ("\n" + "\n\n".join(retrieval_sections)) if retrieval_sections else ""
        few_shot_payload = self.few_shot_integration.registry.select_payload(
            scope=few_shot_scope,
            query=question,
            limit=4,
            db_type=db_type,
            business_domain=few_shot_domain,
        )
        self.last_few_shot_examples = list(few_shot_payload.get("examples") or [])
        dynamic_examples = str(few_shot_payload.get("prompt_block") or "").strip()
        example_text = dynamic_examples or "\n".join(EXAMPLES.get(db_type, []))
        prompt_parts = [
            f"DB Type: {db_type}",
            f"Dialect Rule: Use only {db_type} syntax and functions. Do not use tables that are not listed in the schema.",
            "Generation Rule: Prefer PK/FK columns, relationship hints, and annotated columns when joins or filters are needed.",
        ]
        if packed_context is not None:
            prompt_parts.append(
                "Packing: optimized schema context with PK/FK and annotated columns; "
                f"tables={len(packed_context.tables)}, chars={packed_context.char_count}, truncated={packed_context.truncated}."
            )
            if packed_context.limit_reason:
                dropped_tables = ",".join(packed_context.dropped_tables[:5]) or "-"
                dropped_columns = ", ".join(
                    f"{table}:{'|'.join(columns[:5])}"
                    for table, columns in list(packed_context.dropped_columns.items())[:3]
                ) or "-"
                prompt_parts.append(
                    f"Packing Limit: {packed_context.limit_reason}; "
                    f"dropped_tables={dropped_tables}; "
                    f"dropped_columns={dropped_columns}."
                )
        prompt_parts.extend(
            [
                f"Available Tables: {available_tables}",
                f"Question: {question}",
                f"Keywords: {rewritten_query}",
                f"Schema:\n{schema_text}{retrieval_text}",
                "Examples:\n" + example_text,
            ]
        )
        return "\n".join(prompt_parts)

    def _pack_schema_context(
        self,
        question: str,
        schema_context: list[TableSchema],
        *,
        db_type: str,
        schema_context_details: dict[str, Any] | None,
    ) -> PackedSchemaContext | None:
        if not schema_context_details:
            return None
        details = schema_context_details or {}
        try:
            packed = self.context_packer.pack(
                question,
                schema_context,
                db_type=db_type,
                relationship_clues=list(details.get("relationship_clues") or []),
                column_annotations=dict(details.get("column_annotations") or {}),
                documents=details.get("documents"),
                max_tables=int(details.get("max_tables") or getattr(self.context_packer, "max_tables", 8)),
                max_columns_per_table=int(
                    details.get("max_columns_per_table") or getattr(self.context_packer, "max_columns_per_table", 6)
                ),
                max_relationship_clues=int(
                    details.get("max_relationship_clues") or getattr(self.context_packer, "max_relationship_clues", 6)
                ),
                max_chars=int(details.get("max_chars") or getattr(self.context_packer, "max_chars", 7000)),
                max_tokens=int(details.get("max_tokens") or getattr(self.context_packer, "max_tokens", 1800)),
            )
            self.last_schema_context_metadata = dict(getattr(packed, "limiter_metadata", {}) or {})
            self._record_context_limit_telemetry(packed, details)
            return packed
        except Exception:
            self.last_schema_context_metadata = {}
            return None

    def _record_context_limit_telemetry(
        self,
        packed_context: PackedSchemaContext | None,
        schema_context_details: dict[str, Any] | None,
    ) -> None:
        if self.telemetry_system is None or packed_context is None:
            return
        metadata = dict(getattr(packed_context, "limiter_metadata", {}) or {})
        if not metadata and not packed_context.truncated:
            return
        self.telemetry_system.record_context_limit(
            ContextLimitTelemetryEvent(
                query_id=str((schema_context_details or {}).get("query_id") or "") or None,
                connection_id=str((schema_context_details or {}).get("connection_id") or "") or None,
                limit_reason=str(metadata.get("limit_reason") or packed_context.limit_reason or "") or None,
                truncated=bool(metadata.get("truncated") or packed_context.truncated),
                budget=dict(metadata.get("budget") or packed_context.budget or {}),
                original_char_count=int(metadata.get("original_char_count") or 0),
                original_token_count=int(metadata.get("original_token_count") or 0),
                final_char_count=int(metadata.get("final_char_count") or packed_context.char_count or 0),
                final_token_count=int(metadata.get("final_token_count") or packed_context.token_count or 0),
                dropped_tables=list(metadata.get("dropped_tables") or packed_context.dropped_tables or []),
                dropped_columns={
                    str(key): list(value or [])
                    for key, value in dict(metadata.get("dropped_columns") or packed_context.dropped_columns or {}).items()
                },
                dropped_relationship_clues=[
                    dict(item) for item in list(metadata.get("dropped_relationship_clues") or packed_context.dropped_relationship_clues or [])
                ],
            )
        )

    @staticmethod
    def _render_fallback_schema(schema_context: list[TableSchema]) -> str:
        return "\n".join(
            f"Table: {table_name(table)}\nColumns: " + ", ".join(f"{column.name} {column.type}" for column in table.columns)
            for table in schema_context
        )

    def _fallback(self, question: str, rewritten_query: str, schema_context: list[TableSchema], db_type: str) -> str:
        if not schema_context:
            return f"SELECT 1 LIMIT {self.default_limit};"
        table = best_table_match(f"{question} {rewritten_query}", schema_context) or schema_context[0]
        table_id = table_name(table)
        limit = max(1, min(self.default_limit, detect_limit(question, self.default_limit)))
        aggregate = detect_aggregate(question) or detect_aggregate(rewritten_query)
        numeric_columns = guess_numeric_columns(table)
        datetime_columns = guess_datetime_columns(table)
        dimension_columns = guess_dimension_columns(table)
        metric_column = next(
            (
                column
                for column in numeric_columns
                if column.name.lower() not in {"id"} and not column.name.lower().endswith("_id")
            ),
            numeric_columns[0] if numeric_columns else None,
        )

        if aggregate == "count":
            if datetime_columns and detect_time_filter(question):
                group_column = datetime_columns[0].name
                return (
                    f"SELECT {self._date_expression(group_column, db_type)} AS day, COUNT(*) AS row_count "
                    f"FROM {table_id} GROUP BY 1 ORDER BY 1 LIMIT {limit};"
                )
            return f"SELECT COUNT(*) AS row_count FROM {table_id} LIMIT {limit};"

        if aggregate in {"sum", "avg", "max", "min"} and metric_column is not None:
            metric = metric_column.name
            func = "AVG" if aggregate == "avg" else aggregate.upper()
            if datetime_columns:
                group_column = datetime_columns[0].name
                return (
                    f"SELECT {self._date_expression(group_column, db_type)} AS day, {func}({metric}) AS value "
                    f"FROM {table_id} GROUP BY 1 ORDER BY 1 LIMIT {limit};"
                )
            dimension = dimension_columns[0].name if dimension_columns else "*"
            group_clause = f"GROUP BY {dimension}" if dimension != "*" else ""
            order_clause = "ORDER BY value DESC" if dimension != "*" else ""
            return (
                f"SELECT {dimension}, {func}({metric}) AS value FROM {table_id} "
                f"{group_clause} {order_clause} LIMIT {limit};"
            ).replace("  ", " ").strip()

        columns = ", ".join(column.name for column in (dimension_columns[:3] or table.columns[:8])) or "*"
        return f"SELECT {columns} FROM {table_id} LIMIT {limit};"

    @staticmethod
    def _clean_sql(text: str) -> str:
        cleaned = text.strip().strip("`")
        return re.sub(r"^sql\s*", "", cleaned, flags=re.IGNORECASE)

    def _date_expression(self, column_name: str, db_type: str) -> str:
        normalized = db_type.lower()
        if normalized == "postgresql":
            return f"DATE_TRUNC('day', {column_name})"
        if normalized == "mysql":
            return f"DATE({column_name})"
        return f"DATE({column_name})"
