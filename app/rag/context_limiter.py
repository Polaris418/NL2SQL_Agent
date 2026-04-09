from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, TYPE_CHECKING

from app.agent.contracts import tokenize_text

if TYPE_CHECKING:  # pragma: no cover
    from app.rag.context_packer import PackedColumnContext, PackedSchemaContext, PackedTableContext


@dataclass(slots=True)
class ContextLimitReport:
    limit_reason: str | None = None
    dropped_tables: list[str] = field(default_factory=list)
    dropped_columns: dict[str, list[str]] = field(default_factory=dict)
    dropped_relationship_clues: list[dict[str, Any]] = field(default_factory=list)


class SchemaContextLimiter:
    def __init__(
        self,
        *,
        max_tables: int = 8,
        max_columns_per_table: int = 6,
        max_relationship_clues: int = 6,
        max_chars: int = 7000,
        max_tokens: int = 1800,
    ):
        self.max_tables = max(1, int(max_tables))
        self.max_columns_per_table = max(1, int(max_columns_per_table))
        self.max_relationship_clues = max(1, int(max_relationship_clues))
        self.max_chars = max(1, int(max_chars))
        self.max_tokens = max(1, int(max_tokens))

    def limit(
        self,
        context: Any,
        *,
        render_fn: Callable[[list[Any], list[dict[str, Any]]], str],
        max_chars: int | None = None,
        max_tokens: int | None = None,
    ) -> Any:
        budget_tables = max(1, int(context.budget.get("max_tables") or self.max_tables))
        budget_columns = max(1, int(context.budget.get("max_columns_per_table") or self.max_columns_per_table))
        budget_relationships = max(1, int(context.budget.get("max_relationship_clues") or self.max_relationship_clues))
        budget_chars = max(1, int(max_chars or context.budget.get("max_chars") or self.max_chars))
        budget_tokens = max(1, int(max_tokens or context.budget.get("max_tokens") or self.max_tokens))
        report = ContextLimitReport()

        tables = deepcopy(context.tables)
        relationship_clues = self._filter_relationship_clues(deepcopy(context.relationship_clues), {table.table_name for table in tables}, report)

        def render() -> tuple[str, int, int]:
            text = render_fn(tables, relationship_clues)
            return text, len(text), len(tokenize_text(text))

        text, char_count, token_count = render()
        while (
            len(tables) > budget_tables
            or any(len(table.columns) > budget_columns for table in tables)
            or char_count > budget_chars
            or token_count > budget_tokens
        ) and tables:
            removed = False
            if len(tables) > budget_tables:
                dropped_table = tables.pop()
                report.dropped_tables.append(dropped_table.table_name)
                report.limit_reason = report.limit_reason or "table_budget"
                relationship_clues = self._filter_relationship_clues(relationship_clues, {table.table_name for table in tables}, report)
                removed = True
            elif any(len(table.columns) > budget_columns for table in tables):
                removed = self._drop_low_priority_column(tables, report)
                if removed:
                    report.limit_reason = report.limit_reason or "column_budget"
            else:
                removed = self._drop_low_priority_column(tables, report)
                if removed:
                    report.limit_reason = report.limit_reason or "column_budget"
                elif len(relationship_clues) > budget_relationships:
                    dropped_clue = relationship_clues.pop()
                    report.dropped_relationship_clues.append(dropped_clue)
                    report.limit_reason = report.limit_reason or "relationship_budget"
                    removed = True
            if not removed:
                break
            text, char_count, token_count = render()

        truncated = False
        if char_count > budget_chars or token_count > budget_tokens:
            truncated = True
            report.limit_reason = report.limit_reason or ("char_budget" if char_count > budget_chars else "token_budget")
            text = text[:budget_chars].rstrip()
            text = self._trim_to_token_budget(text, budget_tokens)
            char_count = len(text)
            token_count = len(tokenize_text(text))

        limited = deepcopy(context)
        limited.tables = tables
        limited.relationship_clues = relationship_clues
        limited.packed_text = text
        limited.truncated = context.truncated or truncated or report.limit_reason is not None
        limited.char_count = char_count
        limited.token_count = token_count
        limited.limit_reason = report.limit_reason
        limited.dropped_tables = report.dropped_tables
        limited.dropped_columns = report.dropped_columns
        limited.dropped_relationship_clues = report.dropped_relationship_clues
        limited.budget = {
            "max_tables": budget_tables,
            "max_columns_per_table": budget_columns,
            "max_relationship_clues": budget_relationships,
            "max_chars": budget_chars,
            "max_tokens": budget_tokens,
        }
        limited.limiter_metadata = {
            "limit_reason": report.limit_reason,
            "dropped_tables": list(report.dropped_tables),
            "dropped_columns": {key: list(value) for key, value in report.dropped_columns.items()},
            "dropped_relationship_clues": [dict(item) for item in report.dropped_relationship_clues],
            "budget": dict(limited.budget),
            "original_char_count": getattr(context, "char_count", 0),
            "original_token_count": getattr(context, "token_count", 0),
            "final_char_count": char_count,
            "final_token_count": token_count,
            "truncated": limited.truncated,
        }
        return limited

    @staticmethod
    def _trim_to_token_budget(text: str, token_budget: int) -> str:
        clipped = text.strip()
        while clipped and len(tokenize_text(clipped)) > token_budget:
            tokens = tokenize_text(clipped)
            if len(tokens) <= token_budget:
                break
            clipped = " ".join(tokens[:token_budget]).strip()
            if len(tokenize_text(clipped)) <= token_budget:
                break
            token_budget = max(1, token_budget - 1)
        return clipped

    def _filter_relationship_clues(
        self,
        clues: list[dict[str, Any]],
        table_names: set[str],
        report: ContextLimitReport,
    ) -> list[dict[str, Any]]:
        remaining: list[dict[str, Any]] = []
        for clue in clues:
            source_table = str(clue.get("source_table") or "")
            target_table = str(clue.get("target_table") or "")
            if source_table in table_names and target_table in table_names:
                remaining.append(clue)
            else:
                report.dropped_relationship_clues.append(clue)
        if len(remaining) != len(clues) and report.limit_reason is None:
            report.limit_reason = "relationship_budget"
        return remaining

    def _drop_low_priority_column(self, tables: list[Any], report: ContextLimitReport) -> bool:
        candidate: tuple[int, int, Any, Any] | None = None
        for table_index, table in enumerate(tables):
            if len(table.columns) <= 1:
                continue
            for column in table.columns:
                if self._is_protected(column):
                    continue
                score = self._column_priority(column)
                if candidate is None or score < candidate[0]:
                    candidate = (score, table_index, table, column)
        if candidate is None:
            return False
        _, _, table, column = candidate
        table.columns = [item for item in table.columns if item.name != column.name]
        report.dropped_columns.setdefault(table.table_name, []).append(column.name)
        return True

    @staticmethod
    def _column_priority(column: Any) -> int:
        score = 0
        if column.is_primary_key:
            score += 100
        if column.is_foreign_key:
            score += 80
        if column.reason == "relationship_column":
            score += 60
        if column.reason == "column_annotation":
            score += 50
        if column.is_time_field:
            score += 20
        if column.is_metric_field:
            score += 20
        score += int(max(0.0, column.score) * 10)
        return score

    @staticmethod
    def _is_protected(column: Any) -> bool:
        return column.is_primary_key or column.is_foreign_key or column.reason in {"relationship_column", "column_annotation"}
