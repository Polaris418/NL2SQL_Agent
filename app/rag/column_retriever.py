from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.agent.contracts import tokenize_text
from app.agent.utils import table_name
from app.rag.schema_doc import FieldDocumentation, TableDocumentation

SEMANTIC_COLUMN_ALIASES = {
    "email": {"邮箱", "邮件", "email"},
    "nickname": {"昵称", "nickname", "nick"},
    "amount": {"金额", "amount", "price", "total", "总额"},
    "user": {"用户", "会员", "customer", "user"},
    "department": {"部门", "department", "dept", "组织"},
    "employee": {"员工", "employee", "staff"},
    "created_at": {"创建时间", "创建日期", "时间", "date", "time"},
}


@dataclass(slots=True)
class ColumnMatch:
    table_name: str
    column_name: str
    score: float
    reason: str


class ColumnLevelRetriever:
    """Annotate retrieved tables with the most relevant columns for a query."""

    def __init__(self, top_k: int = 5):
        self.top_k = max(1, int(top_k))

    def rank_columns(
        self,
        query: str,
        documentation: TableDocumentation | None,
        *,
        top_k: int | None = None,
    ) -> list[ColumnMatch]:
        if documentation is None:
            return []
        query_tokens = set(tokenize_text(query))
        query_text = (query or "").lower()
        matches: list[ColumnMatch] = []
        for column in documentation.columns:
            score, reason = self._score_column(query_tokens, query_text, column)
            if score <= 0:
                continue
            matches.append(
                ColumnMatch(
                    table_name=table_name(documentation),
                    column_name=column.name,
                    score=score,
                    reason=reason,
                )
            )
        matches.sort(key=lambda item: (-item.score, item.column_name))
        return matches[: (top_k or self.top_k)]

    def annotate_documents(
        self,
        query: str,
        documents: list[TableDocumentation],
        *,
        top_k: int | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        annotated: dict[str, list[dict[str, Any]]] = {}
        for document in documents:
            ranked = self.rank_columns(query, document, top_k=top_k)
            annotated[table_name(document)] = [
                {
                    "column_name": match.column_name,
                    "score": round(match.score, 4),
                    "reason": match.reason,
                }
                for match in ranked
            ]
        return annotated

    @staticmethod
    def _score_column(query_tokens: set[str], query_text: str, column: FieldDocumentation) -> tuple[float, str]:
        column_tokens = set(tokenize_text(column.name))
        comment_tokens = set(tokenize_text((getattr(column, "comment", None) or "")))
        meaning_tokens = set(tokenize_text((getattr(column, "business_meaning", None) or "")))
        alias_tokens = set()
        lowered_name = column.name.lower()
        for key, aliases in SEMANTIC_COLUMN_ALIASES.items():
            if key in lowered_name or lowered_name in key:
                alias_tokens.update(tokenize_text(" ".join(aliases)))
        sample_tokens = set()
        for value in getattr(column, "sample_values", [])[:5]:
            sample_tokens.update(tokenize_text(value))

        score = 0.0
        reason = "semantic"
        if query_tokens & column_tokens:
            score += 4.0 * len(query_tokens & column_tokens)
            reason = "column_name"
        if query_tokens & comment_tokens:
            score += 2.5 * len(query_tokens & comment_tokens)
            reason = "column_comment"
        if query_tokens & meaning_tokens:
            score += 2.0 * len(query_tokens & meaning_tokens)
            reason = "column_meaning"
        if query_tokens & alias_tokens:
            score += 2.2 * len(query_tokens & alias_tokens)
            reason = "column_alias"
        else:
            alias_terms = SEMANTIC_COLUMN_ALIASES.get(lowered_name, set())
            if any(term.lower() in query_text for term in alias_terms):
                score += 2.2
                reason = "column_alias"
            elif lowered_name in query_text:
                score += 2.0
                reason = "column_name"
        if query_tokens & sample_tokens:
            score += 1.0 * len(query_tokens & sample_tokens)
            reason = "sample_value"
        if lowered_name.endswith("_count") and any(term in query_text for term in {"count", "数量", "总数", "多少", "统计"}):
            score += 3.5
            reason = "metric_count"

        if column.is_primary_key:
            score += 0.3
        if column.is_foreign_key:
            score += 0.5
        relation_terms = {"user", "order", "dept", "department", "employee", "用户", "订单", "部门", "员工", "组织"}
        if column.name.lower().endswith("_id") and any(
            token in query_tokens or token in query_text for token in relation_terms
        ):
            score += 3.0
            reason = "relation_field"
        if getattr(column, "is_metric_field", False):
            score += 0.8
        if getattr(column, "is_time_field", False):
            score += 0.4
        return score, reason
