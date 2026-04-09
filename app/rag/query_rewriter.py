from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from app.rag.synonym_dict import SynonymDictionary


@dataclass(slots=True)
class RewriteResult:
    original_query: str
    rewritten_query: str
    expanded_query: str
    applied_synonyms: list[tuple[str, str]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class QueryRewriter:
    """Lightweight runtime query rewriter for retrieval."""

    def __init__(self, synonym_dict: SynonymDictionary | None = None):
        self.synonym_dict = synonym_dict or SynonymDictionary()
        self._stats = {
            "total_queries": 0,
            "rewritten_queries": 0,
            "reloads": 0,
        }

    @property
    def stats(self) -> dict[str, Any]:
        return dict(self._stats)

    async def rewrite(
        self,
        query: str,
        connection_id: str,
        *,
        domain: str | None = None,
    ) -> RewriteResult:
        self._stats["total_queries"] += 1
        if self.synonym_dict.reload_if_needed():
            self._stats["reloads"] += 1
        rewritten, applied = self.synonym_dict.rewrite_with_trace(
            query,
            connection_id=connection_id,
            domain=domain,
        )
        rewritten = self._normalize_whitespace(rewritten)
        if rewritten != query or applied:
            self._stats["rewritten_queries"] += 1
        expanded_terms = self._expanded_terms(rewritten, connection_id=connection_id, domain=domain)
        expanded_query = self._normalize_whitespace(" ".join([rewritten, *expanded_terms]).strip())
        return RewriteResult(
            original_query=query,
            rewritten_query=rewritten,
            expanded_query=expanded_query or rewritten,
            applied_synonyms=applied,
            metadata={
                "domain": domain,
                "expanded_terms": expanded_terms,
            },
        )

    @staticmethod
    def _normalize_whitespace(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _expanded_terms(
        self,
        query: str,
        *,
        connection_id: str,
        domain: str | None = None,
    ) -> list[str]:
        expanded: list[str] = []
        seen: set[str] = set()
        for token in re.findall(r"[\w\u4e00-\u9fff]+", query):
            canonical = self.synonym_dict.get_canonical(token, connection_id=connection_id, domain=domain)
            if canonical != token and canonical not in seen:
                expanded.append(canonical)
                seen.add(canonical)
            for synonym in self.synonym_dict.get_synonyms(canonical, connection_id=connection_id, domain=domain)[:2]:
                if synonym not in seen and synonym.lower() not in query.lower():
                    expanded.append(synonym)
                    seen.add(synonym)
        return expanded[:6]
