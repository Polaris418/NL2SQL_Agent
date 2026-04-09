from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable


def _normalize_text(value: Any) -> str:
    return str(value).strip().lower()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class BusinessKnowledgeItem:
    knowledge_id: str
    content: str
    connection_id: str | None = None
    domain: str | None = None
    table_name: str | None = None
    keywords: list[str] = field(default_factory=list)
    source: str | None = None
    priority: int = 0
    created_at: str = field(default_factory=_utcnow)
    updated_at: str = field(default_factory=_utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class BusinessKnowledgeRepository:
    """Simple in-memory business knowledge repository.

    The repository supports scoping by connection, domain, and table name so
    it can be queried directly by the retrieval/orchestration layer without any
    extra dependencies or persistence requirements.
    """

    def __init__(self) -> None:
        self._items: dict[str, BusinessKnowledgeItem] = {}

    def upsert(self, item: BusinessKnowledgeItem) -> BusinessKnowledgeItem:
        existing = self._items.get(item.knowledge_id)
        if existing is not None:
            item.created_at = existing.created_at
        item.updated_at = _utcnow()
        self._items[item.knowledge_id] = item
        return item

    def add(
        self,
        knowledge_id: str,
        content: str,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
        table_name: str | None = None,
        keywords: Iterable[str] | None = None,
        source: str | None = None,
        priority: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> BusinessKnowledgeItem:
        item = BusinessKnowledgeItem(
            knowledge_id=knowledge_id,
            content=content,
            connection_id=connection_id,
            domain=domain,
            table_name=table_name,
            keywords=[_normalize_text(keyword) for keyword in (keywords or []) if _normalize_text(keyword)],
            source=source,
            priority=int(priority),
            metadata=dict(metadata or {}),
        )
        return self.upsert(item)

    def extend(self, items: Iterable[BusinessKnowledgeItem]) -> list[BusinessKnowledgeItem]:
        return [self.upsert(item) for item in items]

    def get(self, knowledge_id: str) -> BusinessKnowledgeItem | None:
        return self._items.get(knowledge_id)

    def delete(self, knowledge_id: str) -> bool:
        return self._items.pop(knowledge_id, None) is not None

    def list(self) -> list[BusinessKnowledgeItem]:
        return sorted(self._items.values(), key=lambda item: (-item.priority, item.knowledge_id))

    def query(
        self,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
        table_name: str | None = None,
        keyword: str | None = None,
        limit: int | None = None,
    ) -> list[BusinessKnowledgeItem]:
        normalized_domain = _normalize_text(domain) if domain else None
        normalized_table = _normalize_text(table_name) if table_name else None
        normalized_keyword = _normalize_text(keyword) if keyword else None
        results: list[BusinessKnowledgeItem] = []
        for item in self.list():
            if connection_id and item.connection_id and item.connection_id != connection_id:
                continue
            if connection_id and item.connection_id is None:
                continue
            if normalized_domain and _normalize_text(item.domain or "") != normalized_domain:
                continue
            if normalized_table and _normalize_text(item.table_name or "") != normalized_table:
                continue
            if normalized_keyword:
                haystack = " ".join(
                    filter(
                        None,
                        [
                            item.content,
                            item.domain or "",
                            item.table_name or "",
                            " ".join(item.keywords),
                        ],
                    )
                ).lower()
                if normalized_keyword not in haystack:
                    continue
            results.append(item)
            if limit is not None and len(results) >= max(1, int(limit)):
                break
        return results

    def query_text(
        self,
        text: str,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
        table_name: str | None = None,
        limit: int | None = None,
    ) -> list[BusinessKnowledgeItem]:
        normalized_text = _normalize_text(text)
        if not normalized_text:
            return self.query(
                connection_id=connection_id,
                domain=domain,
                table_name=table_name,
                limit=limit,
            )
        tokens = {token for token in normalized_text.split() if token}
        results: list[BusinessKnowledgeItem] = []
        for item in self.query(connection_id=connection_id, domain=domain, table_name=table_name):
            content_tokens = set(_normalize_text(item.content).split())
            keyword_tokens = {token for keyword in item.keywords for token in _normalize_text(keyword).split()}
            if tokens & (content_tokens | keyword_tokens):
                results.append(item)
            if limit is not None and len(results) >= max(1, int(limit)):
                break
        return results

    def snapshot(self) -> dict[str, Any]:
        return {
            "count": len(self._items),
            "domains": sorted({item.domain for item in self._items.values() if item.domain}),
            "tables": sorted({item.table_name for item in self._items.values() if item.table_name}),
        }


__all__ = ["BusinessKnowledgeItem", "BusinessKnowledgeRepository"]
