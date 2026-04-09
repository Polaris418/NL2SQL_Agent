from __future__ import annotations

from hashlib import sha256
from dataclasses import dataclass, field
from typing import Any, Iterable

from app.agent.contracts import tokenize_text
from app.agent.utils import table_name
from app.rag.metadata_filter import DOMAIN_HINTS
from app.rag.schema_doc import TableDocumentation


@dataclass(slots=True)
class ShardDefinition:
    shard_id: str
    label: str
    table_names: list[str] = field(default_factory=list)
    schema_name: str | None = None
    domain_tags: list[str] = field(default_factory=list)
    table_categories: list[str] = field(default_factory=list)

    @property
    def name(self) -> str:
        return self.shard_id


class ShardPlanner:
    """Plan lightweight schema shards for large connections."""

    def __init__(self, threshold: int = 100):
        self.threshold = max(1, int(threshold))

    def build_shards(
        self,
        connection_id: str,
        tables: list[Any],
        documents: dict[str, TableDocumentation],
    ) -> list[ShardDefinition]:
        if len(tables) <= self.threshold:
            return [
                ShardDefinition(
                    shard_id=f"{connection_id}:all",
                    label="all",
                    table_names=[table_name(table) for table in tables],
                )
            ]

        buckets: dict[str, ShardDefinition] = {}
        schema_names = {
            doc.schema_name or getattr(table, "schema_name", None)
            for table in tables
            for doc in [documents.get(table_name(table))]
            if (doc and doc.schema_name) or getattr(table, "schema_name", None)
        }
        use_schema_as_primary = len(schema_names) > 1
        for table in tables:
            table_id = table_name(table)
            doc = documents.get(table_id)
            schema_name = doc.schema_name if doc else getattr(table, "schema_name", None)
            category = doc.table_category if doc and doc.table_category else "uncategorized"
            domain = (doc.domain_tags[0] if doc and doc.domain_tags else "general")
            label = schema_name if use_schema_as_primary and schema_name else f"{domain}:{category}"
            shard = buckets.setdefault(
                label,
                ShardDefinition(
                    shard_id=f"{connection_id}:{label}",
                    label=label,
                    schema_name=schema_name,
                ),
            )
            shard.table_names.append(table_id)
            if doc:
                for tag in doc.domain_tags:
                    if tag not in shard.domain_tags:
                        shard.domain_tags.append(tag)
                if doc.table_category and doc.table_category not in shard.table_categories:
                    shard.table_categories.append(doc.table_category)

        shards = list(buckets.values())
        if len(shards) == 1:
            return shards
        return sorted(shards, key=lambda item: (-len(item.table_names), item.label))

    def select_shards(self, query: str, shards: list[ShardDefinition]) -> list[ShardDefinition]:
        if len(shards) <= 1:
            return shards
        query_tokens = set(tokenize_text(query))
        query_domains = {
            domain
            for domain, hints in DOMAIN_HINTS.items()
            if any(hint in query_tokens or hint in query.lower() for hint in hints)
        }
        scored: list[tuple[int, ShardDefinition]] = []
        for shard in shards:
            shard_tokens = set(tokenize_text(shard.label))
            for table_id in shard.table_names:
                shard_tokens.update(tokenize_text(table_id))
            for tag in shard.domain_tags:
                shard_tokens.update(tokenize_text(tag))
            for category in shard.table_categories:
                shard_tokens.update(tokenize_text(category))
            score = len(query_tokens & shard_tokens)
            if query_domains and query_domains.intersection(shard.domain_tags):
                score += 3
            scored.append((score, shard))
        scored.sort(key=lambda item: (item[0], len(item[1].table_names)), reverse=True)
        selected = [shard for score, shard in scored if score > 0]
        if selected:
            return selected[: min(3, len(selected))]
        fallback = [shard for _, shard in scored[: min(2, len(scored))]]
        return fallback


class SchemaShardPlanner:
    """Compatibility wrapper exposing the production-oriented planner API."""

    def __init__(self, max_shard_size: int = 100, max_query_shards: int = 3):
        self.max_shard_size = max(1, int(max_shard_size))
        self.max_query_shards = max(1, int(max_query_shards))
        self._planner = ShardPlanner(threshold=self.max_shard_size)

    def build(
        self,
        connection_id: str,
        tables: Iterable[Any],
        documents: dict[str, TableDocumentation],
        *,
        schema_version: str | None = None,
    ) -> list[ShardDefinition]:
        return self._planner.build_shards(connection_id, list(tables), documents)

    build_shards = build

    def select(
        self,
        query: str,
        shards: list[ShardDefinition],
        *,
        metadata_filter: Any | None = None,
    ) -> list[ShardDefinition]:
        selected = self._planner.select_shards(query, shards)
        if metadata_filter is not None and getattr(metadata_filter, "table_names", None):
            table_names = set(getattr(metadata_filter, "table_names", set()) or set())
            selected = [shard for shard in selected if table_names.intersection(shard.table_names)]
        return selected[: self.max_query_shards] or shards[: self.max_query_shards]

    select_shards = select

    def signature(self, shards: Iterable[ShardDefinition]) -> str:
        payload = "|".join(sorted(shard.shard_id for shard in shards))
        return sha256(payload.encode("utf-8")).hexdigest()[:16]


ShardBucket = ShardDefinition
