from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.agent.utils import table_name
from app.rag.column_retriever import ColumnLevelRetriever as BaseColumnLevelRetriever
from app.rag.schema_doc import JoinPath, TableDocumentation


@dataclass(slots=True)
class RelationshipEdge:
    source_table: str
    target_table: str
    source_column: str
    target_column: str
    confidence: float
    reason: str


@dataclass(slots=True)
class RelationshipExpansionResult:
    related_tables: list[Any] = field(default_factory=list)
    edges: list[RelationshipEdge] = field(default_factory=list)


class RelationshipAwareRetriever:
    """Expand retrieved tables with connected related tables using join paths."""

    def __init__(self, expansion_boost: float = 0.18):
        self.expansion_boost = float(expansion_boost)

    def expand(
        self,
        query: str,
        selected_tables: list[Any],
        documents: dict[str, TableDocumentation],
        *,
        all_tables: dict[str, Any] | None = None,
        allowed_table_names: list[str] | set[str] | None = None,
    ) -> RelationshipExpansionResult:
        if not selected_tables:
            return RelationshipExpansionResult()

        allowed = set(allowed_table_names or [])
        known = {table_name(table) for table in selected_tables}
        related_tables: list[Any] = []
        edges: list[RelationshipEdge] = []
        seen_edges: set[tuple[str, str, str, str]] = set()
        table_lookup = dict(all_tables or {})

        def maybe_add_edge(join_path: JoinPath) -> None:
            edge_key = (
                join_path.source_table,
                join_path.source_column,
                join_path.target_table,
                join_path.target_column,
            )
            if edge_key in seen_edges:
                return
            seen_edges.add(edge_key)
            edges.append(self._to_edge(join_path))

        for table in selected_tables:
            table_id = table_name(table)
            document = documents.get(table_id)
            if document is None:
                continue
            if document.table_category not in {'fact_table', 'bridge_table', 'event_table', 'dimension_table'} and not document.join_paths:
                continue
            for join_path in document.join_paths:
                target = join_path.target_table
                if allowed and target not in allowed:
                    continue
                if target in known:
                    maybe_add_edge(join_path)
                    continue
                target_table_obj = table_lookup.get(target)
                if target_table_obj is None:
                    continue
                related_tables.append(target_table_obj)
                known.add(target)
                maybe_add_edge(join_path)

        for source_name, document in documents.items():
            if allowed and source_name not in allowed:
                continue
            for join_path in document.join_paths:
                if join_path.target_table not in known:
                    continue
                maybe_add_edge(join_path)
                if source_name in known:
                    continue
                source_table_obj = table_lookup.get(source_name)
                if source_table_obj is None:
                    continue
                related_tables.append(source_table_obj)
                known.add(source_name)
                break

        return RelationshipExpansionResult(related_tables=related_tables, edges=edges)

    @staticmethod
    def _to_edge(join_path: JoinPath) -> RelationshipEdge:
        return RelationshipEdge(
            source_table=join_path.source_table,
            target_table=join_path.target_table,
            source_column=join_path.source_column,
            target_column=join_path.target_column,
            confidence=join_path.confidence,
            reason=join_path.reason or 'relationship_aware',
        )


class ColumnLevelRetriever(BaseColumnLevelRetriever):
    """Compatibility wrapper used by the orchestrator."""

    def rank(
        self,
        query: str,
        documents: dict[str, TableDocumentation],
        *,
        table_names: list[str] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        selected_name_set = set(table_names or [])
        selected = [
            document
            for name, document in documents.items()
            if table_names is None or name in selected_name_set
        ]
        return self.annotate_documents(query, selected, top_k=self.top_k)
