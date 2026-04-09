from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable
from uuid import uuid4

try:
    from rank_bm25 import BM25Okapi
except ImportError:  # pragma: no cover
    BM25Okapi = None

from app.agent.contracts import tokenize_text
from app.agent.utils import schema_fingerprint, table_name
from app.rag.cache import RetrievalCache
from app.rag.access_control import AccessControlPolicy, AccessEffect, AccessScope
from app.rag.concurrency import (
    RetrievalConcurrencyController,
    RetrievalConcurrencyError,
    RetrievalTimeoutError,
)
from app.rag.degradation import DegradationManager
from app.rag.embedding import EmbeddingConfig, EmbeddingModel, create_embedding_model
from app.rag.failure_classifier import FailureClassifier
from app.rag.fusion import FusedCandidate, RetrievalCandidate, WeightedFusion
from app.rag.metadata_filter import MetadataFilter
from app.rag.input_validation import InputValidationError, InputValidator
from app.rag.query_rewriter import QueryRewriter, RewriteResult
from app.rag.reranker import CrossEncoderReranker, RerankCandidate, RerankerConfig
from app.rag.schema_doc import SchemaDocumentationManager, TableDocumentation
from app.rag.logging import RetrievalLogEntry, RetrievalLogger
from app.rag.multi_tenant import MultiTenantIsolationManager, TenantScope
from app.rag.relationship_retriever import ColumnLevelRetriever, RelationshipAwareRetriever
from app.rag.sharding import SchemaShardPlanner, ShardBucket
from app.rag.synonym_dict import SynonymDictionary
from app.rag.sensitive_fields import SensitiveFieldPolicy
from app.rag.telemetry import RetrievalTelemetryEvent, TelemetrySystem
from app.db.repositories.rag_telemetry_repo import RAGTelemetryRepository
from app.rag.vector_store import (
    VectorStore,
    VectorStoreConfig,
    VectorStoreTimeoutError,
    VectorStoreUnavailableError,
    create_vector_store,
)


@dataclass(slots=True)
class RetrievalTelemetry:
    connection_id: str
    query_id: str | None = None
    schema_version: str | None = None
    cache_hit: bool = False
    cache_key: str | None = None
    cache_entries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_evictions: int = 0
    cache_invalidations: int = 0
    cache_hit_rate: float = 0.0
    retrieval_backend: str = "hybrid-local"
    embedding_backend: str = "fallback"
    bm25_enabled: bool = False
    vector_enabled: bool = False
    reranker_enabled: bool = False
    scope_count: int = 0
    candidate_count: int = 0
    selected_count: int = 0
    lexical_count: int = 0
    vector_count: int = 0
    reranked_count: int = 0
    shard_count: int = 0
    selected_shards: list[str] = field(default_factory=list)
    original_query: str | None = None
    rewritten_query: str | None = None
    expanded_query: str | None = None
    applied_synonyms: list[tuple[str, str]] = field(default_factory=list)
    relationship_count: int = 0
    relationship_tables: list[str] = field(default_factory=list)
    column_annotation_count: int = 0
    used_fallback: bool = False
    degradation_mode: str | None = None
    failure_category: str | None = None
    failure_stage: str | None = None
    active_requests: int = 0
    peak_active_requests: int = 0
    error_message: str | None = None
    latency_ms: float | None = None
    embedding_latency_ms: float | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class RetrievalResult:
    query: str
    connection_id: str
    schema_version: str | None
    tables: list[Any]
    documents: list[TableDocumentation]
    candidates: list[FusedCandidate]
    telemetry: RetrievalTelemetry
    metadata: dict[str, Any] = field(default_factory=dict)


class RetrievalOrchestrator:
    """Production-friendly schema retrieval pipeline."""

    def __init__(
        self,
        *,
        embedding_model: EmbeddingModel | None = None,
        vector_store: VectorStore | None = None,
        documentation_manager: SchemaDocumentationManager | None = None,
        fusion_strategy: Any | None = None,
        reranker: CrossEncoderReranker | None = None,
        query_rewriter: QueryRewriter | None = None,
        cache: RetrievalCache | None = None,
        shard_planner: SchemaShardPlanner | None = None,
        retrieval_logger: RetrievalLogger | None = None,
        telemetry_system: TelemetrySystem | None = None,
        embedding_config: EmbeddingConfig | None = None,
        vector_store_config: VectorStoreConfig | None = None,
        reranker_config: RerankerConfig | None = None,
        synonym_dictionary: SynonymDictionary | None = None,
        synonym_path: str | None = None,
        cache_ttl_seconds: int = 1800,
        cache_max_entries: int = 1000,
        cache_enabled: bool = True,
        shard_threshold: int = 100,
        degradation_manager: DegradationManager | None = None,
        concurrency_controller: RetrievalConcurrencyController | None = None,
        failure_classifier: FailureClassifier | None = None,
        degradation_recovery_threshold: int = 1,
        max_concurrent_requests: int = 8,
        queue_timeout_seconds: float = 1.0,
        retrieval_timeout_seconds: float = 8.0,
        tenant_manager: MultiTenantIsolationManager | None = None,
        access_policy: AccessControlPolicy | None = None,
        sensitive_field_policy: SensitiveFieldPolicy | None = None,
        input_validator: InputValidator | None = None,
        telemetry_repository: RAGTelemetryRepository | None = None,
    ):
        self.embedding_model = embedding_model or create_embedding_model(embedding_config, allow_fallback=True)
        self.vector_store = vector_store or create_vector_store(vector_store_config, collection_name="schema_embeddings")
        self.documentation_manager = documentation_manager or SchemaDocumentationManager()
        self.fusion_strategy = fusion_strategy or WeightedFusion({"lexical": 0.45, "vector": 0.55})
        self.reranker = reranker or CrossEncoderReranker(reranker_config)
        self.relationship_retriever = RelationshipAwareRetriever()
        self.column_retriever = ColumnLevelRetriever()
        if synonym_dictionary is None:
            default_synonym_path = Path(synonym_path) if synonym_path else Path(__file__).resolve().parents[2] / "config" / "synonyms.json"
            synonym_dictionary = SynonymDictionary.from_file(default_synonym_path)
        self.query_rewriter = query_rewriter or QueryRewriter(synonym_dictionary)
        self.cache = cache or RetrievalCache(
            max_entries=cache_max_entries,
            ttl_seconds=cache_ttl_seconds,
            enabled=cache_enabled,
        )
        self.degradation_manager = degradation_manager or DegradationManager(
            recovery_threshold=degradation_recovery_threshold,
        )
        self.concurrency_controller = concurrency_controller or RetrievalConcurrencyController(
            max_concurrent_requests=max_concurrent_requests,
            queue_timeout_seconds=queue_timeout_seconds,
            retrieval_timeout_seconds=retrieval_timeout_seconds,
        )
        self.failure_classifier = failure_classifier or FailureClassifier()
        self.tenant_manager = tenant_manager or MultiTenantIsolationManager()
        self.access_policy = access_policy or AccessControlPolicy(default_effect=AccessEffect.ALLOW)
        self.sensitive_field_policy = sensitive_field_policy or SensitiveFieldPolicy()
        self.input_validator = input_validator or InputValidator()
        self.shard_planner = shard_planner or SchemaShardPlanner(max_shard_size=max(1, shard_threshold), max_query_shards=3)
        self.retrieval_logger = retrieval_logger or RetrievalLogger()
        self.telemetry_system = telemetry_system or TelemetrySystem(repository=telemetry_repository)

        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._tables: dict[str, dict[str, Any]] = defaultdict(dict)
        self._documents: dict[str, dict[str, TableDocumentation]] = defaultdict(dict)
        self._table_texts: dict[str, dict[str, str]] = defaultdict(dict)
        self._table_vectors: dict[str, dict[str, list[float]]] = defaultdict(dict)
        self._bm25_index: dict[str, Any] = {}
        self._schema_versions: dict[str, str] = {}
        self._shards: dict[str, list[Any]] = defaultdict(list)
        self._last_stats: dict[str, Any] = {}
        self._cache_invalidations = 0

    @property
    def last_stats(self) -> dict[str, Any]:
        return dict(self._last_stats)

    @property
    def runtime_metrics(self) -> dict[str, Any]:
        return {
            "cache": {**self.cache.snapshot(), "invalidations": self._cache_invalidations},
            "rewriter": self.query_rewriter.stats,
            "telemetry": self.telemetry_system.get_metrics(),
            "telemetry_snapshot": self.telemetry_system.snapshot(),
            "degradation": self.degradation_manager.export_stats(),
            "concurrency": self.concurrency_controller.snapshot(),
            "log_entries": self.retrieval_logger.size(),
            "connections": len(self._tables),
            "indexed_connections": sum(1 for tables in self._tables.values() if tables),
            "shards": {connection_id: len(shards) for connection_id, shards in self._shards.items()},
            "vector_store_available": self._vector_store_available(),
            "bm25_enabled": BM25Okapi is not None,
        }

    def get_telemetry_events(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return self.telemetry_system.list_events(connection_id=connection_id, query_id=query_id, limit=limit)

    def get_telemetry_event(self, query_id: str) -> dict[str, Any] | None:
        return self.telemetry_system.get_event(query_id)

    def get_degradation_snapshot(self, *, connection_id: str | None = None) -> dict[str, Any]:
        return self.degradation_manager.export_stats(connection_id)

    def list_degradation_events(
        self,
        *,
        connection_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return self.degradation_manager.recent_events(connection_id=connection_id, limit=limit)

    async def initialize(self) -> None:
        await self._ensure_initialized()

    async def index_schema(
        self,
        connection_id: str,
        tables: Iterable[Any],
        *,
        database_name: str | None = None,
        schema_name: str | None = None,
        force: bool = False,
    ) -> list[TableDocumentation]:
        await self._ensure_initialized()
        tables = list(tables)
        schema_version = schema_fingerprint(tables)
        if not force and self._schema_versions.get(connection_id) == schema_version:
            return list(self._documents.get(connection_id, {}).values())

        self.invalidate_connection_cache(connection_id)
        documents = self.documentation_manager.generate_collection(
            tables,
            connection_id=connection_id,
            database_name=database_name,
            schema_name=schema_name,
            version=schema_version,
        )
        descriptions = [self.documentation_manager.to_context_text(doc.table_name) for doc in documents]
        vectors = await self.embedding_model.embed_batch(descriptions)
        ids = [f"{connection_id}:{table_name(table)}" for table in tables]
        metadatas = [
            {
                "connection_id": connection_id,
                "tenant_id": None,
                "project_id": None,
                "table_name": table_name(table),
                "database_name": database_name,
                "schema_name": schema_name,
                "db_type": str(getattr(getattr(table, "db_type", None), "value", getattr(table, "db_type", "")) or "").lower() or None,
                "table_tags": doc.domain_tags,
                "business_domains": doc.domain_tags,
                "table_category": doc.table_category,
                "schema_version": schema_version,
            }
            for table, doc in zip(tables, documents)
        ]

        await self.vector_store.upsert(ids, vectors, metadatas, descriptions)
        self._tables[connection_id] = {table_name(table): table for table in tables}
        self._documents[connection_id] = {doc.table_name: doc for doc in documents}
        self._table_texts[connection_id] = {doc.table_name: text for doc, text in zip(documents, descriptions)}
        self._table_vectors[connection_id] = {doc.table_name: vector for doc, vector in zip(documents, vectors)}
        self._schema_versions[connection_id] = schema_version
        self.cache.invalidate_schema_version(connection_id, schema_version)
        self._bm25_index.pop(connection_id, None)
        if BM25Okapi is not None and descriptions:
            self._bm25_index[connection_id] = BM25Okapi([tokenize_text(text) for text in descriptions])
        build_shards = getattr(self.shard_planner, "build", None) or getattr(self.shard_planner, "build_shards", None)
        try:
            self._shards[connection_id] = build_shards(
                connection_id,
                tables,
                self._documents[connection_id],
                schema_version=schema_version,
            )
        except TypeError:
            self._shards[connection_id] = build_shards(
                connection_id,
                tables,
                self._documents[connection_id],
            )
        self._last_stats = {
            "connection_id": connection_id,
            "indexed": True,
            "schema_version": schema_version,
            "table_count": len(tables),
            "shard_count": len(self._shards[connection_id]),
            "embedding_backend": getattr(self.embedding_model, "model_name", "unknown"),
            "retrieval_backend": "hybrid-local+vector-store",
        }
        return documents

    async def retrieve(
        self,
        query: str,
        connection_id: str,
        top_k: int = 8,
        metadata_filter: MetadataFilter | None = None,
        *,
        include_details: bool = False,
        tenant_scope: TenantScope | dict[str, Any] | None = None,
    ) -> list[Any] | RetrievalResult:
        detailed = await self.retrieve_detailed(
            query,
            connection_id,
            top_k=top_k,
            metadata_filter=metadata_filter,
            tenant_scope=tenant_scope,
        )
        if include_details:
            return detailed
        return detailed.tables

    async def retrieve_detailed(
        self,
        query: str,
        connection_id: str,
        top_k: int = 8,
        metadata_filter: MetadataFilter | None = None,
        tenant_scope: TenantScope | dict[str, Any] | None = None,
    ) -> RetrievalResult:
        try:
            return await self.concurrency_controller.execute(
                connection_id,
                lambda: self._retrieve_detailed_impl(
                    query,
                    connection_id,
                    top_k=top_k,
                    metadata_filter=metadata_filter,
                    tenant_scope=tenant_scope,
                ),
            )
        except (RetrievalConcurrencyError, RetrievalTimeoutError) as exc:
            return await self._build_failure_result(
                query=query,
                connection_id=connection_id,
                metadata_filter=metadata_filter,
                exc=exc,
            )
        except Exception as exc:  # pragma: no cover - production safety net
            return await self._build_failure_result(
                query=query,
                connection_id=connection_id,
                metadata_filter=metadata_filter,
                exc=exc,
            )

    async def _retrieve_detailed_impl(
        self,
        query: str,
        connection_id: str,
        top_k: int = 8,
        metadata_filter: MetadataFilter | None = None,
        tenant_scope: TenantScope | dict[str, Any] | None = None,
    ) -> RetrievalResult:
        started = perf_counter()
        await self._ensure_initialized()
        validation = self.input_validator.validate_query(query)
        if not validation.is_valid:
            raise InputValidationError(validation)
        tables = self._tables.get(connection_id, {})
        documents = self._documents.get(connection_id, {})
        schema_version = self._schema_versions.get(connection_id)
        resolved_filter = metadata_filter or MetadataFilter.infer_from_query(query, connection_id=connection_id)
        tenant_scope_obj = self.tenant_manager.normalize_scope(tenant_scope, connection_id=connection_id)
        isolation_key = self.tenant_manager.isolation_key(tenant_scope_obj)
        resolved_filter = resolved_filter.merge(
            MetadataFilter(
                tenant_id=tenant_scope_obj.tenant_id,
                project_id=tenant_scope_obj.project_id,
                connection_id=tenant_scope_obj.connection_id or connection_id,
                database_name=tenant_scope_obj.database_name,
                schema_name=tenant_scope_obj.schema_name,
                db_type=tenant_scope_obj.db_type,
                business_domains=set(tenant_scope_obj.business_domains),
            )
        )
        rewrite_result = await self._rewrite_query(query, connection_id, resolved_filter)
        query_id = f"rag_{uuid4().hex[:12]}"
        telemetry = RetrievalTelemetry(
            query_id=query_id,
            connection_id=connection_id,
            schema_version=schema_version,
            retrieval_backend="hybrid-local+vector-store" if self._vector_store_available() else "lexical-only",
            embedding_backend=getattr(self.embedding_model, "model_name", "fallback"),
            bm25_enabled=BM25Okapi is not None and connection_id in self._bm25_index,
            vector_enabled=self._vector_store_available() and not self.degradation_manager.should_use_bm25_only(connection_id),
            reranker_enabled=getattr(self.reranker.config, "enabled", True),
            original_query=query,
            rewritten_query=rewrite_result.rewritten_query,
            expanded_query=rewrite_result.expanded_query,
            applied_synonyms=list(rewrite_result.applied_synonyms),
            degradation_mode=self.degradation_manager.current_mode(connection_id).value,
            cache_key=isolation_key,
        )
        concurrency_snapshot = self.concurrency_controller.snapshot()
        telemetry.active_requests = int(concurrency_snapshot.get("active_requests", 0) or 0)
        telemetry.peak_active_requests = int(concurrency_snapshot.get("peak_active_requests", 0) or 0)
        if self.degradation_manager.should_use_bm25_only(connection_id):
            telemetry.retrieval_backend = "bm25-only-degraded"
            telemetry.used_fallback = True

        if not tables:
            telemetry.latency_ms = round((perf_counter() - started) * 1000.0, 2)
            result = self._build_result(
                query=query,
                connection_id=connection_id,
                schema_version=schema_version,
                tables=[],
                documents=[],
                candidates=[],
                telemetry=telemetry,
                rewrite_result=rewrite_result,
            )
            self._record_observability(result)
            return result

        table_items = [table for table in tables.values() if resolved_filter.matches(self._metadata_for_table(connection_id, table))]
        if not table_items:
            table_items = list(tables.values())

        principal_scope = AccessScope.from_any(
            {
                "tenant_id": tenant_scope_obj.tenant_id,
                "project_id": tenant_scope_obj.project_id,
                "connection_id": tenant_scope_obj.connection_id or connection_id,
                "database_name": tenant_scope_obj.database_name,
                "schema_name": tenant_scope_obj.schema_name,
                "db_type": tenant_scope_obj.db_type,
                "business_domains": tenant_scope_obj.business_domains,
            }
        )
        access_decisions = [
            (table, self.access_policy.evaluate(principal_scope, self._metadata_for_table(connection_id, table)))
            for table in table_items
        ]
        allowed_items = [table for table, decision in access_decisions if decision.allowed]
        denied_decisions = [decision for _, decision in access_decisions if decision.denied]
        if allowed_items:
            table_items = allowed_items
        elif denied_decisions:
            denied_result = await self._build_failure_result(
                query=query,
                connection_id=connection_id,
                metadata_filter=metadata_filter,
                exc=PermissionError(f"Access denied: {denied_decisions[0].reason or 'policy denied access'}"),
            )
            denied_result.metadata["access"] = {
                "principal_scope": principal_scope.to_dict(),
                "denied_rules": [decision.to_dict() for decision in denied_decisions[:10]],
            }
            return denied_result

        selected_shards = self._select_shards(connection_id, rewrite_result.expanded_query, table_items)
        telemetry.shard_count = len(self._shards.get(connection_id, []))
        telemetry.selected_shards = [shard.name for shard in selected_shards]
        selected_table_names = {name for shard in selected_shards for name in shard.table_names}
        if selected_table_names:
            filtered_items = [table for table in table_items if table_name(table) in selected_table_names]
            if filtered_items:
                table_items = filtered_items

        scoped_connection_id = f"{connection_id}:{isolation_key}"
        cached = self.cache.get(rewrite_result.expanded_query, scoped_connection_id, schema_version, top_k=top_k)
        if cached is not None:
            cache_stats = self.cache.snapshot()
            cached.telemetry.query_id = cached.telemetry.query_id or query_id
            cached.telemetry.cache_hit = True
            cached.telemetry.latency_ms = round((perf_counter() - started) * 1000.0, 2)
            cached.telemetry.shard_count = telemetry.shard_count
            cached.telemetry.selected_shards = list(telemetry.selected_shards)
            cached.telemetry.cache_entries = cache_stats["entries"]
            cached.telemetry.cache_hits = cache_stats["hits"]
            cached.telemetry.cache_misses = cache_stats["misses"]
            cached.telemetry.cache_evictions = cache_stats["evictions"]
            cached.telemetry.cache_invalidations = cache_stats["invalidations"]
            cached.telemetry.cache_hit_rate = cache_stats["hit_rate"]
            self._record_last_stats(cached, table_count=len(tables))
            self._record_observability(cached)
            return cached

        retrieval_pool_size = max(top_k * 3, 12)
        embedding_started = perf_counter()
        query_vector = await self.embedding_model.embed_text(rewrite_result.expanded_query)
        telemetry.embedding_latency_ms = round((perf_counter() - embedding_started) * 1000.0, 2)
        lexical_candidates = self._lexical_retrieve(
            connection_id,
            rewrite_result.expanded_query,
            table_items,
            resolved_filter,
            retrieval_pool_size,
        )
        vector_candidates = await self._vector_retrieve(
            connection_id,
            query_vector,
            resolved_filter,
            retrieval_pool_size,
            table_items,
            telemetry,
        )
        telemetry.scope_count = len(table_items)
        telemetry.lexical_count = len(lexical_candidates)
        telemetry.vector_count = len(vector_candidates)

        fused = self.fusion_strategy.fuse({"lexical": lexical_candidates, "vector": vector_candidates})
        telemetry.candidate_count = len(fused)
        rerank_candidates = [
            RerankCandidate(
                key=item.key,
                payload=item.payload,
                score=item.score,
                source_scores=dict(item.source_scores),
                metadata=dict(item.metadata),
                text=self._table_texts.get(connection_id, {}).get(item.key, ""),
            )
            for item in fused[: max(top_k, 20)]
        ]
        if rerank_candidates and getattr(self.reranker.config, "enabled", True):
            reranked = await self.reranker.rerank(rewrite_result.expanded_query, rerank_candidates)
            fused = [
                FusedCandidate(
                    key=item.key,
                    payload=item.payload,
                    score=item.score,
                    source_scores=dict(item.source_scores),
                    metadata=dict(item.metadata),
                    rank=index + 1,
                )
                for index, item in enumerate(reranked)
            ]
            telemetry.reranked_count = len(reranked)

        selected_items = fused[:top_k]
        telemetry.selected_count = len(selected_items)
        selected_tables = [
            self._tables[connection_id].get(item.key, item.payload)
            for item in selected_items
            if item.key in self._tables.get(connection_id, {})
        ]
        selected_docs = [documents[item.key] for item in selected_items if item.key in documents]
        relationship_result = self.relationship_retriever.expand(
            rewrite_result.expanded_query,
            selected_tables,
            documents,
            all_tables=self._tables.get(connection_id, {}),
            allowed_table_names=tables.keys(),
        )
        relationship_tables = [
            table for table in relationship_result.related_tables if table_name(table) not in {table_name(item) for item in selected_tables}
        ]
        relationship_tables = [
            table
            for table in relationship_tables
            if self.access_policy.is_allowed(principal_scope, self._metadata_for_table(connection_id, table))
        ]
        all_tables = [*selected_tables, *relationship_tables]
        all_docs = [documents[key] for key in [table_name(table) for table in all_tables] if key in documents]
        column_annotations = self.column_retriever.rank(
            rewrite_result.expanded_query,
            documents,
            table_names=[table_name(table) for table in all_tables],
        )
        relationship_clues = [
            {
                "source_table": edge.source_table,
                "target_table": edge.target_table,
                "source_column": edge.source_column,
                "target_column": edge.target_column,
                "confidence": edge.confidence,
                "reason": edge.reason,
            }
            for edge in relationship_result.edges
        ]
        telemetry.latency_ms = round((perf_counter() - started) * 1000.0, 2)
        telemetry.relationship_count = len(relationship_clues)
        telemetry.relationship_tables = [table_name(table) for table in relationship_tables]
        telemetry.column_annotation_count = sum(len(items) for items in column_annotations.values())
        result = self._build_result(
            query=query,
            connection_id=connection_id,
            schema_version=schema_version,
            tables=all_tables,
            documents=all_docs,
            candidates=selected_items,
            telemetry=telemetry,
            rewrite_result=rewrite_result,
            relationship_clues=relationship_clues,
            column_annotations=column_annotations,
            input_validation=validation.to_dict(),
            access_metadata={
                "principal_scope": principal_scope.to_dict(),
                "denied_count": len(denied_decisions),
                "allowed_tables": [table_name(item) for item in all_tables],
            },
        )
        self.cache.put(rewrite_result.expanded_query, scoped_connection_id, schema_version, result, top_k=top_k)
        cache_stats = self.cache.snapshot()
        result.telemetry.cache_entries = cache_stats["entries"]
        result.telemetry.cache_hits = cache_stats["hits"]
        result.telemetry.cache_misses = cache_stats["misses"]
        result.telemetry.cache_evictions = cache_stats["evictions"]
        result.telemetry.cache_invalidations = cache_stats["invalidations"]
        result.telemetry.cache_hit_rate = cache_stats["hit_rate"]
        self._record_last_stats(result, table_count=len(tables))
        self._record_observability(result)
        return result

    async def health(self, connection_id: str | None = None) -> dict[str, Any]:
        await self._ensure_initialized()
        vector_health = await self.vector_store.health_check()
        if connection_id is None:
            return {
                "vector_store": vector_health,
                "connections": len(self._tables),
                "cache": {**self.cache.snapshot(), "invalidations": self._cache_invalidations},
                "rewriter": self.query_rewriter.stats,
                "degradation": self.degradation_manager.export_stats(),
                "concurrency": self.concurrency_controller.snapshot(),
            }
        tables = self._tables.get(connection_id, {})
        documents = self._documents.get(connection_id, {})
        return {
            "connection_id": connection_id,
            "schema_version": self._schema_versions.get(connection_id),
            "is_indexed": bool(tables),
            "table_count": len(tables),
            "vector_count": len(documents),
            "shard_count": len(self._shards.get(connection_id, [])),
            "vector_store": vector_health,
            "bm25_enabled": connection_id in self._bm25_index,
            "cache": {**self.cache.snapshot(), "invalidations": self._cache_invalidations},
            "rewriter": self.query_rewriter.stats,
            "degradation": self.degradation_manager.export_stats(connection_id),
            "concurrency": self.concurrency_controller.snapshot(),
            "updated_at": self._last_stats.get("updated_at"),
        }

    def invalidate_connection_cache(self, connection_id: str) -> int:
        removed = self.cache.invalidate_connection(connection_id)
        self._cache_invalidations += removed
        return removed

    def _metadata_for_table(self, connection_id: str, table: Any) -> dict[str, Any]:
        doc = self._documents.get(connection_id, {}).get(table_name(table))
        return {
            "tenant_id": doc.metadata.get("tenant_id") if doc else None,
            "project_id": doc.metadata.get("project_id") if doc else None,
            "connection_id": connection_id,
            "table_name": table_name(table),
            "database_name": doc.database_name if doc else getattr(table, "database_name", None),
            "schema_name": doc.schema_name if doc else getattr(table, "schema_name", None),
            "db_type": str(getattr(getattr(table, "db_type", None), "value", getattr(table, "db_type", "")) or "").lower() or None,
            "table_tags": doc.domain_tags if doc else [],
            "business_domains": doc.domain_tags if doc else [],
            "table_category": doc.table_category if doc else None,
        }

    async def _rewrite_query(
        self,
        query: str,
        connection_id: str,
        metadata_filter: MetadataFilter | None,
    ) -> RewriteResult:
        domain = None
        if metadata_filter and metadata_filter.business_domains:
            domain = sorted(metadata_filter.business_domains)[0]
        return await self.query_rewriter.rewrite(query, connection_id, domain=domain)

    def _select_shards(self, connection_id: str, query: str, table_items: list[Any]) -> list[Any]:
        shards = self._shards.get(connection_id, [])
        if not shards:
            return []
        select_shards = getattr(self.shard_planner, "select", None) or getattr(self.shard_planner, "select_shards", None)
        selected = select_shards(query, shards)
        allowed = {table_name(table) for table in table_items}
        if allowed:
            selected = [shard for shard in selected if allowed.intersection(shard.table_names)]
        return selected

    def _lexical_retrieve(
        self,
        connection_id: str,
        query: str,
        table_items: list[Any],
        metadata_filter: MetadataFilter,
        top_k: int,
    ) -> list[RetrievalCandidate]:
        query_tokens = set(tokenize_text(query))
        candidates: list[RetrievalCandidate] = []
        for table in table_items:
            meta = self._metadata_for_table(connection_id, table)
            if not metadata_filter.matches(meta):
                continue
            text = self._table_texts.get(connection_id, {}).get(table_name(table), "")
            tokens = set(tokenize_text(text))
            overlap = len(query_tokens & tokens)
            score = overlap + len(query_tokens & set(tokenize_text(table_name(table)))) * 2
            if score > 0:
                candidates.append(
                    RetrievalCandidate(
                        key=table_name(table),
                        payload=table,
                        score=float(score),
                        source="lexical",
                        metadata=meta,
                    )
                )
        if not candidates:
            candidates = [
                RetrievalCandidate(
                    key=table_name(table),
                    payload=table,
                    score=0.1,
                    source="lexical",
                    metadata=self._metadata_for_table(connection_id, table),
                )
                for table in table_items
            ]
        candidates.sort(key=lambda item: item.score, reverse=True)
        return candidates[:top_k]

    async def _vector_retrieve(
        self,
        connection_id: str,
        query_vector: list[float],
        metadata_filter: MetadataFilter,
        top_k: int,
        table_items: list[Any],
        telemetry: RetrievalTelemetry,
    ) -> list[RetrievalCandidate]:
        if not self._vector_store_available():
            return []
        if self.degradation_manager.should_use_bm25_only(connection_id):
            try:
                health = await self.vector_store.health_check()
            except Exception:
                health = None
            if health is not None and getattr(health, "is_healthy", False):
                self.degradation_manager.record_recovery_probe(connection_id, reason="vector store health probe recovered")
            if self.degradation_manager.should_use_bm25_only(connection_id):
                telemetry.used_fallback = True
                telemetry.degradation_mode = self.degradation_manager.current_mode(connection_id).value
                return []
        where = metadata_filter.merge(MetadataFilter(connection_id=connection_id)).to_where_clause()
        try:
            results = await self.vector_store.query(query_vector, top_k=max(top_k, 12), filter=where)
            self.degradation_manager.record_recovery_probe(connection_id, reason="vector query succeeded")
        except VectorStoreTimeoutError as exc:
            self.degradation_manager.record_timeout(connection_id, reason=str(exc))
            telemetry.used_fallback = True
            telemetry.degradation_mode = self.degradation_manager.current_mode(connection_id).value
            telemetry.failure_category = "timeout_error"
            telemetry.failure_stage = "vector_retrieval"
            telemetry.error_message = str(exc)
            self._last_stats["vector_error"] = str(exc)
            return []
        except VectorStoreUnavailableError as exc:
            self.degradation_manager.record_vector_store_unavailable(connection_id, reason=str(exc))
            telemetry.used_fallback = True
            telemetry.degradation_mode = self.degradation_manager.current_mode(connection_id).value
            telemetry.failure_category = "vector_store_unavailable"
            telemetry.failure_stage = "vector_retrieval"
            telemetry.error_message = str(exc)
            self._last_stats["vector_error"] = str(exc)
            return []
        except Exception as exc:  # pragma: no cover
            self._last_stats["vector_error"] = str(exc)
            classification = self.failure_classifier.classify_exception(exc, stage="vector_retrieval")
            telemetry.used_fallback = True
            telemetry.failure_category = classification.category
            telemetry.failure_stage = classification.stage
            telemetry.error_message = classification.message
            return []
        candidates: list[RetrievalCandidate] = []
        allowed = {table_name(table) for table in table_items}
        for result in results:
            payload = self._tables.get(connection_id, {}).get(result.metadata.get("table_name"), None)
            key = result.metadata.get("table_name") or result.id.split(":")[-1]
            if allowed and key not in allowed:
                continue
            candidates.append(
                RetrievalCandidate(
                    key=key,
                    payload=payload,
                    score=float(result.score),
                    source="vector",
                    metadata=dict(result.metadata),
                )
            )
        candidates.sort(key=lambda item: item.score, reverse=True)
        return candidates[:top_k]

    async def _build_failure_result(
        self,
        *,
        query: str,
        connection_id: str,
        metadata_filter: MetadataFilter | None,
        exc: Exception,
    ) -> RetrievalResult:
        schema_version = self._schema_versions.get(connection_id)
        resolved_filter = metadata_filter or MetadataFilter.infer_from_query(query, connection_id=connection_id)
        rewrite_result = await self._rewrite_query(query, connection_id, resolved_filter)
        query_id = f"rag_{uuid4().hex[:12]}"
        classification = self.failure_classifier.classify_exception(
            exc,
            stage=getattr(exc, "stage", None) or "retrieval",
        )
        if classification.category == "timeout_error":
            self.degradation_manager.record_timeout(connection_id, reason=classification.message)
        elif classification.category == "vector_store_unavailable":
            self.degradation_manager.record_vector_store_unavailable(connection_id, reason=classification.message)
        telemetry = RetrievalTelemetry(
            query_id=query_id,
            connection_id=connection_id,
            schema_version=schema_version,
            retrieval_backend="bm25-only-degraded" if self.degradation_manager.should_use_bm25_only(connection_id) else "hybrid-local+vector-store",
            embedding_backend=getattr(self.embedding_model, "model_name", "fallback"),
            bm25_enabled=BM25Okapi is not None and connection_id in self._bm25_index,
            vector_enabled=self._vector_store_available() and not self.degradation_manager.should_use_bm25_only(connection_id),
            reranker_enabled=getattr(self.reranker.config, "enabled", True),
            original_query=query,
            rewritten_query=rewrite_result.rewritten_query,
            expanded_query=rewrite_result.expanded_query,
            applied_synonyms=list(rewrite_result.applied_synonyms),
            used_fallback=self.degradation_manager.should_use_bm25_only(connection_id),
            degradation_mode=self.degradation_manager.current_mode(connection_id).value,
            failure_category=classification.category,
            failure_stage=classification.stage,
            error_message=classification.message,
            latency_ms=0.0,
        )
        result = self._build_result(
            query=query,
            connection_id=connection_id,
            schema_version=schema_version,
            tables=[],
            documents=[],
            candidates=[],
            telemetry=telemetry,
            rewrite_result=rewrite_result,
        )
        self._record_last_stats(result, table_count=len(self._tables.get(connection_id, {})))
        self._record_observability(result)
        return result

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            if hasattr(self.vector_store, "initialize"):
                await self.vector_store.initialize()
            self._initialized = True

    def _vector_store_available(self) -> bool:
        return self.vector_store is not None

    def _build_result(
        self,
        *,
        query: str,
        connection_id: str,
        schema_version: str | None,
        tables: list[Any],
        documents: list[TableDocumentation],
        candidates: list[FusedCandidate],
        telemetry: RetrievalTelemetry,
        rewrite_result: RewriteResult,
        relationship_clues: list[dict[str, Any]] | None = None,
        column_annotations: dict[str, list[dict[str, Any]]] | None = None,
        input_validation: dict[str, Any] | None = None,
        access_metadata: dict[str, Any] | None = None,
    ) -> RetrievalResult:
        cache_stats = self.cache.snapshot()
        telemetry.cache_hit = telemetry.cache_hit or False
        metadata = {
            "query_id": telemetry.query_id,
            "original_query": rewrite_result.original_query,
            "rewritten_query": rewrite_result.rewritten_query,
            "expanded_query": rewrite_result.expanded_query,
            "applied_synonyms": list(rewrite_result.applied_synonyms),
            "expanded_terms": list(rewrite_result.metadata.get("expanded_terms", [])),
            "relationship_clues": list(relationship_clues or []),
            "column_annotations": dict(column_annotations or {}),
            "failure_category": telemetry.failure_category,
            "failure_stage": telemetry.failure_stage,
            "degradation_mode": telemetry.degradation_mode,
            "cache": cache_stats,
            "input_validation": dict(input_validation or {}),
        }
        if access_metadata:
            metadata["access"] = dict(access_metadata)
        return RetrievalResult(
            query=query,
            connection_id=connection_id,
            schema_version=schema_version,
            tables=tables,
            documents=documents,
            candidates=candidates,
            telemetry=telemetry,
            metadata=metadata,
        )

    def record_generation_artifacts(
        self,
        query_id: str | None,
        *,
        prompt_schema: str | None = None,
        final_sql: str | None = None,
    ) -> None:
        if not query_id:
            return
        self.retrieval_logger.attach_generation(query_id, prompt_schema=prompt_schema, final_sql=final_sql)

    def get_query_details(self, query_id: str) -> dict[str, Any] | None:
        return self.retrieval_logger.get(query_id)

    def list_query_logs(self, *, connection_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self.retrieval_logger.list(connection_id=connection_id, limit=limit)

    def _record_last_stats(self, result: RetrievalResult, *, table_count: int) -> None:
        cache_stats = self.cache.snapshot()
        self._last_stats = {
            "connection_id": result.connection_id,
            "query_id": result.telemetry.query_id,
            "query": result.query,
            "schema_version": result.schema_version,
            "selected": len(result.tables),
            "candidates": len(result.candidates),
            "retrieval_backend": result.telemetry.retrieval_backend,
            "embedding_backend": result.telemetry.embedding_backend,
            "bm25_enabled": result.telemetry.bm25_enabled,
            "vector_enabled": result.telemetry.vector_enabled,
            "scope_count": result.telemetry.scope_count,
            "lexical_count": result.telemetry.lexical_count,
            "vector_count": result.telemetry.vector_count,
            "reranker_enabled": result.telemetry.reranker_enabled,
            "table_count": table_count,
            "cache_hit": result.telemetry.cache_hit,
            "cache_entries": cache_stats["entries"],
            "cache_hits": cache_stats["hits"],
            "cache_misses": cache_stats["misses"],
            "cache_evictions": cache_stats["evictions"],
            "cache_invalidations": cache_stats["invalidations"],
            "cache_hit_rate": cache_stats["hit_rate"],
            "shard_count": result.telemetry.shard_count,
            "selected_shards": list(result.telemetry.selected_shards),
            "query_rewritten": bool(result.telemetry.applied_synonyms),
            "rewritten_query": result.telemetry.rewritten_query,
            "latency_ms": result.telemetry.latency_ms,
            "embedding_latency_ms": result.telemetry.embedding_latency_ms,
            "relationship_count": result.telemetry.relationship_count,
            "relationship_tables": list(result.telemetry.relationship_tables),
            "column_annotation_count": result.telemetry.column_annotation_count,
            "degradation_mode": result.telemetry.degradation_mode,
            "failure_category": result.telemetry.failure_category,
            "failure_stage": result.telemetry.failure_stage,
            "active_requests": result.telemetry.active_requests,
            "peak_active_requests": result.telemetry.peak_active_requests,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _record_observability(self, result: RetrievalResult) -> None:
        query_id = result.telemetry.query_id or result.metadata.get("query_id")
        selected_tables = [table_name(table) for table in result.tables]
        candidate_scores = [
            {
                "table": item.key,
                "score": item.score,
                "source_scores": dict(item.source_scores),
                "rank": item.rank,
            }
            for item in result.candidates
        ]
        self.retrieval_logger.record(
            RetrievalLogEntry(
                query_id=str(query_id),
                connection_id=result.connection_id,
                original_query=str(result.metadata.get("original_query") or result.query),
                rewritten_query=result.metadata.get("rewritten_query"),
                expanded_query=result.metadata.get("expanded_query"),
                selected_tables=selected_tables,
                candidate_scores=candidate_scores,
                reranked_tables=[item.key for item in result.candidates],
                cache_hit=bool(result.telemetry.cache_hit),
                used_fallback=bool(result.telemetry.used_fallback),
                degradation_mode=result.telemetry.degradation_mode,
                failure_category=result.telemetry.failure_category,
                failure_stage=result.telemetry.failure_stage,
                retrieval_latency_ms=result.telemetry.latency_ms,
                stage_latencies={
                    "retrieval_ms": float(result.telemetry.latency_ms or 0.0),
                    "embedding_ms": float(result.telemetry.embedding_latency_ms or 0.0),
                },
                error_message=result.telemetry.error_message,
            )
        )
        self.telemetry_system.record_retrieval(
            RetrievalTelemetryEvent(
                query_id=str(query_id),
                connection_id=result.connection_id,
                retrieval_latency_ms=float(result.telemetry.latency_ms or 0.0),
                embedding_latency_ms=float(result.telemetry.embedding_latency_ms or 0.0),
                lexical_count=int(result.telemetry.lexical_count or 0),
                vector_count=int(result.telemetry.vector_count or 0),
                cache_hit=bool(result.telemetry.cache_hit),
                used_fallback=bool(result.telemetry.used_fallback),
                error_type=result.telemetry.failure_category,
                failure_stage=result.telemetry.failure_stage,
                selected_tables=selected_tables,
                degradation_mode=result.telemetry.degradation_mode,
                error_message=result.telemetry.error_message,
                retrieval_backend=result.telemetry.retrieval_backend,
                embedding_backend=result.telemetry.embedding_backend,
            )
        )
