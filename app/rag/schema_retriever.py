from __future__ import annotations

from typing import Any

from app.rag.embedding import EmbeddingConfig
from app.rag.business_knowledge import BusinessKnowledgeRepository
from app.rag.cache import RetrievalCache
from app.rag.fewshot_integration import FewShotIntegration
from app.rag.multi_tenant import MultiTenantIsolationManager, TenantScope
from app.rag.profiling import ProfilingStore
from app.rag.query_rewriter import QueryRewriter
from app.rag.reranker import RerankerConfig
from app.rag.schema_doc import SchemaDocumentationManager
from app.rag.sharding import SchemaShardPlanner
from app.rag.synonym_dict import SynonymDictionary
from app.rag.telemetry import TelemetrySystem
from app.db.repositories.rag_telemetry_repo import RAGTelemetryRepository
from app.db.repositories.rag_query_log_repo import RAGQueryLogRepository
from app.rag.logging import RetrievalLogger
from app.rag.vector_store import VectorStoreConfig
from app.rag.orchestrator import RetrievalOrchestrator


class SchemaRetriever:
    """Compatibility wrapper around the production retrieval orchestrator.

    This mirrors the current prototype interface so the main NL2SQL chain can
    switch over with minimal glue code when the caller is ready.
    """

    def __init__(
        self,
        persist_directory: str,
        *,
        embedding_config: EmbeddingConfig | None = None,
        vector_store_config: VectorStoreConfig | None = None,
        reranker_config: RerankerConfig | None = None,
        synonym_path: str | None = None,
        cache_ttl_seconds: int = 1800,
        cache_max_entries: int = 1000,
        cache_enabled: bool = True,
        shard_threshold: int = 100,
        degradation_recovery_threshold: int = 1,
        max_concurrent_requests: int = 8,
        queue_timeout_seconds: float = 1.0,
        retrieval_timeout_seconds: float = 8.0,
        profiling_store: ProfilingStore | None = None,
        business_knowledge_repository: BusinessKnowledgeRepository | None = None,
        tenant_manager: MultiTenantIsolationManager | None = None,
        telemetry_repository: RAGTelemetryRepository | None = None,
        telemetry_system: TelemetrySystem | None = None,
        query_log_repository: RAGQueryLogRepository | None = None,
    ):
        self.persist_directory = persist_directory
        query_rewriter = QueryRewriter(SynonymDictionary.from_file(synonym_path)) if synonym_path else None
        documentation_manager = SchemaDocumentationManager(
            profiling_store=profiling_store,
            business_knowledge_repository=business_knowledge_repository,
        )
        self._orchestrator = RetrievalOrchestrator(
            documentation_manager=documentation_manager,
            embedding_config=embedding_config,
            vector_store_config=vector_store_config or VectorStoreConfig(persist_directory=persist_directory),
            reranker_config=reranker_config,
            query_rewriter=query_rewriter,
            cache=RetrievalCache(
                max_entries=cache_max_entries,
                ttl_seconds=cache_ttl_seconds,
                enabled=cache_enabled,
            ),
            shard_planner=SchemaShardPlanner(max_shard_size=max(1, shard_threshold), max_query_shards=3),
            synonym_path=synonym_path,
            degradation_recovery_threshold=degradation_recovery_threshold,
            max_concurrent_requests=max_concurrent_requests,
            queue_timeout_seconds=queue_timeout_seconds,
            retrieval_timeout_seconds=retrieval_timeout_seconds,
            tenant_manager=tenant_manager,
            telemetry_repository=telemetry_repository,
            telemetry_system=telemetry_system,
            retrieval_logger=RetrievalLogger(repository=query_log_repository),
        )

    @property
    def last_stats(self) -> dict[str, Any]:
        return self._orchestrator.last_stats

    @property
    def runtime_metrics(self) -> dict[str, Any]:
        return self._orchestrator.runtime_metrics

    async def index_schema(
        self,
        connection_id: str,
        tables: list[Any],
        *,
        database_name: str | None = None,
        schema_name: str | None = None,
        force: bool = False,
    ) -> None:
        await self._orchestrator.index_schema(
            connection_id,
            tables,
            database_name=database_name,
            schema_name=schema_name,
            force=force,
        )

    async def retrieve(
        self,
        query: str,
        connection_id: str,
        top_k: int = 8,
        metadata_filter: Any | None = None,
        *,
        include_details: bool = False,
        tenant_scope: TenantScope | dict[str, Any] | None = None,
    ) -> list[Any] | Any:
        return await self._orchestrator.retrieve(
            query,
            connection_id,
            top_k=top_k,
            metadata_filter=metadata_filter,
            include_details=include_details,
            tenant_scope=tenant_scope,
        )

    async def retrieve_detailed(
        self,
        query: str,
        connection_id: str,
        top_k: int = 8,
        metadata_filter: Any | None = None,
        tenant_scope: TenantScope | dict[str, Any] | None = None,
    ) -> Any:
        return await self._orchestrator.retrieve(
            query,
            connection_id,
            top_k=top_k,
            metadata_filter=metadata_filter,
            include_details=True,
            tenant_scope=tenant_scope,
        )

    def invalidate_connection_cache(self, connection_id: str) -> int:
        return self._orchestrator.invalidate_connection_cache(connection_id)

    def record_generation_artifacts(
        self,
        query_id: str | None,
        *,
        prompt_schema: str | None = None,
        final_sql: str | None = None,
    ) -> None:
        self._orchestrator.record_generation_artifacts(query_id, prompt_schema=prompt_schema, final_sql=final_sql)

    def get_query_details(self, query_id: str) -> dict[str, Any] | None:
        return self._orchestrator.get_query_details(query_id)

    def list_query_logs(self, *, connection_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self._orchestrator.list_query_logs(connection_id=connection_id, limit=limit)

    def get_degradation_snapshot(self, *, connection_id: str | None = None) -> dict[str, Any]:
        return self._orchestrator.get_degradation_snapshot(connection_id=connection_id)

    def list_degradation_events(
        self,
        *,
        connection_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return self._orchestrator.list_degradation_events(connection_id=connection_id, limit=limit)

    def get_telemetry_events(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return self._orchestrator.get_telemetry_events(connection_id=connection_id, query_id=query_id, limit=limit)

    def get_telemetry_event(self, query_id: str) -> dict[str, Any] | None:
        return self._orchestrator.get_telemetry_event(query_id)

    def get_telemetry_dashboard(self) -> dict[str, Any]:
        return self._orchestrator.telemetry_system.dashboard()

    def list_telemetry_history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._orchestrator.telemetry_system.list_snapshot_history(limit=limit)
