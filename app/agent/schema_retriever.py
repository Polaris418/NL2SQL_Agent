from __future__ import annotations

from typing import Any

from app.rag.embedding import EmbeddingConfig
from app.rag.business_knowledge import BusinessKnowledgeRepository
from app.rag.metadata_filter import MetadataFilter
from app.rag.multi_tenant import MultiTenantIsolationManager, TenantScope
from app.rag.profiling import ProfilingStore
from app.rag.reranker import RerankerConfig
from app.rag.schema_retriever import SchemaRetriever as ProductionSchemaRetriever
from app.rag.telemetry import TelemetrySystem
from app.db.repositories.rag_telemetry_repo import RAGTelemetryRepository
from app.db.repositories.rag_query_log_repo import RAGQueryLogRepository
from app.rag.vector_store import VectorStoreConfig


class SchemaRetriever:
    """Compatibility wrapper for the production RAG retriever.

    The NL2SQL agent still imports this module path, so we keep the public
    interface stable while delegating to the new production-grade
    implementation under ``app.rag``.
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
        self._delegate = ProductionSchemaRetriever(
            persist_directory,
            embedding_config=embedding_config,
            vector_store_config=vector_store_config,
            reranker_config=reranker_config,
            synonym_path=synonym_path,
            cache_ttl_seconds=cache_ttl_seconds,
            cache_max_entries=cache_max_entries,
            cache_enabled=cache_enabled,
            shard_threshold=shard_threshold,
            degradation_recovery_threshold=degradation_recovery_threshold,
            max_concurrent_requests=max_concurrent_requests,
            queue_timeout_seconds=queue_timeout_seconds,
            retrieval_timeout_seconds=retrieval_timeout_seconds,
            profiling_store=profiling_store,
            business_knowledge_repository=business_knowledge_repository,
            tenant_manager=tenant_manager,
            telemetry_repository=telemetry_repository,
            telemetry_system=telemetry_system,
            query_log_repository=query_log_repository,
        )

    @property
    def last_stats(self) -> dict[str, Any]:
        return self._delegate.last_stats

    @property
    def embedding_backend(self) -> str:
        return str(self.last_stats.get("embedding_backend", "unknown"))

    @property
    def retrieval_backend(self) -> str:
        return str(self.last_stats.get("retrieval_backend", "unknown"))

    @property
    def runtime_metrics(self) -> dict[str, Any]:
        return self._delegate.runtime_metrics

    async def index_schema(
        self,
        connection_id: str,
        tables: list[Any],
        *,
        database_name: str | None = None,
        schema_name: str | None = None,
        force: bool = False,
    ) -> None:
        await self._delegate.index_schema(
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
        metadata_filter: MetadataFilter | None = None,
        tenant_scope: TenantScope | dict[str, Any] | None = None,
    ) -> list[Any]:
        return await self._delegate.retrieve(
            query,
            connection_id,
            top_k=top_k,
            metadata_filter=metadata_filter,
            tenant_scope=tenant_scope,
        )

    async def retrieve_detailed(
        self,
        query: str,
        connection_id: str,
        top_k: int = 8,
        metadata_filter: MetadataFilter | None = None,
        tenant_scope: TenantScope | dict[str, Any] | None = None,
    ) -> Any:
        return await self._delegate.retrieve(
            query,
            connection_id,
            top_k=top_k,
            metadata_filter=metadata_filter,
            include_details=True,
            tenant_scope=tenant_scope,
        )

    def invalidate_connection_cache(self, connection_id: str) -> int:
        return self._delegate.invalidate_connection_cache(connection_id)

    def record_generation_artifacts(
        self,
        query_id: str | None,
        *,
        prompt_schema: str | None = None,
        final_sql: str | None = None,
    ) -> None:
        self._delegate.record_generation_artifacts(query_id, prompt_schema=prompt_schema, final_sql=final_sql)

    def get_query_details(self, query_id: str) -> dict[str, Any] | None:
        return self._delegate.get_query_details(query_id)

    def list_query_logs(self, *, connection_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return self._delegate.list_query_logs(connection_id=connection_id, limit=limit)

    def get_degradation_snapshot(self, *, connection_id: str | None = None) -> dict[str, Any]:
        return self._delegate.get_degradation_snapshot(connection_id=connection_id)

    def list_degradation_events(
        self,
        *,
        connection_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return self._delegate.list_degradation_events(connection_id=connection_id, limit=limit)

    def get_telemetry_events(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return self._delegate.get_telemetry_events(connection_id=connection_id, query_id=query_id, limit=limit)

    def get_telemetry_event(self, query_id: str) -> dict[str, Any] | None:
        return self._delegate.get_telemetry_event(query_id)

    def get_telemetry_dashboard(self) -> dict[str, Any]:
        return self._delegate.get_telemetry_dashboard()

    def list_telemetry_history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._delegate.list_telemetry_history(limit=limit)
