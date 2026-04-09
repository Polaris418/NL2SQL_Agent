from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.agent.chart_suggester import ChartSuggester
from app.agent.error_reflector import ErrorReflector
from app.agent.nl2sql_agent import NL2SQLAgent
from app.agent.query_rewriter import QueryRewriter
from app.agent.result_summarizer import ResultSummarizer
from app.agent.schema_retriever import SchemaRetriever
from app.agent.sql_executor import SQLExecutor
from app.agent.sql_generator import SQLGenerator
from app.api.ai_assistant import router as ai_assistant_router
from app.api.analytics import router as analytics_router
from app.api.connections import router as connections_router
from app.api.documents import router as documents_router
from app.api.health import router as health_router
from app.api.history import router as history_router
from app.api.prompts import router as prompts_router
from app.api.rag_telemetry import router as rag_telemetry_router
from app.api.query import router as query_router
from app.api.settings import router as settings_router
from app.core.config import Settings, get_settings
from app.core.rag_index_manager import RAGIndexManager
from app.db.manager import DBManager
from app.db.metadata import MetadataDB
from app.db.repositories.llm_repo import LLMSettingsRepository
from app.db.repositories.rag_query_log_repo import RAGQueryLogRepository
from app.db.repositories.rag_telemetry_repo import RAGTelemetryRepository
from app.llm.client import LLMClient
from app.agent.nl2sql_agent import TooManyRequestsError
from app.prompts.few_shot_examples import FEW_SHOT_EXAMPLES
from app.rag.business_knowledge import BusinessKnowledgeRepository
from app.rag.document_rag import DocumentRAG, DocumentMetadataStore
from app.rag.embedding import EmbeddingConfig
from app.rag.fewshot_integration import FewShotIntegration
from app.rag.multi_tenant import MultiTenantIsolationManager
from app.rag.profiling import ProfilingStore
from app.rag.reranker import RerankerConfig
from app.rag.telemetry import TelemetrySystem
from app.rag.vector_store import VectorStoreConfig


LOGGER = logging.getLogger("text_to_sql_agent")


@dataclass
class ServiceContainer:
    settings: Settings
    metadata_db: MetadataDB
    db_manager: DBManager
    rag_index_manager: RAGIndexManager
    llm_client: LLMClient
    agent: NL2SQLAgent
    document_rag: DocumentRAG | None = None
    document_metadata_store: DocumentMetadataStore | None = None


@lru_cache(maxsize=1)
def get_container() -> ServiceContainer:
    settings = get_settings()
    metadata_db = MetadataDB()
    metadata_db.initialize()
    db_manager = DBManager(metadata_db, settings)
    llm_client = LLMClient(settings)
    profiling_store = ProfilingStore()
    business_knowledge_repository = BusinessKnowledgeRepository()
    tenant_manager = MultiTenantIsolationManager()
    few_shot_integration = FewShotIntegration(tenant_manager=tenant_manager)
    for db_type, items in FEW_SHOT_EXAMPLES.items():
        few_shot_integration.register_many(
            {
                "id": f"default-{db_type}-{index + 1}",
                "question": item["question"],
                "sql": item["sql"],
                "db_types": [db_type],
                "priority": 1,
            }
            for index, item in enumerate(items)
        )
    few_shot_integration.register_many([
        {
            "id": "polaris-user-activity-1",
            "question": "????7??????",
            "sql": "SELECT COUNT(DISTINCT tu.user_id) AS active_user_count FROM t_tool_usage tu WHERE tu.used_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)",
            "connection_ids": ["metadata_localmysql"],
            "db_types": ["mysql"],
            "business_domains": ["user", "tool"],
            "priority": 10,
            "metadata": {"connection_name": "localMysql", "database": "polaris"},
        },
        {
            "id": "polaris-user-activity-2",
            "question": "????7???????",
            "sql": "SELECT u.id, u.username, COUNT(*) AS usage_count FROM t_tool_usage tu JOIN t_user u ON tu.user_id = u.id WHERE tu.used_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) GROUP BY u.id, u.username ORDER BY usage_count DESC LIMIT 10",
            "connection_ids": ["metadata_localmysql"],
            "db_types": ["mysql"],
            "business_domains": ["user", "tool"],
            "priority": 12,
            "metadata": {"connection_name": "localMysql", "database": "polaris"},
        },
        {
            "id": "polaris-tool-ranking-1",
            "question": "??30??????????",
            "sql": "SELECT t.id, t.name_zh, COUNT(*) AS usage_count FROM t_tool_usage tu JOIN t_tool t ON tu.tool_id = t.id WHERE tu.used_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY t.id, t.name_zh ORDER BY usage_count DESC LIMIT 10",
            "connection_ids": ["metadata_localmysql"],
            "db_types": ["mysql"],
            "business_domains": ["tool"],
            "priority": 9,
            "metadata": {"connection_name": "localMysql", "database": "polaris"},
        },
        {
            "id": "polaris-notification-1",
            "question": "??30??????????",
            "sql": "SELECT DATE(created_at) AS stat_date, COUNT(*) AS notification_count, ROUND(SUM(CASE WHEN is_read = 1 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS read_rate FROM t_notification WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY DATE(created_at) ORDER BY stat_date",
            "connection_ids": ["metadata_localmysql"],
            "db_types": ["mysql"],
            "business_domains": ["user"],
            "priority": 8,
            "metadata": {"connection_name": "localMysql", "database": "polaris"},
        },
    ])
    llm_repo = LLMSettingsRepository(metadata_db)
    telemetry_repository = RAGTelemetryRepository(metadata_db)
    query_log_repository = RAGQueryLogRepository(metadata_db)
    telemetry_system = TelemetrySystem(
        max_events=settings.rag_telemetry_max_events,
        snapshot_interval=settings.rag_telemetry_snapshot_interval,
        repository=telemetry_repository,
        persist_path=settings.rag_telemetry_persist_path,
    )
    profiles = llm_repo.list_profiles()
    routing = llm_repo.get_routing()
    if profiles:
        llm_client.configure_profiles(
            profiles=[
                {
                    "id": profile.id,
                    "provider": profile.provider,
                    "model": profile.model,
                    "api_key": profile.api_key,
                    "base_url": profile.base_url,
                }
                for profile in profiles
            ],
            primary_profile_id=routing.primary_profile_id or profiles[0].id,
            fallback_profile_id=routing.fallback_profile_id,
        )

    schema_retriever = SchemaRetriever(
        settings.chroma_persist_directory,
        embedding_config=EmbeddingConfig(
            provider=settings.rag_embedding_provider,
            model_name=settings.rag_embedding_model,
            timeout=settings.rag_embedding_timeout_seconds,
        ),
        vector_store_config=VectorStoreConfig(persist_directory=settings.chroma_persist_directory),
        reranker_config=RerankerConfig(
            enabled=settings.rag_reranker_enabled,
            model_name=settings.rag_reranker_model,
            timeout_seconds=settings.rag_reranker_timeout_seconds,
        ),
        synonym_path=settings.rag_synonym_dict_path,
        cache_ttl_seconds=settings.rag_cache_ttl_seconds,
        cache_max_entries=settings.rag_cache_max_entries,
        cache_enabled=settings.rag_cache_enabled,
        shard_threshold=settings.rag_shard_threshold,
        degradation_recovery_threshold=settings.rag_degradation_recovery_threshold,
        max_concurrent_requests=settings.rag_max_concurrent_requests,
        queue_timeout_seconds=settings.rag_queue_timeout_seconds,
        retrieval_timeout_seconds=settings.rag_retrieval_timeout_seconds,
        profiling_store=profiling_store,
        business_knowledge_repository=business_knowledge_repository,
        tenant_manager=tenant_manager,
        telemetry_repository=telemetry_repository,
        telemetry_system=telemetry_system,
        query_log_repository=query_log_repository,
    )
    rag_index_manager = RAGIndexManager(metadata_db, db_manager, schema_retriever=schema_retriever)
    
    # 初始化文档 RAG 系统
    document_metadata_store = None
    document_rag = None
    try:
        from app.rag.vector_store import ChromaVectorStore
        from app.rag.embedding import DeterministicHashEmbedding, SentenceTransformerEmbedding
        
        # 尝试使用真实的 embedding 模型，失败则使用 fallback
        try:
            document_embedding_config = EmbeddingConfig(
                model_name=settings.rag_embedding_model,
                provider="local",
                timeout=max(15.0, settings.rag_embedding_timeout_seconds),
            )
            document_embedding_model = SentenceTransformerEmbedding(config=document_embedding_config)
            LOGGER.info("Using SentenceTransformer embedding for documents")
        except Exception as e:
            LOGGER.warning(f"Failed to load SentenceTransformer, using fallback: {e}")
            document_embedding_model = DeterministicHashEmbedding()
        
        # 初始化文档向量存储
        document_vector_store = ChromaVectorStore(
            config=VectorStoreConfig(
                persist_directory="./chroma_documents",
                timeout=5.0
            ),
            collection_name="assistant_knowledge"
        )
        
        # 异步初始化（在后台）
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # 如果事件循环正在运行，创建任务
            asyncio.create_task(document_vector_store.initialize())
        else:
            # 否则同步初始化
            loop.run_until_complete(document_vector_store.initialize())
        
        # 初始化文档元数据存储
        document_metadata_store = DocumentMetadataStore(storage_path="./data/documents")
        
        # 初始化文档 RAG
        document_rag = DocumentRAG(
            embedding_model=document_embedding_model,
            vector_store=document_vector_store,
            chunk_size=500,
            chunk_overlap=50
        )
        
        LOGGER.info("Document RAG system initialized successfully")
    except Exception as e:
        LOGGER.warning(f"Failed to initialize document RAG system: {e}")
    
    agent = NL2SQLAgent(
        db_manager=db_manager,
        query_rewriter=QueryRewriter(llm_client),
        schema_retriever=schema_retriever,
        sql_generator=SQLGenerator(
            llm_client,
            default_limit=settings.default_query_limit,
            few_shot_integration=few_shot_integration,
            context_max_tables=settings.rag_context_max_tables,
            context_max_columns_per_table=settings.rag_context_max_columns_per_table,
            context_max_relationship_clues=settings.rag_context_max_relationship_clues,
            context_max_chars=settings.rag_context_max_chars,
            context_max_tokens=settings.rag_context_max_tokens,
            telemetry_system=telemetry_system,
        ),
        sql_executor=SQLExecutor(default_limit=settings.default_query_limit),
        error_reflector=ErrorReflector(llm_client),
        chart_suggester=ChartSuggester(),
        result_summarizer=ResultSummarizer(llm_client),
    )
    return ServiceContainer(
        settings=settings,
        metadata_db=metadata_db,
        db_manager=db_manager,
        rag_index_manager=rag_index_manager,
        llm_client=llm_client,
        agent=agent,
        document_rag=document_rag,
        document_metadata_store=document_metadata_store,
    )


def create_app() -> FastAPI:
    settings = get_settings()
    logging.basicConfig(level=logging.INFO)
    app = FastAPI(title=settings.app_name, version=settings.app_version, debug=settings.debug)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health_router)
    app.include_router(connections_router, prefix=settings.api_prefix)
    app.include_router(query_router, prefix=settings.api_prefix)
    app.include_router(history_router, prefix=settings.api_prefix)
    app.include_router(analytics_router, prefix=settings.api_prefix)
    app.include_router(rag_telemetry_router, prefix=settings.api_prefix)
    app.include_router(settings_router, prefix=settings.api_prefix)
    app.include_router(prompts_router, prefix=settings.api_prefix)
    app.include_router(ai_assistant_router, prefix=f"{settings.api_prefix}/assistant")
    app.include_router(documents_router, prefix=f"{settings.api_prefix}/documents")

    @app.exception_handler(TooManyRequestsError)
    async def handle_too_many_requests(_request: Request, exc: TooManyRequestsError):
        return JSONResponse(status_code=429, content={"message": str(exc), "error_type": "rate_limit"})

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(_request: Request, exc: Exception):
        LOGGER.exception("Unhandled exception", exc_info=exc)
        return JSONResponse(status_code=500, content={"message": "Internal server error", "error_type": "internal_error"})

    return app
