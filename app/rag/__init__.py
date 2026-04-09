"""RAG (Retrieval-Augmented Generation) module for production-grade schema retrieval."""

from app.rag.access_control import AccessControlPolicy, AccessDecision, AccessEffect, AccessResource, AccessRule, AccessRuleRepository, AccessScope
from app.rag.async_indexing import AsyncIndexingManager, IndexBuildArtifact
from app.rag.business_knowledge import BusinessKnowledgeItem, BusinessKnowledgeRepository
from app.rag.cache import RetrievalCache, RetrievalCacheKey
from app.rag.column_retriever import ColumnLevelRetriever
from app.rag.context_limiter import SchemaContextLimiter
from app.rag.concurrency import RetrievalConcurrencyController, RetrievalConcurrencyError, RetrievalTimeoutError
from app.rag.degradation import DegradationEvent, DegradationManager, DegradationState
from app.rag.debug_view import build_debug_view_from_manager, build_query_debug_view
from app.rag.embedding import (
    DeterministicHashEmbedding,
    EmbeddingConfig,
    EmbeddingError,
    EmbeddingModel,
    EmbeddingTimeoutError,
    SentenceTransformerEmbedding,
    create_embedding_model,
)
from app.rag.failure_classifier import FailureClassification, FailureClassifier
from app.rag.fewshot_integration import FewShotExample, FewShotIntegration, FewShotRegistry, FewShotScopeMatcher
from app.rag.fusion import FusedCandidate, FusionStrategy, ReciprocalRankFusion, RetrievalCandidate, WeightedFusion
from app.rag.input_validation import DEFAULT_INPUT_VALIDATOR, InputValidationConfig, InputValidationError, InputValidator, ValidationIssue, ValidationResult, validate_input
from app.rag.metadata_filter import MetadataFilter
from app.rag.multi_tenant import MultiTenantFilter, MultiTenantIsolationManager, TenantScope
from app.rag.orchestrator import RetrievalOrchestrator, RetrievalResult, RetrievalTelemetry
from app.rag.profiling import ColumnProfile, ProfilingSnapshot, ProfilingStore, TableProfile
from app.rag.query_rewriter import QueryRewriter, RewriteResult
from app.rag.reranker import CrossEncoderReranker, RerankCandidate, RerankerConfig
from app.rag.relationship_retriever import RelationshipAwareRetriever, RelationshipEdge, RelationshipExpansionResult
from app.rag.schema_doc import FieldDocumentation, JoinPath, SchemaDocumentationManager, TableDocumentation
from app.rag.schema_retriever import SchemaRetriever
from app.rag.sensitive_fields import (
    DEFAULT_FIELD_ACCESS_POLICY,
    DEFAULT_SENSITIVE_POLICY,
    DEFAULT_SENSITIVE_SANITIZER,
    FieldAccessPolicy,
    SensitiveFieldFinding,
    SensitiveFieldPolicy,
    SensitiveFieldSanitizer,
    sanitize_schema_context,
    sanitize_table_documentation,
)
from app.rag.sharding import SchemaShardPlanner, ShardBucket
from app.rag.synonym_dict import SynonymDictionary
from app.rag.telemetry import ContextLimitTelemetryEvent, RetrievalTelemetryEvent, TelemetrySystem
from app.rag.telemetry_store import TelemetryEventRecord, TelemetryStore
from app.rag.vector_store import (
    ChromaVectorStore,
    HealthStatus,
    InMemoryVectorStore,
    VectorSearchResult,
    VectorStore,
    VectorStoreConfig,
    VectorStoreError,
    VectorStoreTimeoutError,
    VectorStoreUnavailableError,
    create_vector_store,
)
from app.schemas.rag import (
    RAGDebugArtifacts,
    RAGDebugCandidateScore,
    RAGDebugQueryContext,
    RAGDebugTiming,
    RAGQueryDebugView,
    RAGTelemetryEventRecord,
    RAGTelemetrySummary,
)

__all__ = [
    "AccessControlPolicy",
    "AccessDecision",
    "AccessEffect",
    "AccessResource",
    "AccessRule",
    "AccessRuleRepository",
    "AccessScope",
    "AsyncIndexingManager",
    "BusinessKnowledgeItem",
    "BusinessKnowledgeRepository",
    "ChromaVectorStore",
    "ColumnLevelRetriever",
    "ColumnProfile",
    "CrossEncoderReranker",
    "DEFAULT_FIELD_ACCESS_POLICY",
    "DEFAULT_INPUT_VALIDATOR",
    "DEFAULT_SENSITIVE_POLICY",
    "DEFAULT_SENSITIVE_SANITIZER",
    "DegradationEvent",
    "DegradationManager",
    "DegradationState",
    "DeterministicHashEmbedding",
    "EmbeddingConfig",
    "EmbeddingError",
    "EmbeddingModel",
    "EmbeddingTimeoutError",
    "FailureClassification",
    "FailureClassifier",
    "FieldAccessPolicy",
    "FieldDocumentation",
    "FewShotExample",
    "FewShotIntegration",
    "FewShotRegistry",
    "FewShotScopeMatcher",
    "FusedCandidate",
    "FusionStrategy",
    "HealthStatus",
    "IndexBuildArtifact",
    "InMemoryVectorStore",
    "InputValidationConfig",
    "InputValidationError",
    "InputValidator",
    "JoinPath",
    "MetadataFilter",
    "MultiTenantFilter",
    "MultiTenantIsolationManager",
    "ProfilingSnapshot",
    "ProfilingStore",
    "QueryRewriter",
    "RAGDebugArtifacts",
    "RAGDebugCandidateScore",
    "RAGDebugQueryContext",
    "RAGDebugTiming",
    "RAGQueryDebugView",
    "ContextLimitTelemetryEvent",
    "RAGTelemetryEventRecord",
    "RAGTelemetrySummary",
    "ReciprocalRankFusion",
    "RetrievalCache",
    "RetrievalCacheKey",
    "RetrievalCandidate",
    "RetrievalConcurrencyController",
    "RetrievalConcurrencyError",
    "RetrievalOrchestrator",
    "RetrievalResult",
    "RetrievalTelemetry",
    "RetrievalTelemetryEvent",
    "RetrievalTimeoutError",
    "RelationshipAwareRetriever",
    "RelationshipEdge",
    "RelationshipExpansionResult",
    "RewriteResult",
    "RerankCandidate",
    "RerankerConfig",
    "SchemaDocumentationManager",
    "SchemaContextLimiter",
    "SchemaRetriever",
    "SchemaShardPlanner",
    "ShardBucket",
    "SensitiveFieldFinding",
    "SensitiveFieldPolicy",
    "SensitiveFieldSanitizer",
    "SentenceTransformerEmbedding",
    "TableDocumentation",
    "TableProfile",
    "TelemetryEventRecord",
    "TelemetryStore",
    "TelemetrySystem",
    "TenantScope",
    "ValidationIssue",
    "ValidationResult",
    "VectorSearchResult",
    "VectorStore",
    "VectorStoreConfig",
    "VectorStoreError",
    "VectorStoreTimeoutError",
    "VectorStoreUnavailableError",
    "WeightedFusion",
    "build_debug_view_from_manager",
    "build_query_debug_view",
    "create_embedding_model",
    "create_vector_store",
    "sanitize_schema_context",
    "sanitize_table_documentation",
    "validate_input",
]
