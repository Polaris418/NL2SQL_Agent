from functools import lru_cache
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="Text-to-SQL Agent")
    app_version: str = Field(default="0.1.0")
    api_prefix: str = Field(default="/api")
    debug: bool = Field(default=False)
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    metadata_db_url: str = Field(default="sqlite:///./metadata.db")
    chroma_persist_directory: str = Field(default="./chroma", alias="CHROMA_PERSIST_DIRECTORY")
    rag_embedding_provider: str = Field(default="local", alias="RAG_EMBEDDING_PROVIDER")
    rag_embedding_model: str = Field(
        default="paraphrase-multilingual-MiniLM-L12-v2",
        alias="RAG_EMBEDDING_MODEL",
    )
    rag_embedding_timeout_seconds: float = Field(default=2.0, ge=0.1, le=30.0, alias="RAG_EMBEDDING_TIMEOUT_SECONDS")
    rag_reranker_enabled: bool = Field(default=True, alias="RAG_RERANKER_ENABLED")
    rag_reranker_model: str = Field(
        default="cross-encoder/ms-marco-MiniLM-L-6-v2",
        alias="RAG_RERANKER_MODEL",
    )
    rag_reranker_timeout_seconds: float = Field(default=1.0, ge=0.1, le=10.0, alias="RAG_RERANKER_TIMEOUT_SECONDS")
    rag_bm25_weight: float = Field(default=0.45, ge=0.0, le=1.0, alias="RAG_BM25_WEIGHT")
    rag_vector_weight: float = Field(default=0.55, ge=0.0, le=1.0, alias="RAG_VECTOR_WEIGHT")
    rag_synonym_dict_path: str = Field(default="./config/synonyms.json", alias="RAG_SYNONYM_DICT_PATH")
    rag_cache_enabled: bool = Field(default=True, alias="RAG_CACHE_ENABLED")
    rag_cache_ttl_seconds: int = Field(default=1800, ge=60, le=86400, alias="RAG_CACHE_TTL_SECONDS")
    rag_cache_max_entries: int = Field(default=1000, ge=10, le=10000, alias="RAG_CACHE_MAX_ENTRIES")
    rag_shard_threshold: int = Field(default=100, ge=8, le=10000, alias="RAG_SHARD_THRESHOLD")
    rag_degradation_recovery_threshold: int = Field(default=1, ge=1, le=10, alias="RAG_DEGRADATION_RECOVERY_THRESHOLD")
    rag_max_concurrent_requests: int = Field(default=8, ge=1, le=64, alias="RAG_MAX_CONCURRENT_REQUESTS")
    rag_queue_timeout_seconds: float = Field(default=1.0, ge=0.1, le=30.0, alias="RAG_QUEUE_TIMEOUT_SECONDS")
    rag_retrieval_timeout_seconds: float = Field(default=8.0, ge=0.1, le=60.0, alias="RAG_RETRIEVAL_TIMEOUT_SECONDS")
    rag_context_max_chars: int = Field(default=7000, ge=512, le=50000, alias="RAG_CONTEXT_MAX_CHARS")
    rag_context_max_tokens: int = Field(default=1800, ge=64, le=50000, alias="RAG_CONTEXT_MAX_TOKENS")
    rag_context_max_tables: int = Field(default=8, ge=1, le=64, alias="RAG_CONTEXT_MAX_TABLES")
    rag_context_max_columns_per_table: int = Field(default=6, ge=1, le=64, alias="RAG_CONTEXT_MAX_COLUMNS_PER_TABLE")
    rag_context_max_relationship_clues: int = Field(default=6, ge=0, le=64, alias="RAG_CONTEXT_MAX_RELATIONSHIP_CLUES")
    rag_telemetry_persist_path: str = Field(default="./rag_telemetry.jsonl", alias="RAG_TELEMETRY_PERSIST_PATH")
    rag_telemetry_snapshot_interval: int = Field(default=25, ge=1, le=1000, alias="RAG_TELEMETRY_SNAPSHOT_INTERVAL")
    rag_telemetry_max_events: int = Field(default=5000, ge=100, le=50000, alias="RAG_TELEMETRY_MAX_EVENTS")
    default_query_limit: int = Field(default=1000, ge=1, le=1000)
    max_concurrent_queries: int = Field(default=5, ge=1, le=20)
    query_timeout_seconds: int = Field(default=30, ge=1, le=300)
    query_cache_ttl_seconds: int = Field(default=3600, ge=60, le=86400)
    llm_provider: str = Field(default="openai")
    llm_model: str = Field(default="gpt-4.1-mini")
    llm_api_key: str | None = Field(default=None)
    llm_base_url: str | None = Field(default=None)
    openai_api_key: str | None = Field(default=None)
    anthropic_api_key: str | None = Field(default=None)

    @field_validator("api_prefix")
    @classmethod
    def _normalize_api_prefix(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith("/"):
            value = f"/{value}"
        return value.rstrip("/") or "/"

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _parse_cors_origins(cls, value: Any) -> list[str]:
        if value is None or value == "":
            return ["*"]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return ["*"]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
