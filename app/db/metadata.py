from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class MetadataDB:
    def __init__(self, database_path: str | None = None):
        self.database_path = database_path or os.getenv("NL2SQL_METADATA_DB", "./metadata.sqlite3")
        self.path = Path(self.database_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialized = False

    def initialize(self) -> None:
        if self._initialized:
            return
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS db_connections (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    db_type TEXT NOT NULL,
                    host TEXT,
                    port INTEGER,
                    username TEXT,
                    password_encrypted TEXT,
                    database_name TEXT NOT NULL,
                    file_path TEXT,
                    is_online INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    schema_updated_at TEXT
                );

                CREATE TABLE IF NOT EXISTS schema_cache (
                    connection_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS query_history (
                    id TEXT PRIMARY KEY,
                    connection_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    rewritten_query TEXT,
                    retrieved_tables_json TEXT NOT NULL,
                    retrieved_table_details_json TEXT,
                    sql_text TEXT,
                    result_json TEXT,
                    chart_json TEXT,
                    telemetry_json TEXT,
                    result_row_count INTEGER,
                    status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    llm_latency_ms REAL,
                    db_latency_ms REAL,
                    error_message TEXT,
                    error_type TEXT,
                    error_suggestion TEXT,
                    context_source_query_id TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS agent_steps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_history_id TEXT NOT NULL,
                    step_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    metadata_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS llm_settings (
                    id TEXT PRIMARY KEY,
                    display_name TEXT,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    base_url TEXT,
                    api_key_encrypted TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS llm_routing (
                    id TEXT PRIMARY KEY,
                    primary_profile_id TEXT,
                    fallback_profile_id TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rag_index_state (
                    connection_id TEXT PRIMARY KEY,
                    schema_version TEXT,
                    index_status TEXT NOT NULL DEFAULT 'pending',
                    health_status TEXT NOT NULL DEFAULT 'unknown',
                    index_mode TEXT NOT NULL DEFAULT 'hybrid',
                    is_indexed INTEGER NOT NULL DEFAULT 0,
                    table_count INTEGER NOT NULL DEFAULT 0,
                    vector_count INTEGER NOT NULL DEFAULT 0,
                    last_started_at TEXT,
                    last_completed_at TEXT,
                    last_success_at TEXT,
                    last_error TEXT,
                    last_forced_rebuild INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rag_index_jobs (
                    id TEXT PRIMARY KEY,
                    connection_id TEXT NOT NULL,
                    schema_version TEXT,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    force_full_rebuild INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    duration_ms REAL,
                    error_message TEXT,
                    payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS rag_telemetry_events (
                    id TEXT PRIMARY KEY,
                    query_id TEXT NOT NULL,
                    connection_id TEXT NOT NULL,
                    retrieval_latency_ms REAL NOT NULL DEFAULT 0,
                    embedding_latency_ms REAL NOT NULL DEFAULT 0,
                    lexical_count INTEGER NOT NULL DEFAULT 0,
                    vector_count INTEGER NOT NULL DEFAULT 0,
                    cache_hit INTEGER NOT NULL DEFAULT 0,
                    used_fallback INTEGER NOT NULL DEFAULT 0,
                    error_type TEXT,
                    failure_stage TEXT,
                    selected_tables_json TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rag_telemetry_snapshots (
                    id TEXT PRIMARY KEY,
                    logged_queries INTEGER NOT NULL DEFAULT 0,
                    metrics_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rag_telemetry_context_limits (
                    id TEXT PRIMARY KEY,
                    query_id TEXT,
                    connection_id TEXT,
                    limit_reason TEXT,
                    truncated INTEGER NOT NULL DEFAULT 0,
                    budget_json TEXT NOT NULL,
                    original_char_count INTEGER NOT NULL DEFAULT 0,
                    original_token_count INTEGER NOT NULL DEFAULT 0,
                    final_char_count INTEGER NOT NULL DEFAULT 0,
                    final_token_count INTEGER NOT NULL DEFAULT 0,
                    dropped_tables_json TEXT NOT NULL,
                    dropped_columns_json TEXT NOT NULL,
                    dropped_relationship_clues_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rag_query_logs (
                    query_id TEXT PRIMARY KEY,
                    connection_id TEXT NOT NULL,
                    original_query TEXT NOT NULL,
                    rewritten_query TEXT,
                    expanded_query TEXT,
                    selected_tables_json TEXT NOT NULL,
                    candidate_scores_json TEXT NOT NULL,
                    reranked_tables_json TEXT NOT NULL,
                    prompt_schema TEXT,
                    final_sql TEXT,
                    cache_hit INTEGER NOT NULL DEFAULT 0,
                    used_fallback INTEGER NOT NULL DEFAULT 0,
                    degradation_mode TEXT,
                    failure_category TEXT,
                    failure_stage TEXT,
                    retrieval_latency_ms REAL,
                    stage_latencies_json TEXT NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column(conn, "llm_settings", "display_name", "TEXT")
            self._ensure_column(conn, "query_history", "error_type", "TEXT")
            self._ensure_column(conn, "query_history", "error_suggestion", "TEXT")
            self._ensure_column(conn, "query_history", "result_json", "TEXT")
            self._ensure_column(conn, "query_history", "chart_json", "TEXT")
            self._ensure_column(conn, "query_history", "retrieved_table_details_json", "TEXT")
            self._ensure_column(conn, "query_history", "telemetry_json", "TEXT")
            self._ensure_column(conn, "query_history", "result_row_count", "INTEGER")
            self._ensure_column(conn, "query_history", "context_source_query_id", "TEXT")
            self._ensure_column(conn, "llm_settings", "base_url", "TEXT")
            self._ensure_column(conn, "llm_settings", "api_key_encrypted", "TEXT")
            self._ensure_column(conn, "rag_index_state", "schema_version", "TEXT")
            self._ensure_column(conn, "rag_index_state", "index_status", "TEXT")
            self._ensure_column(conn, "rag_index_state", "health_status", "TEXT")
            self._ensure_column(conn, "rag_index_state", "index_mode", "TEXT")
            self._ensure_column(conn, "rag_index_state", "is_indexed", "INTEGER")
            self._ensure_column(conn, "rag_index_state", "table_count", "INTEGER")
            self._ensure_column(conn, "rag_index_state", "vector_count", "INTEGER")
            self._ensure_column(conn, "rag_index_state", "last_started_at", "TEXT")
            self._ensure_column(conn, "rag_index_state", "last_completed_at", "TEXT")
            self._ensure_column(conn, "rag_index_state", "last_success_at", "TEXT")
            self._ensure_column(conn, "rag_index_state", "last_error", "TEXT")
            self._ensure_column(conn, "rag_index_state", "last_forced_rebuild", "INTEGER")
            self._ensure_column(conn, "rag_index_jobs", "schema_version", "TEXT")
            self._ensure_column(conn, "rag_index_jobs", "action", "TEXT")
            self._ensure_column(conn, "rag_index_jobs", "status", "TEXT")
            self._ensure_column(conn, "rag_index_jobs", "force_full_rebuild", "INTEGER")
            self._ensure_column(conn, "rag_index_jobs", "started_at", "TEXT")
            self._ensure_column(conn, "rag_index_jobs", "completed_at", "TEXT")
            self._ensure_column(conn, "rag_index_jobs", "duration_ms", "REAL")
            self._ensure_column(conn, "rag_index_jobs", "error_message", "TEXT")
            self._ensure_column(conn, "rag_index_jobs", "payload_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_events", "query_id", "TEXT")
            self._ensure_column(conn, "rag_telemetry_events", "connection_id", "TEXT")
            self._ensure_column(conn, "rag_telemetry_events", "retrieval_latency_ms", "REAL")
            self._ensure_column(conn, "rag_telemetry_events", "embedding_latency_ms", "REAL")
            self._ensure_column(conn, "rag_telemetry_events", "lexical_count", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_events", "vector_count", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_events", "cache_hit", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_events", "used_fallback", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_events", "error_type", "TEXT")
            self._ensure_column(conn, "rag_telemetry_events", "failure_stage", "TEXT")
            self._ensure_column(conn, "rag_telemetry_events", "selected_tables_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_events", "payload_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_events", "created_at", "TEXT")
            self._ensure_column(conn, "rag_telemetry_snapshots", "logged_queries", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_snapshots", "metrics_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_snapshots", "created_at", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "query_id", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "connection_id", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "limit_reason", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "truncated", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_context_limits", "budget_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "original_char_count", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_context_limits", "original_token_count", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_context_limits", "final_char_count", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_context_limits", "final_token_count", "INTEGER")
            self._ensure_column(conn, "rag_telemetry_context_limits", "dropped_tables_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "dropped_columns_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "dropped_relationship_clues_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "payload_json", "TEXT")
            self._ensure_column(conn, "rag_telemetry_context_limits", "created_at", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "query_id", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "connection_id", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "original_query", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "rewritten_query", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "expanded_query", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "selected_tables_json", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "candidate_scores_json", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "reranked_tables_json", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "prompt_schema", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "final_sql", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "cache_hit", "INTEGER")
            self._ensure_column(conn, "rag_query_logs", "used_fallback", "INTEGER")
            self._ensure_column(conn, "rag_query_logs", "degradation_mode", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "failure_category", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "failure_stage", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "retrieval_latency_ms", "REAL")
            self._ensure_column(conn, "rag_query_logs", "stage_latencies_json", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "error_message", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "created_at", "TEXT")
            self._ensure_column(conn, "rag_query_logs", "updated_at", "TEXT")
        self._initialized = True

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path.as_posix())
        conn.row_factory = sqlite3.Row
        # 确保使用 UTF-8 编码
        conn.execute("PRAGMA encoding = 'UTF-8'")
        conn.text_factory = str
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
        existing = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
