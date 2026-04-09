from __future__ import annotations

from typing import Any

from app.db.repositories.base import RepositoryBase


class RAGQueryLogRepository(RepositoryBase):
    def upsert(
        self,
        *,
        query_id: str,
        connection_id: str,
        original_query: str,
        rewritten_query: str | None,
        expanded_query: str | None,
        selected_tables: list[str],
        candidate_scores: list[dict[str, Any]],
        reranked_tables: list[str],
        prompt_schema: str | None,
        final_sql: str | None,
        cache_hit: bool,
        used_fallback: bool,
        degradation_mode: str | None,
        failure_category: str | None,
        failure_stage: str | None,
        retrieval_latency_ms: float | None,
        stage_latencies: dict[str, float],
        error_message: str | None,
        created_at: str,
    ) -> None:
        now = self.utcnow()
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO rag_query_logs (
                    query_id, connection_id, original_query, rewritten_query, expanded_query,
                    selected_tables_json, candidate_scores_json, reranked_tables_json, prompt_schema,
                    final_sql, cache_hit, used_fallback, degradation_mode, failure_category,
                    failure_stage, retrieval_latency_ms, stage_latencies_json, error_message,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(query_id) DO UPDATE SET
                    connection_id = excluded.connection_id,
                    original_query = excluded.original_query,
                    rewritten_query = excluded.rewritten_query,
                    expanded_query = excluded.expanded_query,
                    selected_tables_json = excluded.selected_tables_json,
                    candidate_scores_json = excluded.candidate_scores_json,
                    reranked_tables_json = excluded.reranked_tables_json,
                    prompt_schema = COALESCE(excluded.prompt_schema, rag_query_logs.prompt_schema),
                    final_sql = COALESCE(excluded.final_sql, rag_query_logs.final_sql),
                    cache_hit = excluded.cache_hit,
                    used_fallback = excluded.used_fallback,
                    degradation_mode = excluded.degradation_mode,
                    failure_category = excluded.failure_category,
                    failure_stage = excluded.failure_stage,
                    retrieval_latency_ms = excluded.retrieval_latency_ms,
                    stage_latencies_json = excluded.stage_latencies_json,
                    error_message = excluded.error_message,
                    updated_at = excluded.updated_at
                """,
                (
                    query_id,
                    connection_id,
                    original_query,
                    rewritten_query,
                    expanded_query,
                    self.dumps(selected_tables or []),
                    self.dumps(candidate_scores or []),
                    self.dumps(reranked_tables or []),
                    prompt_schema,
                    final_sql,
                    int(bool(cache_hit)),
                    int(bool(used_fallback)),
                    degradation_mode,
                    failure_category,
                    failure_stage,
                    float(retrieval_latency_ms or 0.0) if retrieval_latency_ms is not None else None,
                    self.dumps(stage_latencies or {}),
                    error_message,
                    created_at,
                    now,
                ),
            )

    def attach_generation(
        self,
        query_id: str,
        *,
        prompt_schema: str | None = None,
        final_sql: str | None = None,
    ) -> None:
        if not prompt_schema and not final_sql:
            return
        fields: list[str] = []
        params: list[Any] = []
        if prompt_schema is not None:
            fields.append("prompt_schema = ?")
            params.append(prompt_schema)
        if final_sql is not None:
            fields.append("final_sql = ?")
            params.append(final_sql)
        fields.append("updated_at = ?")
        params.append(self.utcnow())
        params.append(query_id)
        with self.metadata_db.connect() as conn:
            conn.execute(
                f"UPDATE rag_query_logs SET {', '.join(fields)} WHERE query_id = ?",
                tuple(params),
            )

    def get(self, query_id: str) -> dict[str, Any] | None:
        row = self.fetch_one("SELECT * FROM rag_query_logs WHERE query_id = ?", (query_id,))
        return self._row_to_payload(row) if row else None

    def list(
        self,
        *,
        connection_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if connection_id:
            rows = self.fetch_all(
                "SELECT * FROM rag_query_logs WHERE connection_id = ? ORDER BY created_at DESC, updated_at DESC LIMIT ?",
                (connection_id, max(1, int(limit))),
            )
        else:
            rows = self.fetch_all(
                "SELECT * FROM rag_query_logs ORDER BY created_at DESC, updated_at DESC LIMIT ?",
                (max(1, int(limit)),),
            )
        return [self._row_to_payload(row) for row in rows]

    @staticmethod
    def _row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "query_id": row["query_id"],
            "connection_id": row["connection_id"],
            "original_query": row["original_query"],
            "rewritten_query": row.get("rewritten_query"),
            "expanded_query": row.get("expanded_query"),
            "selected_tables": RepositoryBase.loads(row.get("selected_tables_json"), []),
            "candidate_scores": RepositoryBase.loads(row.get("candidate_scores_json"), []),
            "reranked_tables": RepositoryBase.loads(row.get("reranked_tables_json"), []),
            "prompt_schema": row.get("prompt_schema"),
            "final_sql": row.get("final_sql"),
            "cache_hit": bool(row.get("cache_hit")),
            "used_fallback": bool(row.get("used_fallback")),
            "degradation_mode": row.get("degradation_mode"),
            "failure_category": row.get("failure_category"),
            "failure_stage": row.get("failure_stage"),
            "retrieval_latency_ms": row.get("retrieval_latency_ms"),
            "stage_latencies": RepositoryBase.loads(row.get("stage_latencies_json"), {}),
            "error_message": row.get("error_message"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }
