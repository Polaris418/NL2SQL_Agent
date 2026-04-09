from __future__ import annotations

from collections import Counter
from uuid import uuid4
from typing import Any

from app.db.repositories.base import RepositoryBase


class RAGTelemetryRepository(RepositoryBase):
    def add_event(
        self,
        *,
        query_id: str,
        connection_id: str,
        retrieval_latency_ms: float,
        embedding_latency_ms: float,
        lexical_count: int,
        vector_count: int,
        cache_hit: bool,
        used_fallback: bool,
        error_type: str | None,
        failure_stage: str | None,
        selected_tables: list[str],
        payload: dict,
    ) -> None:
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO rag_telemetry_events (
                    id, query_id, connection_id, retrieval_latency_ms, embedding_latency_ms,
                    lexical_count, vector_count, cache_hit, used_fallback, error_type,
                    failure_stage, selected_tables_json, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"rag_evt_{uuid4().hex[:16]}",
                    query_id,
                    connection_id,
                    float(retrieval_latency_ms or 0.0),
                    float(embedding_latency_ms or 0.0),
                    int(lexical_count or 0),
                    int(vector_count or 0),
                    int(bool(cache_hit)),
                    int(bool(used_fallback)),
                    error_type,
                    failure_stage,
                    self.dumps(selected_tables or []),
                    self.dumps(payload or {}),
                    self.utcnow(),
                ),
            )

    def add_snapshot(self, metrics: dict[str, Any]) -> None:
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO rag_telemetry_snapshots (id, logged_queries, metrics_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    f"rag_ts_{uuid4().hex[:16]}",
                    int(metrics.get("logged_queries", 0) or 0),
                    self.dumps(metrics),
                    self.utcnow(),
                ),
            )

    def add_context_limit(
        self,
        *,
        query_id: str | None,
        connection_id: str | None,
        limit_reason: str | None,
        truncated: bool,
        budget: dict[str, Any],
        original_char_count: int,
        original_token_count: int,
        final_char_count: int,
        final_token_count: int,
        dropped_tables: list[str],
        dropped_columns: dict[str, list[str]],
        dropped_relationship_clues: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> None:
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO rag_telemetry_context_limits (
                    id, query_id, connection_id, limit_reason, truncated, budget_json,
                    original_char_count, original_token_count, final_char_count, final_token_count,
                    dropped_tables_json, dropped_columns_json, dropped_relationship_clues_json,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"rag_ctl_{uuid4().hex[:16]}",
                    query_id,
                    connection_id,
                    limit_reason,
                    int(bool(truncated)),
                    self.dumps(budget or {}),
                    int(original_char_count or 0),
                    int(original_token_count or 0),
                    int(final_char_count or 0),
                    int(final_token_count or 0),
                    self.dumps(dropped_tables or []),
                    self.dumps(dropped_columns or {}),
                    self.dumps(dropped_relationship_clues or []),
                    self.dumps(payload or {}),
                    self.utcnow(),
                ),
            )

    def list_events(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM rag_telemetry_events"
        clauses: list[str] = []
        params: list[Any] = []
        if connection_id:
            clauses.append("connection_id = ?")
            params.append(connection_id)
        if query_id:
            clauses.append("query_id = ?")
            params.append(query_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"
        rows = self.fetch_all(sql, tuple(params + [max(1, int(limit))]))
        return [
            {
                **row,
                "cache_hit": bool(row.get("cache_hit")),
                "used_fallback": bool(row.get("used_fallback")),
                "selected_tables": self.loads(row.get("selected_tables_json"), []),
                "payload": self.loads(row.get("payload_json"), {}),
            }
            for row in rows
        ]

    def latest_snapshot(self) -> dict[str, Any] | None:
        row = self.fetch_one("SELECT * FROM rag_telemetry_snapshots ORDER BY created_at DESC, id DESC LIMIT 1")
        if row is None:
            return None
        return {
            **row,
            "metrics": self.loads(row.get("metrics_json"), {}),
        }

    def list_snapshots(self, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.fetch_all(
            "SELECT * FROM rag_telemetry_snapshots ORDER BY created_at DESC, id DESC LIMIT ?",
            (max(1, int(limit)),),
        )
        return [
            {
                **row,
                "metrics": self.loads(row.get("metrics_json"), {}),
            }
            for row in rows
        ]

    def list_context_limits(
        self,
        *,
        connection_id: str | None = None,
        query_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM rag_telemetry_context_limits"
        clauses: list[str] = []
        params: list[Any] = []
        if connection_id:
            clauses.append("connection_id = ?")
            params.append(connection_id)
        if query_id:
            clauses.append("query_id = ?")
            params.append(query_id)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        rows = self.fetch_all(sql, tuple(params + [max(1, int(limit))]))
        return [
            {
                **row,
                "truncated": bool(row.get("truncated")),
                "budget": self.loads(row.get("budget_json"), {}),
                "dropped_tables": self.loads(row.get("dropped_tables_json"), []),
                "dropped_columns": self.loads(row.get("dropped_columns_json"), {}),
                "dropped_relationship_clues": self.loads(row.get("dropped_relationship_clues_json"), []),
                "payload": self.loads(row.get("payload_json"), {}),
            }
            for row in rows
        ]

    def get_metrics(self) -> dict[str, Any]:
        events = self.list_events(limit=100000)
        context_limits = self.list_context_limits(limit=100000)
        if not events and not context_limits:
            latest_snapshot = self.latest_snapshot()
            return dict(latest_snapshot.get("metrics", {})) if latest_snapshot else {
                "logged_queries": 0,
                "retrieval_p50_ms": 0.0,
                "retrieval_p95_ms": 0.0,
                "retrieval_p99_ms": 0.0,
                "embedding_p50_ms": 0.0,
                "embedding_p95_ms": 0.0,
                "embedding_p99_ms": 0.0,
                "vector_hit_rate": 0.0,
                "bm25_hit_rate": 0.0,
                "fallback_rate": 0.0,
                "table_not_found_rate": 0.0,
                "timeout_rate": 0.0,
                "concurrency_rejection_rate": 0.0,
                "failure_categories": {},
                "context_limit_events": 0,
                "context_limit_rate": 0.0,
                "last_snapshot_at": None,
            }

        retrieval_latencies = [max(0.0, float(event.get("retrieval_latency_ms") or 0.0)) for event in events]
        embedding_latencies = [max(0.0, float(event.get("embedding_latency_ms") or 0.0)) for event in events]
        total = max(1, len(events))
        failure_categories = Counter()
        for event in events:
            if event.get("error_type"):
                failure_categories[str(event["error_type"])] += 1
        for item in context_limits:
            if item.get("limit_reason"):
                failure_categories[f"context_limit:{item['limit_reason']}"] += 1
        table_not_found = failure_categories.get("table_not_found", 0) + failure_categories.get("database_mismatch", 0)
        timeout_count = sum(
            failure_categories.get(key, 0)
            for key in ("timeout_error", "retrieval_timeout", "embedding_timeout", "reranker_timeout")
        )
        context_limit_count = len(context_limits)
        latest_snapshot = self.latest_snapshot()
        return {
            "logged_queries": len(events),
            "retrieval_p50_ms": self._percentile(retrieval_latencies, 50),
            "retrieval_p95_ms": self._percentile(retrieval_latencies, 95),
            "retrieval_p99_ms": self._percentile(retrieval_latencies, 99),
            "embedding_p50_ms": self._percentile(embedding_latencies, 50),
            "embedding_p95_ms": self._percentile(embedding_latencies, 95),
            "embedding_p99_ms": self._percentile(embedding_latencies, 99),
            "vector_hit_rate": round(sum(1 for event in events if int(event.get("vector_count") or 0) > 0) / total, 4),
            "bm25_hit_rate": round(sum(1 for event in events if int(event.get("lexical_count") or 0) > 0) / total, 4),
            "fallback_rate": round(sum(1 for event in events if bool(event.get("used_fallback"))) / total, 4),
            "table_not_found_rate": round(table_not_found / total, 4),
            "timeout_rate": round(timeout_count / total, 4),
            "concurrency_rejection_rate": round(failure_categories.get("concurrency_limit", 0) / total, 4),
            "failure_categories": dict(failure_categories),
            "context_limit_events": context_limit_count,
            "context_limit_rate": round(context_limit_count / total, 4),
            "last_snapshot_at": latest_snapshot.get("created_at") if latest_snapshot else None,
        }

    @staticmethod
    def _percentile(values: list[float], percentile: int) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = max(0, min(len(ordered) - 1, round((percentile / 100) * (len(ordered) - 1))))
        return round(float(ordered[index]), 2)

    def get_event(self, query_id: str) -> dict | None:
        items = self.list_events(query_id=query_id, limit=1)
        return items[0] if items else None

    @staticmethod
    def _percentile(values: list[float], percentile: int) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = max(0, min(len(ordered) - 1, round((percentile / 100) * (len(ordered) - 1))))
        return round(float(ordered[index]), 2)
