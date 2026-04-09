from __future__ import annotations

import asyncio
import sys
import tempfile
import types
import unittest
from pathlib import Path

if "app.db.manager" not in sys.modules:
    fake_db_manager_module = types.ModuleType("app.db.manager")

    class DBManager:  # pragma: no cover - import stub
        pass

    fake_db_manager_module.DBManager = DBManager
    sys.modules["app.db.manager"] = fake_db_manager_module

from app.core.rag_index_manager import RAGIndexManager
from app.db.metadata import MetadataDB
from app.rag.logging import RetrievalLogEntry, RetrievalLogger
from app.rag.telemetry import RetrievalTelemetryEvent, TelemetrySystem
from app.schemas.connection import ColumnInfo, TableSchema
from app.schemas.rag import RAGHealthStatus, RAGIndexStatus


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(name=name, comment=comment, description=comment, columns=columns)


class RetrievalObservabilityUnitTests(unittest.TestCase):
    def test_retrieval_logger_can_attach_generation_artifacts(self) -> None:
        logger = RetrievalLogger(max_entries=10)
        logger.record(
            RetrievalLogEntry(
                query_id="q1",
                connection_id="conn-1",
                original_query="show orders",
                selected_tables=["orders"],
            )
        )

        logger.attach_generation("q1", prompt_schema="Table: orders", final_sql="SELECT * FROM orders")
        payload = logger.get("q1")

        self.assertIsNotNone(payload)
        self.assertEqual(payload["prompt_schema"], "Table: orders")
        self.assertEqual(payload["final_sql"], "SELECT * FROM orders")

    def test_telemetry_system_aggregates_rates_and_percentiles(self) -> None:
        telemetry = TelemetrySystem(max_events=20)
        telemetry.record_retrieval(
            RetrievalTelemetryEvent(
                query_id="q1",
                connection_id="conn-1",
                retrieval_latency_ms=10.0,
                embedding_latency_ms=2.0,
                lexical_count=2,
                vector_count=1,
                cache_hit=False,
            )
        )
        telemetry.record_retrieval(
            RetrievalTelemetryEvent(
                query_id="q2",
                connection_id="conn-1",
                retrieval_latency_ms=30.0,
                embedding_latency_ms=4.0,
                lexical_count=1,
                vector_count=0,
                cache_hit=True,
                used_fallback=True,
                error_type="table_not_found",
            )
        )

        metrics = telemetry.get_metrics()
        self.assertEqual(metrics["logged_queries"], 2)
        self.assertGreaterEqual(metrics["retrieval_p95_ms"], metrics["retrieval_p50_ms"])
        self.assertEqual(metrics["vector_hit_rate"], 0.5)
        self.assertEqual(metrics["bm25_hit_rate"], 1.0)
        self.assertEqual(metrics["fallback_rate"], 0.5)
        self.assertEqual(metrics["table_not_found_rate"], 0.5)


class RAGIndexManagerIntegrationTests(unittest.TestCase):
    def test_manager_rebuild_updates_status_and_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata_db = MetadataDB(str(Path(tmp_dir) / "metadata.sqlite3"))
            metadata_db.initialize()

            tables = [
                build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
                build_table("orders", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            ]

            class FakeDBManager:
                def get_schema_cache(self, _connection_id: str):
                    return type("SchemaCache", (), {"tables": tables})()

                def refresh_schema_cache(self, _connection_id: str):
                    return type("SchemaCache", (), {"tables": tables})()

                def get_connection_status(self, _connection_id: str):
                    return type("Connection", (), {"database": "demo"})()

            class FakeSchemaRetriever:
                def __init__(self):
                    self.runtime_metrics = {
                        "cache": {
                            "entries": 1,
                            "hits": 2,
                            "misses": 1,
                            "evictions": 0,
                            "invalidations": 0,
                            "hit_rate": 0.6667,
                        },
                        "telemetry": {
                            "logged_queries": 3,
                            "retrieval_p50_ms": 12.0,
                            "retrieval_p95_ms": 25.0,
                            "retrieval_p99_ms": 30.0,
                            "embedding_p50_ms": 3.0,
                            "embedding_p95_ms": 4.0,
                            "embedding_p99_ms": 5.0,
                            "vector_hit_rate": 0.8,
                            "bm25_hit_rate": 1.0,
                            "fallback_rate": 0.1,
                            "table_not_found_rate": 0.05,
                        },
                    }

                async def index_schema(self, *_args, **_kwargs):
                    await asyncio.sleep(0)

                def invalidate_connection_cache(self, _connection_id: str):
                    return 0

                def get_query_details(self, query_id: str):
                    return {"query_id": query_id, "selected_tables": ["orders"]}

                def list_query_logs(self, *, connection_id: str | None = None, limit: int = 50):
                    return [{"query_id": "q1", "connection_id": connection_id or "conn-1"}][:limit]

            manager = RAGIndexManager(metadata_db, FakeDBManager(), schema_retriever=FakeSchemaRetriever(), max_workers=1)
            state = manager.schedule_rebuild("conn-1")
            self.assertEqual(state.index_status, RAGIndexStatus.INDEXING)

            manager.async_indexing.wait_for_job("conn-1", timeout=2)
            final_state = manager.get_status("conn-1")
            metrics = manager.get_metrics()

            self.assertEqual(final_state.index_status, RAGIndexStatus.READY)
            self.assertEqual(final_state.health_status, RAGHealthStatus.HEALTHY)
            self.assertTrue(final_state.is_indexed)
            self.assertEqual(metrics.logged_queries, 3)
            self.assertEqual(metrics.vector_hit_rate, 0.8)
            self.assertEqual(manager.get_schema_version("conn-1").table_count, 2)
            self.assertTrue(manager.list_query_logs(connection_id="conn-1"))
            self.assertEqual(manager.get_query_details("q1")["query_id"], "q1")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
