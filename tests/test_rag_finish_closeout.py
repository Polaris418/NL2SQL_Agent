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
from app.db.repositories.rag_query_log_repo import RAGQueryLogRepository
from app.rag.logging import RetrievalLogEntry, RetrievalLogger
from app.schemas.connection import ColumnInfo, TableSchema
from app.schemas.rag import RAGHealthStatus, RAGIndexStatus


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(name=name, comment=comment, description=comment, columns=columns)


class RetrievalLogPersistenceTests(unittest.TestCase):
    def test_query_logs_are_persisted_and_generation_can_be_attached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata_db = MetadataDB(str(Path(tmp_dir) / "metadata.sqlite3"))
            metadata_db.initialize()
            repository = RAGQueryLogRepository(metadata_db)
            logger = RetrievalLogger(max_entries=2, repository=repository)

            logger.record(
                RetrievalLogEntry(
                    query_id="q1",
                    connection_id="conn-1",
                    original_query="show users",
                    rewritten_query="show active users",
                    selected_tables=["users"],
                    candidate_scores=[{"table": "users", "score": 0.9}],
                    reranked_tables=["users"],
                    cache_hit=True,
                    retrieval_latency_ms=12.5,
                    stage_latencies={"retrieval_ms": 12.5},
                )
            )
            logger.attach_generation("q1", prompt_schema="Table users(id)", final_sql="SELECT * FROM users")

            payload = logger.get("q1")
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertEqual(payload["prompt_schema"], "Table users(id)")
            self.assertEqual(payload["final_sql"], "SELECT * FROM users")
            self.assertEqual(payload["selected_tables"], ["users"])


class RAGCloseoutManagerTests(unittest.TestCase):
    def test_manager_exposes_version_history_diff_jobs_and_health_details(self) -> None:
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
                        "telemetry": {"logged_queries": 2},
                        "degradation": {
                            "current_mode": "hybrid",
                            "degradation_count": 0,
                            "recovery_count": 0,
                            "observation_count": 0,
                            "degradation_rate": 0.0,
                            "event_count": 0,
                            "degraded_connections": 0,
                            "total_connections": 1,
                        },
                        "vector_store_available": True,
                        "bm25_enabled": True,
                    }

                async def index_schema(self, *_args, **_kwargs):
                    await asyncio.sleep(0)

                def invalidate_connection_cache(self, _connection_id: str):
                    return 0

            manager = RAGIndexManager(metadata_db, FakeDBManager(), schema_retriever=FakeSchemaRetriever(), max_workers=1)

            manager.schedule_rebuild("conn-1")
            manager.async_indexing.wait_for_job("conn-1", timeout=2)

            first_version = manager.get_schema_version("conn-1").schema_version
            self.assertIsNotNone(first_version)
            self.assertEqual(manager.get_status("conn-1").index_status, RAGIndexStatus.READY)
            self.assertEqual(manager.get_status("conn-1").health_status, RAGHealthStatus.HEALTHY)

            tables.append(build_table("payments", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]))
            manager.schedule_rebuild("conn-1")
            manager.async_indexing.wait_for_job("conn-1", timeout=2)

            current = manager.get_schema_version("conn-1")
            history = manager.list_schema_versions("conn-1")
            detail = manager.get_schema_version_detail("conn-1", current.schema_version or "")
            diff = manager.diff_schema_versions("conn-1", left_version=first_version, right_version=current.schema_version)
            jobs = manager.list_index_jobs("conn-1", limit=10)
            health = manager.get_index_health_detail("conn-1")

            self.assertGreaterEqual(len(history), 2)
            self.assertIsNotNone(detail)
            self.assertIn("payments", diff.added_tables)
            self.assertTrue(jobs)
            self.assertTrue(health.vector_store_available)
            self.assertTrue(health.bm25_enabled)
            self.assertTrue(health.latest_jobs)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
