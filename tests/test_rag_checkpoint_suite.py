from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

import sys
import types

if "app.db.manager" not in sys.modules:
    fake_db_manager_module = types.ModuleType("app.db.manager")

    class DBManager:  # pragma: no cover - import stub
        pass

    fake_db_manager_module.DBManager = DBManager
    sys.modules["app.db.manager"] = fake_db_manager_module

from app.core.rag_index_manager import RAGIndexManager
from app.db.metadata import MetadataDB
from app.rag.cache import RetrievalCache
from app.rag.evaluation import EvaluationDataset, EvaluationRunner
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.sharding import SchemaShardPlanner
from app.rag.vector_store import InMemoryVectorStore
from app.schemas.connection import ColumnInfo, TableSchema


def make_table(name: str, *columns: str) -> TableSchema:
    return TableSchema(name=name, columns=[ColumnInfo(name=column, type="TEXT") for column in columns])


class PhaseCheckpointSuiteTests(unittest.TestCase):
    def test_phase_1_core_retrieval_checkpoint(self) -> None:
        orchestrator = RetrievalOrchestrator(
            vector_store=InMemoryVectorStore(),
            shard_planner=SchemaShardPlanner(max_shard_size=2, max_query_shards=2),
            cache=RetrievalCache(max_entries=8, ttl_seconds=600, enabled=True),
        )
        tables = [
            make_table("users", "id", "name", "email"),
            make_table("orders", "id", "user_id", "amount"),
            make_table("payments", "id", "order_id", "paid_at"),
        ]

        async def scenario() -> None:
            await orchestrator.index_schema("conn-checkpoint", tables, database_name="demo", schema_name="public")
            first = await orchestrator.retrieve_detailed("统计用户订单金额", "conn-checkpoint", top_k=2)
            second = await orchestrator.retrieve_detailed("统计用户订单金额", "conn-checkpoint", top_k=2)
            self.assertTrue(first.tables)
            self.assertTrue(second.telemetry.cache_hit)
            self.assertGreaterEqual(first.telemetry.shard_count, 1)

        asyncio.run(scenario())

    def test_phase_2_evaluation_checkpoint(self) -> None:
        class FakeRetriever:
            async def retrieve(self, query: str, connection_id: str, top_k: int = 8):
                if "order" in query.lower() or "订单" in query:
                    return [type("Table", (), {"name": "orders"})(), type("Table", (), {"name": "users"})()]
                return [type("Table", (), {"name": "users"})()]

        runner = EvaluationRunner(FakeRetriever())
        dataset = EvaluationDataset.from_dict(
            {
                "name": "checkpoint-eval",
                "samples": [
                    {"question": "show orders", "target_tables": ["orders"]},
                    {"question": "list users", "target_tables": ["users"]},
                ],
            }
        )

        report = asyncio.run(runner.run_evaluation(dataset, top_k_values=(1, 3)))
        self.assertEqual(report.summary.total_samples, 2)
        self.assertGreaterEqual(report.summary.recall_at_k[1], 0.5)
        self.assertGreaterEqual(report.summary.mrr, 0.5)
        self.assertLessEqual(report.summary.table_not_found_rate, 0.5)
        json.loads(report.to_json())

    def test_phase_3_index_health_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata_db = MetadataDB(str(Path(tmp_dir) / "metadata.sqlite3"))
            metadata_db.initialize()
            tables = [
                make_table("users", "id", "name"),
                make_table("orders", "id", "user_id", "amount"),
            ]

            class FakeDBManager:
                def get_schema_cache(self, _connection_id: str):
                    return type("SchemaCache", (), {"tables": tables})()

                def refresh_schema_cache(self, _connection_id: str):
                    return type("SchemaCache", (), {"tables": tables})()

                def get_connection_status(self, _connection_id: str):
                    return type("Connection", (), {"database": "demo"})()

            class FakeSchemaRetriever:
                runtime_metrics = {
                    "telemetry": {"logged_queries": 1},
                    "cache": {"entries": 1, "hits": 1, "misses": 0, "evictions": 0, "invalidations": 0, "hit_rate": 1.0},
                    "degradation": {"current_mode": "hybrid", "degraded_connections": 0, "degradation_count": 0, "recovery_count": 0},
                    "concurrency": {"retrieval_timeout_count": 0, "queue_timeout_count": 0, "rejected_requests": 0, "peak_active_requests": 1},
                }

                async def index_schema(self, *_args, **_kwargs):
                    await asyncio.sleep(0)

                def invalidate_connection_cache(self, _connection_id: str):
                    return 0

            manager = RAGIndexManager(metadata_db, FakeDBManager(), schema_retriever=FakeSchemaRetriever(), max_workers=1)
            manager.schedule_rebuild("conn-checkpoint")
            manager.async_indexing.wait_for_job("conn-checkpoint", timeout=2)

            state = manager.get_status("conn-checkpoint")
            health = manager.get_index_health_detail("conn-checkpoint")
            versions = manager.list_schema_versions("conn-checkpoint")

            self.assertTrue(state.is_indexed)
            self.assertTrue(state.schema_version)
            self.assertTrue(health.vector_store_available)
            self.assertTrue(health.bm25_enabled)
            self.assertTrue(health.latest_jobs)
            self.assertTrue(versions)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
