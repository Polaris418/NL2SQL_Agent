from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.db.metadata import MetadataDB
from app.db.repositories.connection_repo import DBConnectionRepository
from app.db.repositories.history_repo import QueryHistoryRepository
from app.schemas.connection import ColumnInfo, ConnectionConfig, DatabaseType, TableSchema
from app.schemas.query import AgentStep, ChartSuggestion, ExecutionResult, QueryResult, QueryStatus


class RepositoryRoundTripTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "metadata.sqlite3"
        self.metadata_db = MetadataDB(str(self.db_path))
        self.metadata_db.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_connection_and_schema_cache_round_trip(self) -> None:
        repo = DBConnectionRepository(self.metadata_db)
        config = ConnectionConfig(name="local", db_type=DatabaseType.SQLITE, database="demo.db")

        status = repo.create(config, is_online=True)
        self.assertTrue(status.is_online)

        restored_config = repo.get_config(status.id)
        self.assertIsNotNone(restored_config)
        self.assertEqual(restored_config.name, config.name)
        self.assertEqual(restored_config.db_type, config.db_type)

        tables = [
            TableSchema(
                name="orders",
                comment="Orders",
                columns=[ColumnInfo(name="id", type="INT", is_primary_key=True)],
            )
        ]
        cache = repo.upsert_schema_cache(status.id, tables)
        restored_cache = repo.get_schema_cache(status.id)

        self.assertEqual(cache.connection_id, status.id)
        self.assertIsNotNone(restored_cache)
        self.assertEqual(restored_cache.tables[0].name, "orders")
        self.assertTrue(restored_cache.tables[0].columns[0].is_primary_key)

    def test_history_and_analytics_round_trip(self) -> None:
        connection_repo = DBConnectionRepository(self.metadata_db)
        history_repo = QueryHistoryRepository(self.metadata_db)
        status = connection_repo.create(
            ConnectionConfig(name="local", db_type=DatabaseType.SQLITE, database="demo.db"),
            is_online=True,
        )

        result = QueryResult(
            question="top orders",
            rewritten_query="orders top",
            retrieved_tables=[TableSchema(name="orders", columns=[ColumnInfo(name="id", type="INT")])],
            sql="SELECT id FROM orders LIMIT 10;",
            result=ExecutionResult(columns=["id"], rows=[{"id": 1}, {"id": 2}], row_count=2, db_latency_ms=5.0),
            chart=ChartSuggestion(chart_type="table", reason="preview"),
            status=QueryStatus.SUCCESS,
            retry_count=1,
            llm_latency_ms=18.0,
            db_latency_ms=5.0,
            steps=[
                AgentStep(step_type="rewrite", content="orders top"),
                AgentStep(step_type="done", content="success"),
            ],
        )

        query_id = history_repo.save_query_result(status.id, result)
        history = history_repo.list_history(limit=10)
        detail = history_repo.get_history(query_id)
        analytics = history_repo.analytics()

        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].id, query_id)
        self.assertEqual(history[0].status, QueryStatus.SUCCESS)
        self.assertEqual(history[0].retrieved_tables, ["orders"])
        self.assertEqual(history[0].result_row_count, 2)
        self.assertIsNotNone(detail)
        self.assertEqual(detail.result.row_count, 2)
        self.assertEqual(detail.result.rows[0]["id"], 1)
        self.assertEqual(detail.chart.chart_type, "table")
        self.assertIn('"id": 1', detail.follow_up_context)
        self.assertGreater(analytics.recent_success_rate, 0.0)
        self.assertTrue(any(item["table"] == "orders" for item in analytics.top_tables))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
