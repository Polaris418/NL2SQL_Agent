from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.db.metadata import MetadataDB
from app.db.repositories.rag_telemetry_repo import RAGTelemetryRepository
from app.rag.telemetry import ContextLimitTelemetryEvent, RetrievalTelemetryEvent, TelemetrySystem


class TelemetrySystemTests(unittest.TestCase):
    def test_telemetry_system_persists_events_and_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata_db = MetadataDB(str(Path(tmp_dir) / "metadata.sqlite3"))
            metadata_db.initialize()
            repo = RAGTelemetryRepository(metadata_db)
            telemetry = TelemetrySystem(max_events=100, snapshot_interval=2, repository=repo)

            telemetry.record_retrieval(
                RetrievalTelemetryEvent(
                    query_id="q1",
                    connection_id="conn-1",
                    retrieval_latency_ms=10.0,
                    embedding_latency_ms=2.0,
                    lexical_count=2,
                    vector_count=1,
                    selected_tables=["orders"],
                    payload={"retrieval_backend": "hybrid"},
                )
            )
            telemetry.record_retrieval(
                RetrievalTelemetryEvent(
                    query_id="q2",
                    connection_id="conn-1",
                    retrieval_latency_ms=20.0,
                    embedding_latency_ms=3.0,
                    lexical_count=1,
                    vector_count=0,
                    cache_hit=True,
                    used_fallback=True,
                    error_type="table_not_found",
                    failure_stage="retrieval",
                    selected_tables=["users"],
                )
            )
            telemetry.record_context_limit(
                ContextLimitTelemetryEvent(
                    query_id="q2",
                    connection_id="conn-1",
                    limit_reason="token_budget",
                    truncated=True,
                    budget={"max_tokens": 128},
                    original_char_count=2048,
                    original_token_count=512,
                    final_char_count=512,
                    final_token_count=128,
                    dropped_tables=["users"],
                    payload={"stage": "context_packing"},
                )
            )
            telemetry.flush_snapshot(force=True)

            dashboard = telemetry.dashboard()
            history = telemetry.list_snapshot_history(limit=10)
            events = telemetry.list_events(connection_id="conn-1", limit=10)
            limits = telemetry.list_context_limit_events(connection_id="conn-1", limit=10)
            event = telemetry.get_event("q1")

            self.assertEqual(dashboard["current"]["logged_queries"], 2)
            self.assertEqual(dashboard["current"]["context_limit_events"], 1)
            self.assertIn("failure_categories", dashboard["current"])
            self.assertIn("table_not_found", dashboard["current"]["failure_categories"])
            self.assertIn("token_budget", str(dashboard["current"]["failure_categories"]))
            self.assertIsNotNone(dashboard["latest_snapshot"])
            self.assertGreaterEqual(len(history), 1)
            self.assertEqual(len(events), 2)
            self.assertEqual(len(limits), 1)
            self.assertEqual(limits[0]["limit_reason"], "token_budget")
            self.assertTrue(limits[0]["truncated"])
            self.assertIsNotNone(event)
            self.assertEqual(event["query_id"], "q1")
            self.assertEqual(events[0]["connection_id"], "conn-1")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
