from __future__ import annotations

import unittest

from app.rag.degradation import DegradationManager
from app.schemas.rag import RAGIndexMode


class DegradationManagerTestCase(unittest.TestCase):
    def test_vector_store_unavailable_switches_to_bm25_only_and_records_event(self) -> None:
        manager = DegradationManager()

        mode = manager.record_vector_store_unavailable(
            "conn-1",
            reason="vector store unavailable",
            details={"timeout": True},
        )

        self.assertEqual(mode, RAGIndexMode.BM25_ONLY)
        self.assertTrue(manager.should_use_bm25_only("conn-1"))

        stats = manager.export_stats("conn-1")
        self.assertEqual(stats["current_mode"], "bm25_only")
        self.assertEqual(stats["degradation_count"], 1)
        self.assertEqual(stats["recovery_count"], 0)
        self.assertEqual(stats["observation_count"], 1)
        self.assertEqual(stats["event_count"], 1)
        self.assertEqual(stats["degradation_rate"], 1.0)
        self.assertEqual(stats["degraded_connections"], 1)

        events = manager.recent_events(connection_id="conn-1")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "vector_store_unavailable")
        self.assertEqual(events[0]["previous_mode"], "hybrid")
        self.assertEqual(events[0]["current_mode"], "bm25_only")
        self.assertIn("vector store unavailable", events[0]["reason"])
        self.assertIn("timeout=True", events[0]["reason"])

    def test_timeout_records_event_without_double_counting_transition(self) -> None:
        manager = DegradationManager()

        manager.record_vector_store_unavailable("conn-1", reason="store down")
        manager.record_timeout("conn-1", reason="query timeout")

        stats = manager.export_stats("conn-1")
        self.assertEqual(stats["current_mode"], "bm25_only")
        self.assertEqual(stats["degradation_count"], 1)
        self.assertEqual(stats["event_count"], 2)
        self.assertEqual(stats["observation_count"], 2)

        events = manager.recent_events(connection_id="conn-1")
        self.assertEqual([event["event_type"] for event in events], ["vector_store_unavailable", "timeout"])

    def test_recovery_requires_consecutive_healthy_probes(self) -> None:
        manager = DegradationManager(recovery_threshold=2)

        manager.record_timeout("conn-1", reason="slow vector store")
        mode_after_first_probe = manager.observe_vector_store_health("conn-1", available=True, reason="probe ok")
        self.assertEqual(mode_after_first_probe, RAGIndexMode.BM25_ONLY)

        mode_after_second_probe = manager.observe_vector_store_health("conn-1", available=True, reason="probe ok")
        self.assertEqual(mode_after_second_probe, RAGIndexMode.HYBRID)

        stats = manager.export_stats("conn-1")
        self.assertEqual(stats["current_mode"], "hybrid")
        self.assertEqual(stats["degradation_count"], 1)
        self.assertEqual(stats["recovery_count"], 1)
        self.assertEqual(stats["observation_count"], 3)
        self.assertEqual(stats["event_count"], 3)
        self.assertAlmostEqual(stats["degradation_rate"], 1 / 3, places=4)

        events = manager.recent_events(connection_id="conn-1")
        self.assertEqual(events[-1]["current_mode"], "hybrid")
        self.assertEqual(events[-1]["observed_healthy"], True)

    def test_global_snapshot_and_stats_track_multiple_connections(self) -> None:
        manager = DegradationManager()

        manager.record_vector_store_unavailable("conn-a", reason="vector store unavailable")
        manager.observe_vector_store_health("conn-b", available=True, reason="healthy probe")

        snapshot = manager.snapshot()
        self.assertEqual(snapshot["total_connections"], 2)
        self.assertEqual(snapshot["current_mode"], "bm25_only")
        self.assertEqual(snapshot["degraded_connections"], 1)
        self.assertIn("conn-a", snapshot["states"])
        self.assertIn("conn-b", snapshot["states"])
        self.assertEqual(snapshot["states"]["conn-a"]["mode"], "bm25_only")
        self.assertEqual(snapshot["states"]["conn-b"]["mode"], "hybrid")

        stats = manager.export_stats()
        self.assertEqual(stats["current_mode"], "bm25_only")
        self.assertEqual(stats["total_connections"], 2)
        self.assertEqual(stats["degraded_connections"], 1)
        self.assertEqual(stats["event_count"], 2)
        self.assertEqual(stats["degradation_count"], 1)

    def test_manual_override_and_invalid_mode(self) -> None:
        manager = DegradationManager()

        mode = manager.record_manual_mode("conn-1", RAGIndexMode.VECTOR, reason="manual override")
        self.assertEqual(mode, RAGIndexMode.VECTOR)
        self.assertEqual(manager.current_mode("conn-1"), RAGIndexMode.VECTOR)

        with self.assertRaises(ValueError):
            manager.record_manual_mode("conn-1", "not-a-mode")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
