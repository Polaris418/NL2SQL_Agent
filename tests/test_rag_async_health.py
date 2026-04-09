from __future__ import annotations

import threading
import time
import unittest

from app.rag.async_indexing import AsyncIndexingManager, IndexBuildArtifact
from app.rag.index_health import IndexHealthBuilder, IndexHealthSnapshot, IndexJobSnapshot
from app.schemas.rag import RAGHealthStatus, RAGIndexStatus


class AsyncIndexingManagerTests(unittest.TestCase):
    def test_rebuild_keeps_old_snapshot_until_successful_commit(self) -> None:
        manager = AsyncIndexingManager(max_workers=1)
        manager.register_snapshot(
            "conn-1",
            schema_version="v1",
            table_count=2,
            vector_count=2,
            metadata={"source": "initial"},
        )

        ready = threading.Event()
        release = threading.Event()

        def build_fn(connection_id: str, force_full_rebuild: bool, payload: dict[str, object]):
            ready.set()
            release.wait(timeout=2)
            return IndexBuildArtifact(
                schema_version="v2",
                table_count=4,
                vector_count=4,
                metadata={"connection_id": connection_id, "force_full_rebuild": force_full_rebuild, "payload": payload},
            )

        job = manager.schedule_rebuild("conn-1", build_fn, force_full_rebuild=True, payload={"reason": "refresh"})
        self.assertEqual(job.status, "running")
        self.assertTrue(ready.wait(timeout=1))

        in_progress_snapshot = manager.get_snapshot("conn-1")
        self.assertIsNotNone(in_progress_snapshot)
        self.assertEqual(in_progress_snapshot.schema_version, "v1")
        self.assertEqual(manager.get_job_state("conn-1"), "running")

        release.set()
        manager.wait_for_job("conn-1", timeout=2)

        finished_snapshot = manager.get_snapshot("conn-1")
        self.assertIsNotNone(finished_snapshot)
        self.assertEqual(finished_snapshot.schema_version, "v2")
        self.assertEqual(finished_snapshot.table_count, 4)
        self.assertEqual(manager.get_job_state("conn-1"), "completed")

    def test_failed_rebuild_preserves_previous_snapshot(self) -> None:
        manager = AsyncIndexingManager(max_workers=1)
        manager.register_snapshot("conn-2", schema_version="v1", table_count=3, vector_count=3)

        def build_fn(connection_id: str, force_full_rebuild: bool, payload: dict[str, object]):
            raise RuntimeError(f"rebuild failed for {connection_id}")

        manager.schedule_rebuild("conn-2", build_fn)
        with self.assertRaises(RuntimeError):
            manager.wait_for_job("conn-2", timeout=2)

        snapshot = manager.get_snapshot("conn-2")
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.schema_version, "v1")
        self.assertEqual(snapshot.table_count, 3)
        self.assertEqual(manager.get_job_state("conn-2"), "failed")

    def test_health_report_reflects_connection_state(self) -> None:
        manager = AsyncIndexingManager(max_workers=1)
        manager.register_snapshot("conn-healthy", schema_version="v1", table_count=3, vector_count=3)
        manager.register_snapshot("conn-degraded", schema_version="v2", table_count=2, vector_count=2)

        def failing_build(connection_id: str):
            raise RuntimeError("vector store unavailable")

        manager.schedule_rebuild("conn-degraded", failing_build)
        with self.assertRaises(RuntimeError):
            manager.wait_for_job("conn-degraded", timeout=2)

        healthy_state = manager.get_connection_state("conn-healthy")
        degraded_state = manager.get_connection_state("conn-degraded")
        report = manager.build_health_report("conn-healthy")

        self.assertEqual(healthy_state.index_status, RAGIndexStatus.READY)
        self.assertEqual(healthy_state.health_status, RAGHealthStatus.HEALTHY)
        self.assertEqual(degraded_state.index_status, RAGIndexStatus.FAILED)
        self.assertEqual(degraded_state.health_status, RAGHealthStatus.DEGRADED)
        self.assertEqual(report.metrics.total_connections, 1)
        self.assertEqual(report.connections[0].connection_id, "conn-healthy")


class IndexHealthBuilderTests(unittest.TestCase):
    def test_build_state_and_report(self) -> None:
        builder = IndexHealthBuilder()
        state = builder.build_state(
            "conn-3",
            snapshot=IndexHealthSnapshot(connection_id="conn-3", schema_version="v9", table_count=5, vector_count=7, is_indexed=True),
            job=IndexJobSnapshot(connection_id="conn-3", status="running", started_at="2026-04-07T12:00:00+08:00"),
            vector_store_available=False,
            bm25_enabled=True,
        )

        self.assertEqual(state.index_status, RAGIndexStatus.INDEXING)
        self.assertEqual(state.health_status, RAGHealthStatus.DEGRADED)
        self.assertEqual(state.schema_version, "v9")

        report = builder.build_report([state])
        self.assertEqual(report.metrics.total_connections, 1)
        self.assertEqual(report.metrics.indexing_connections, 1)
        self.assertEqual(report.connections[0].connection_id, "conn-3")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
