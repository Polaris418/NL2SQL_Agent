from __future__ import annotations

import asyncio
import unittest

from app.rag.concurrency import (
    RetrievalConcurrencyController,
    RetrievalConcurrencyError,
    RetrievalTimeoutError,
)
from app.rag.failure_classifier import FailureClassifier
from app.rag.vector_store import VectorStoreTimeoutError, VectorStoreUnavailableError


class RetrievalConcurrencyControllerTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_timeout_rejects_when_slots_are_busy(self) -> None:
        controller = RetrievalConcurrencyController(
            max_concurrent_requests=1,
            queue_timeout_seconds=0.05,
            retrieval_timeout_seconds=1.0,
        )
        blocker = asyncio.Event()

        async def long_running() -> str:
            await blocker.wait()
            return "done"

        first = asyncio.create_task(controller.execute("conn-1", long_running))
        await asyncio.sleep(0.05)

        with self.assertRaises(RetrievalConcurrencyError):
            await controller.execute("conn-1", long_running)

        blocker.set()
        self.assertEqual(await first, "done")
        snapshot = controller.snapshot()
        self.assertEqual(snapshot["rejected_requests"], 1)
        self.assertEqual(snapshot["queue_timeout_count"], 1)

    async def test_retrieval_timeout_is_recorded(self) -> None:
        controller = RetrievalConcurrencyController(
            max_concurrent_requests=1,
            queue_timeout_seconds=0.2,
            retrieval_timeout_seconds=0.05,
        )

        async def slow_operation() -> str:
            await asyncio.sleep(0.2)
            return "late"

        with self.assertRaises(RetrievalTimeoutError):
            await controller.execute("conn-1", slow_operation)

        snapshot = controller.snapshot()
        self.assertEqual(snapshot["retrieval_timeout_count"], 1)


class FailureClassifierTests(unittest.TestCase):
    def test_classifies_specialized_rag_errors(self) -> None:
        classifier = FailureClassifier()

        self.assertEqual(
            classifier.classify_exception(RetrievalConcurrencyError("busy"), stage="retrieval").category,
            "concurrency_limit",
        )
        self.assertEqual(
            classifier.classify_exception(RetrievalTimeoutError("slow"), stage="retrieval").category,
            "retrieval_timeout",
        )
        self.assertEqual(
            classifier.classify_exception(VectorStoreTimeoutError("vector timeout"), stage="vector").category,
            "timeout_error",
        )
        self.assertEqual(
            classifier.classify_exception(VectorStoreUnavailableError("down"), stage="vector").category,
            "vector_store_unavailable",
        )

    def test_falls_back_to_message_classification(self) -> None:
        classifier = FailureClassifier()

        table_missing = classifier.classify_message("no such table: employees", stage="retrieval")
        self.assertEqual(table_missing.category, "table_not_found")

        mismatch = classifier.classify_message("The current database may not match this question.", stage="retrieval")
        self.assertEqual(mismatch.category, "database_mismatch")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
