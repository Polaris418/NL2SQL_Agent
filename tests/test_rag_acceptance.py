from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from app.rag.evaluation import EvaluationDataset, EvaluationRunner, EvaluationSample


class DummyRetriever:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def retrieve(self, query: str, connection_id: str, top_k: int = 8) -> list[object]:
        self.calls.append((query, connection_id, top_k))
        if "employee" in query.lower() or "员工" in query:
            return [type("Table", (), {"name": "employees"})(), type("Table", (), {"name": "departments"})()]
        return [type("Table", (), {"name": "orders"})(), type("Table", (), {"name": "users"})()]


class RAGAcceptanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_acceptance_runner_returns_pass_status_and_metrics(self) -> None:
        dataset = EvaluationDataset(
            name="acceptance-demo",
            samples=[
                EvaluationSample(question="count employees by department", target_tables=["employees"], connection_id="hr"),
                EvaluationSample(question="show orders", target_tables=["orders"], connection_id="sales"),
            ],
        )
        runner = EvaluationRunner(retriever=DummyRetriever())

        payload = await runner.run_acceptance(
            dataset,
            min_recall_at_1=0.5,
            min_mrr=0.5,
            max_table_not_found_rate=0.5,
        )

        self.assertTrue(payload["passed"])
        self.assertEqual(payload["reasons"], [])
        self.assertGreaterEqual(payload["report"]["summary"]["recall_at_k"]["1"], 0.5)
        self.assertEqual(payload["report"]["dataset_name"], "acceptance-demo")

    async def test_acceptance_runner_can_save_scriptable_output(self) -> None:
        dataset = EvaluationDataset(
            name="acceptance-save",
            samples=[EvaluationSample(question="show orders", target_tables=["orders"], connection_id="sales")],
        )
        runner = EvaluationRunner(retriever=DummyRetriever())
        payload = await runner.run_acceptance(dataset, min_recall_at_1=0.0)

        with tempfile.TemporaryDirectory(prefix="rag_acceptance_") as tmpdir:
            path = Path(tmpdir) / "acceptance.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            loaded = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(loaded["passed"])
            self.assertEqual(loaded["checks"]["min_recall_at_1"], 0.0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
