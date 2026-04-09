from __future__ import annotations

import asyncio
import unittest

from app.rag.evaluation import EvaluationDataset, EvaluationRunner, EvaluationSample


class StableRetriever:
    async def retrieve(self, query: str, connection_id: str, top_k: int = 8) -> list[object]:
        table_name = "employees" if "employee" in query.lower() or "员工" in query else "orders"
        return [type("Table", (), {"name": table_name})(), type("Table", (), {"name": "fallback"})()]


class RAGStabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_stability_check_is_deterministic_across_rounds(self) -> None:
        dataset = EvaluationDataset(
            name="stability-demo",
            samples=[
                EvaluationSample(question="count employees", target_tables=["employees"], connection_id="hr"),
                EvaluationSample(question="show orders", target_tables=["orders"], connection_id="sales"),
            ],
        )
        runner = EvaluationRunner(retriever=StableRetriever())

        payload = await runner.run_stability_check(dataset, rounds=4, top_k_values=(1, 3))

        self.assertTrue(payload["stable"])
        self.assertEqual(payload["rounds"], 4)
        self.assertEqual(len(payload["reports"]), 4)
        self.assertEqual(payload["variability"]["recall_at_1_span"], 0.0)
        self.assertEqual(payload["variability"]["mrr_span"], 0.0)
        self.assertEqual(payload["variability"]["table_not_found_rate_span"], 0.0)

    async def test_stability_check_output_is_json_friendly(self) -> None:
        dataset = EvaluationDataset(
            name="stability-json",
            samples=[EvaluationSample(question="show orders", target_tables=["orders"], connection_id="sales")],
        )
        runner = EvaluationRunner(retriever=StableRetriever())

        payload = await runner.run_stability_check(dataset, rounds=2)
        self.assertIn("reports", payload)
        self.assertIn("variability", payload)
        self.assertTrue(all("summary" in report for report in payload["reports"]))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
