from __future__ import annotations

import json
import unittest
from dataclasses import dataclass
from pathlib import Path

from app.rag.evaluation import (
    EvaluationDataset,
    EvaluationRunner,
    EvaluationSample,
    SynonymDictionary,
)


@dataclass(slots=True)
class DummyTable:
    name: str


class RecordingRetriever:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def retrieve(self, query: str, connection_id: str, top_k: int = 8) -> list[DummyTable]:
        self.calls.append((query, connection_id, top_k))
        if "员工" in query or "department" in query.lower():
            return [DummyTable("employees"), DummyTable("departments"), DummyTable("salaries")]
        if "订单" in query or "order" in query.lower():
            return [DummyTable("orders"), DummyTable("users"), DummyTable("products")]
        return [DummyTable("users"), DummyTable("products"), DummyTable("categories")]


class RagEvaluationTests(unittest.IsolatedAsyncioTestCase):
    def test_dataset_loader_supports_bare_list_and_object(self) -> None:
        with self.subTest("bare list"):
            bare_list_path = Path(self._temp_dir()) / "bare.json"
            bare_list_path.write_text(
                json.dumps(
                    [
                        {
                            "question": "统计员工数量",
                            "target_tables": ["employees"],
                            "connection_id": "hr",
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            bare_dataset = EvaluationRunner.load_dataset(bare_list_path)
            self.assertEqual(bare_dataset.name, "rag-evaluation")
            self.assertEqual(len(bare_dataset.samples), 1)
            self.assertEqual(bare_dataset.samples[0].target_tables, ["employees"])

        with self.subTest("wrapped object"):
            wrapped_path = Path(self._temp_dir()) / "wrapped.json"
            wrapped_path.write_text(
                json.dumps(
                    {
                        "name": "demo-eval",
                        "metadata": {"source": "unit-test"},
                        "samples": [
                            {
                                "question": "统计订单数量",
                                "target_tables": ["orders"],
                                "relationships": ["orders.users_id -> users.id"],
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            wrapped_dataset = EvaluationRunner.load_dataset(wrapped_path)
            self.assertEqual(wrapped_dataset.name, "demo-eval")
            self.assertEqual(wrapped_dataset.metadata, {"source": "unit-test"})
            self.assertEqual(wrapped_dataset.samples[0].relationships, ["orders.users_id -> users.id"])

    async def test_evaluation_runner_computes_key_metrics(self) -> None:
        dataset = EvaluationDataset(
            name="metrics-demo",
            samples=[
                EvaluationSample(
                    question="count employees by department",
                    target_tables=["employees"],
                    connection_id="hr",
                ),
                EvaluationSample(
                    question="show invoices",
                    target_tables=["invoices"],
                    connection_id="finance",
                ),
            ],
        )
        retriever = RecordingRetriever()
        synonyms = SynonymDictionary.from_file("config/synonyms.json")
        runner = EvaluationRunner(retriever=retriever, synonym_dictionary=synonyms)

        report = await runner.run_evaluation(dataset)

        self.assertEqual(report.summary.total_samples, 2)
        self.assertAlmostEqual(report.summary.recall_at_k[1], 0.5)
        self.assertAlmostEqual(report.summary.recall_at_k[3], 0.5)
        self.assertAlmostEqual(report.summary.mrr, 0.5)
        self.assertAlmostEqual(report.summary.top1_accuracy, 0.5)
        self.assertAlmostEqual(report.summary.table_not_found_rate, 0.5)
        self.assertEqual(len(report.samples), 2)
        self.assertTrue(any("员工" in call[0] for call in retriever.calls))
        self.assertTrue(any(sample.first_hit_rank == 1 for sample in report.samples))
        self.assertTrue(any(sample.table_not_found for sample in report.samples))

        report_path = Path(self._temp_dir()) / "report.json"
        report_path.write_text(report.to_json(), encoding="utf-8")
        loaded = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(loaded["summary"]["total_samples"], 2)
        self.assertAlmostEqual(loaded["summary"]["recall_at_k"]["1"], 0.5)

    def _temp_dir(self) -> str:
        from tempfile import mkdtemp

        return mkdtemp(prefix="rag_eval_test_")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
