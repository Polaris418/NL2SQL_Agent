from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from app.rag.column_retriever import ColumnLevelRetriever
from app.rag.evaluation import EvaluationDataset, EvaluationRunner, EvaluationSample
from app.rag.query_rewriter import QueryRewriter
from app.rag.relationship_retriever import RelationshipAwareRetriever
from app.rag.schema_doc import JoinPath, TableDocumentation
from app.rag.synonym_dict import SynonymDictionary, SynonymEntry
from app.schemas.connection import ColumnInfo, TableSchema


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(name=name, comment=comment, description=comment, columns=columns)


class EvaluationPropertyTests(unittest.IsolatedAsyncioTestCase):
    async def test_recall_and_mrr_follow_rank_positions(self) -> None:
        class RankingRetriever:
            async def retrieve(self, query: str, connection_id: str, top_k: int = 8):  # noqa: ARG002
                if "员工" in query:
                    return [
                        type("Table", (), {"name": "employees"})(),
                        type("Table", (), {"name": "departments"})(),
                        type("Table", (), {"name": "salaries"})(),
                    ]
                return [
                    type("Table", (), {"name": "customers"})(),
                    type("Table", (), {"name": "payments"})(),
                    type("Table", (), {"name": "invoices"})(),
                ]

        dataset = EvaluationDataset(
            name="retrieval-metrics",
            samples=[
                EvaluationSample(question="统计员工数量", target_tables=["employees"], connection_id="hr"),
                EvaluationSample(question="show invoices", target_tables=["invoices"], connection_id="finance"),
            ],
        )
        runner = EvaluationRunner(retriever=RankingRetriever(), apply_synonyms=False)

        report = await runner.run_evaluation(dataset, top_k_values=(1, 3))

        self.assertEqual(report.summary.total_samples, 2)
        self.assertAlmostEqual(report.summary.recall_at_k[1], 0.5)
        self.assertAlmostEqual(report.summary.recall_at_k[3], 1.0)
        self.assertAlmostEqual(report.summary.mrr, (1.0 + (1.0 / 3.0)) / 2.0)
        self.assertAlmostEqual(report.summary.top1_accuracy, 0.5)
        self.assertEqual(report.samples[0].first_hit_rank, 1)
        self.assertEqual(report.samples[1].first_hit_rank, 3)

    async def test_acceptance_and_stability_reports_include_expected_metrics(self) -> None:
        class StableRetriever:
            async def retrieve(self, query: str, connection_id: str, top_k: int = 8):  # noqa: ARG002
                if "订单" in query:
                    return [type("Table", (), {"name": "orders"})(), type("Table", (), {"name": "users"})()]
                return [type("Table", (), {"name": "users"})(), type("Table", (), {"name": "orders"})()]

        dataset = EvaluationDataset(
            name="stability-demo",
            samples=[
                EvaluationSample(question="查询订单", target_tables=["orders"], connection_id="conn-a"),
                EvaluationSample(question="查询用户", target_tables=["users"], connection_id="conn-a"),
            ],
        )
        runner = EvaluationRunner(retriever=StableRetriever(), apply_synonyms=False)

        acceptance = await runner.run_acceptance(dataset, min_recall_at_1=0.5, min_mrr=0.5)
        stability = await runner.run_stability_check(dataset, rounds=3)

        self.assertTrue(acceptance["passed"])
        self.assertIn("report", acceptance)
        self.assertIn("reports", stability)
        self.assertIn("variability", stability)
        self.assertEqual(stability["rounds"], 3)
        self.assertEqual(len(stability["reports"]), 3)
        self.assertTrue(stability["stable"])
        self.assertEqual(stability["variability"]["recall_at_1_span"], 0.0)


class QueryRewriteAndSynonymPropertyTests(unittest.TestCase):
    def test_rewrite_preserves_numbers_and_expands_synonyms(self) -> None:
        dictionary = SynonymDictionary()
        dictionary.add_entry(SynonymEntry(canonical="订单", synonyms=["order", "purchase"]))
        dictionary.add_entry(SynonymEntry(canonical="用户", synonyms=["member"], scope="conn-1"))
        rewriter = QueryRewriter(dictionary)

        result = asyncio.run(rewriter.rewrite("show order count top 10 for member", "conn-1", domain="sales"))

        self.assertEqual(result.original_query, "show order count top 10 for member")
        self.assertIn("10", result.rewritten_query)
        self.assertIn("订单", result.rewritten_query)
        self.assertIn("用户", result.rewritten_query)
        self.assertIn("purchase", result.expanded_query)
        self.assertIn("10", result.expanded_query)
        self.assertTrue({"total_queries", "rewritten_queries", "reloads"}.issubset(rewriter.stats))
        self.assertIn(("order", "订单"), result.applied_synonyms)
        self.assertIn(("member", "用户"), result.applied_synonyms)

    def test_synonym_dictionary_scopes_and_reload_remain_consistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "synonyms.json"
            path.write_text(
                json.dumps(
                    {
                        "global": {"订单": ["order"], "用户": ["member"]},
                        "connections": {"conn-1": {"产品": ["product"]}},
                        "domains": {"sales": {"收入": ["revenue"]}},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            dictionary = SynonymDictionary.from_file(path)

            self.assertEqual(dictionary.get_canonical("order"), "订单")
            self.assertEqual(dictionary.get_canonical("product", connection_id="conn-1"), "产品")
            self.assertEqual(dictionary.get_canonical("revenue", domain="sales"), "收入")
            self.assertIn("order", dictionary.get_synonyms("订单"))

            path.write_text(
                json.dumps(
                    {
                        "global": {"订单": ["order", "purchase"], "用户": ["member"]},
                        "connections": {"conn-1": {"产品": ["product"]}},
                        "domains": {"sales": {"收入": ["revenue"]}},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            dictionary.reload()
            self.assertIn("purchase", dictionary.get_synonyms("订单"))


class RelationshipAndColumnPropertyTests(unittest.TestCase):
    def test_relationship_aware_retriever_expands_connected_tables_in_order(self) -> None:
        users = build_table(
            "users",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="email", type="TEXT", comment="user email"),
            ],
            comment="User dimension table",
        )
        orders = build_table(
            "orders",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True),
                ColumnInfo(name="amount", type="DECIMAL", comment="order amount"),
            ],
            comment="Order fact table",
        )
        documents = {
            "users": TableDocumentation(table_name="users", business_summary="User dimension table", table_category="dimension_table", join_paths=[]),
            "orders": TableDocumentation(
                table_name="orders",
                business_summary="Order fact table",
                table_category="fact_table",
                join_paths=[
                    JoinPath(
                        source_table="orders",
                        source_column="user_id",
                        target_table="users",
                        target_column="id",
                        confidence=0.95,
                        reason="foreign_key",
                    )
                ],
            ),
        }
        retriever = RelationshipAwareRetriever()
        result = retriever.expand("查询用户订单", [users], documents, all_tables={"users": users, "orders": orders})

        self.assertEqual([table.name for table in result.related_tables], ["orders"])
        self.assertEqual(len(result.edges), 1)
        self.assertEqual(result.edges[0].source_table, "orders")
        self.assertEqual(result.edges[0].target_table, "users")

    def test_column_level_annotations_prioritize_relevant_fields(self) -> None:
        users = TableDocumentation(
            table_name="users",
            business_summary="User dimension table",
            columns=[
                type("Column", (), {"name": "id", "type": "INTEGER", "comment": "", "sample_values": [], "business_meaning": None, "is_primary_key": True, "is_foreign_key": False, "is_metric_field": False, "is_time_field": False, "score": 0.0})(),
                type("Column", (), {"name": "email", "type": "TEXT", "comment": "user email", "sample_values": ["a@example.com"], "business_meaning": "email address", "is_primary_key": False, "is_foreign_key": False, "is_metric_field": False, "is_time_field": False, "score": 0.0})(),
                type("Column", (), {"name": "nickname", "type": "TEXT", "comment": "display nickname", "sample_values": ["alice"], "business_meaning": "nickname", "is_primary_key": False, "is_foreign_key": False, "is_metric_field": False, "is_time_field": False, "score": 0.0})(),
            ],
        )
        orders = TableDocumentation(
            table_name="orders",
            business_summary="Order fact table",
            columns=[
                type("Column", (), {"name": "id", "type": "INTEGER", "comment": "", "sample_values": [], "business_meaning": None, "is_primary_key": True, "is_foreign_key": False, "is_metric_field": False, "is_time_field": False, "score": 0.0})(),
                type("Column", (), {"name": "user_id", "type": "INTEGER", "comment": "related user", "sample_values": ["1"], "business_meaning": "foreign key to users", "is_primary_key": False, "is_foreign_key": True, "is_metric_field": False, "is_time_field": False, "score": 0.0})(),
                type("Column", (), {"name": "amount", "type": "DECIMAL", "comment": "order amount", "sample_values": ["12.50"], "business_meaning": "amount", "is_primary_key": False, "is_foreign_key": False, "is_metric_field": True, "is_time_field": False, "score": 0.0})(),
            ],
        )
        retriever = ColumnLevelRetriever(top_k=2)
        annotations = retriever.annotate_documents("查询用户邮箱和订单金额", [users, orders])

        self.assertIn("users", annotations)
        self.assertIn("orders", annotations)
        self.assertIn("email", {item["column_name"] for item in annotations["users"]})
        self.assertIn("amount", {item["column_name"] for item in annotations["orders"]})
        self.assertGreaterEqual(len(annotations["users"]), 1)
        self.assertGreaterEqual(len(annotations["orders"]), 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
