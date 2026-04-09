from __future__ import annotations

import unittest

from app.rag.business_knowledge import BusinessKnowledgeItem, BusinessKnowledgeRepository
from app.rag.profiling import ColumnProfile, ProfilingSnapshot, ProfilingStore, TableProfile


class ProfilingModelTests(unittest.TestCase):
    def test_column_profile_supports_optional_summary_fields(self) -> None:
        profile = ColumnProfile.from_samples(
            "amount",
            [10, 10, None, 20, "30"],
            data_type="INTEGER",
            comment="订单金额",
            treat_as_numeric=True,
        )

        self.assertEqual(profile.column_name, "amount")
        self.assertEqual(profile.sample_values, ["10", "20", "30"])
        self.assertEqual(profile.distinct_count, 3)
        self.assertAlmostEqual(profile.null_ratio or 0.0, 0.2, places=2)
        self.assertEqual(profile.min_value, "10")
        self.assertEqual(profile.max_value, "30")
        self.assertTrue(profile.has_numeric_bounds)

    def test_snapshot_and_store_query_by_table(self) -> None:
        user_profile = ColumnProfile(column_name="user_id", distinct_count=5, null_ratio=0.0)
        table_profile = TableProfile(
            table_name="orders",
            columns=[user_profile],
            row_count=12,
            sample_rows=[{"user_id": 1}],
        )
        snapshot = ProfilingSnapshot(connection_id="conn-1", database_name="demo", tables=[table_profile])
        store = ProfilingStore()
        store.upsert(snapshot)

        self.assertIs(store.get("conn-1"), snapshot)
        self.assertEqual(store.query(connection_id="conn-1", table_name="orders")[0].table_name, "orders")
        self.assertEqual(snapshot.table("orders").row_count, 12)
        self.assertEqual(table_profile.column("user_id").distinct_count, 5)


class BusinessKnowledgeRepositoryTests(unittest.TestCase):
    def test_query_filters_by_connection_domain_and_table(self) -> None:
        repo = BusinessKnowledgeRepository()
        repo.add(
            "bk-1",
            "Employees belong to departments through department_id.",
            connection_id="conn-a",
            domain="hr",
            table_name="employees",
            keywords=["employee", "department"],
            priority=10,
        )
        repo.add(
            "bk-2",
            "Orders require a customer_id and order_date.",
            connection_id="conn-a",
            domain="sales",
            table_name="orders",
            keywords=["order", "customer"],
            priority=5,
        )
        repo.add(
            "bk-3",
            "Global policy for date filters.",
            domain="shared",
            keywords=["date", "filter"],
            priority=1,
        )

        hr_items = repo.query(connection_id="conn-a", domain="hr", table_name="employees")
        self.assertEqual([item.knowledge_id for item in hr_items], ["bk-1"])

        keyword_items = repo.query_text("department employee", connection_id="conn-a", domain="hr")
        self.assertEqual([item.knowledge_id for item in keyword_items], ["bk-1"])

        snapshot = repo.snapshot()
        self.assertEqual(snapshot["count"], 3)
        self.assertIn("hr", snapshot["domains"])
        self.assertIn("orders", snapshot["tables"])

    def test_upsert_preserves_created_at(self) -> None:
        repo = BusinessKnowledgeRepository()
        first = repo.add("bk-1", "Initial note", domain="sales")
        second = repo.add("bk-1", "Updated note", domain="sales")

        self.assertEqual(first.created_at, second.created_at)
        self.assertEqual(repo.get("bk-1").content, "Updated note")

    def test_extend_and_delete(self) -> None:
        repo = BusinessKnowledgeRepository()
        repo.extend(
            [
                BusinessKnowledgeItem(knowledge_id="bk-1", content="A", domain="a"),
                BusinessKnowledgeItem(knowledge_id="bk-2", content="B", domain="b"),
            ]
        )
        self.assertTrue(repo.delete("bk-1"))
        self.assertIsNone(repo.get("bk-1"))
        self.assertEqual(len(repo.list()), 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
