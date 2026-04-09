from __future__ import annotations

import unittest

from app.rag.fewshot_integration import FewShotExample, FewShotIntegration, FewShotRegistry
from app.rag.multi_tenant import MultiTenantIsolationManager, TenantScope


class MultiTenantIsolationManagerTests(unittest.TestCase):
    def test_isolation_key_and_metadata_filter_include_scope(self) -> None:
        manager = MultiTenantIsolationManager()
        scope = TenantScope(
            tenant_id="Tenant-A",
            project_id="Project-1",
            connection_id="Conn-9",
            database_name="SalesDB",
            schema_name="public",
            db_type="PostgreSQL",
            business_domains=["sales", "revenue"],
        )

        isolation_key = manager.isolation_key(scope)
        metadata_filter = manager.metadata_filter(scope).to_dict()
        payload = manager.scope_payload(scope)

        self.assertIn("tenant:tenant-a", isolation_key)
        self.assertIn("project:project-1", isolation_key)
        self.assertIn("connection:conn-9", isolation_key)
        self.assertEqual(metadata_filter["tenant_id"], "tenant-a")
        self.assertEqual(metadata_filter["db_type"], "postgresql")
        self.assertIn("sales", metadata_filter["business_domains"])
        self.assertEqual(payload["isolation_key"], isolation_key)

    def test_metadata_matching_respects_business_domain(self) -> None:
        manager = MultiTenantIsolationManager()
        scope = TenantScope(tenant_id="tenant-a", business_domains=["finance"])
        self.assertTrue(manager.matches_metadata({"tenant_id": "tenant-a", "business_domains": ["finance", "billing"]}, scope))
        self.assertFalse(manager.matches_metadata({"tenant_id": "tenant-a", "business_domains": ["sales"]}, scope))


class FewShotRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = FewShotRegistry()
        self.registry.register_many(
            [
                {
                    "id": "global-sqlite-sales",
                    "question": "How many orders do we have?",
                    "sql": "SELECT COUNT(*) FROM orders;",
                    "tenant_ids": [],
                    "project_ids": [],
                    "connection_ids": [],
                    "db_types": ["sqlite"],
                    "business_domains": ["sales"],
                    "priority": 1,
                },
                {
                    "id": "tenant-a-mysql-sales",
                    "question": "How many employees are in each department?",
                    "sql": "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id;",
                    "tenant_ids": ["tenant-a"],
                    "project_ids": ["project-1"],
                    "connection_ids": ["conn-9"],
                    "db_types": ["mysql"],
                    "business_domains": ["hr"],
                    "priority": 10,
                },
                {
                    "id": "tenant-a-mysql-sales-2",
                    "question": "Show sales by region",
                    "sql": "SELECT region, SUM(amount) FROM sales GROUP BY region;",
                    "tenant_ids": ["tenant-a"],
                    "project_ids": ["project-1"],
                    "db_types": ["mysql"],
                    "business_domains": ["sales"],
                    "priority": 2,
                },
            ]
        )

    def test_selects_by_tenant_connection_db_type_and_domain(self) -> None:
        scope = TenantScope(
            tenant_id="tenant-a",
            project_id="project-1",
            connection_id="conn-9",
            db_type="mysql",
            business_domains=["sales"],
        )

        selected = self.registry.select(scope=scope, query="sales region", limit=5)
        self.assertEqual([item.id for item in selected], ["tenant-a-mysql-sales-2"])

    def test_global_examples_are_used_when_scope_is_missing(self) -> None:
        selected = self.registry.select(query="orders", limit=1, db_type="sqlite", business_domain="sales")
        self.assertEqual(selected[0].id, "global-sqlite-sales")

    def test_prompt_block_is_human_readable(self) -> None:
        scope = TenantScope(tenant_id="tenant-a", project_id="project-1", db_type="mysql", business_domains=["sales"])
        prompt = self.registry.build_prompt_block(scope=scope, query="show sales by region", limit=2)
        self.assertIn("Question:", prompt)
        self.assertIn("SQL:", prompt)
        self.assertIn("Show sales by region", prompt)


class FewShotIntegrationTests(unittest.TestCase):
    def test_scope_payload_returns_isolation_and_few_shots(self) -> None:
        integration = FewShotIntegration()
        integration.register(
            {
                "id": "tenant-a-sales",
                "question": "Show sales by region",
                "sql": "SELECT region, SUM(amount) FROM sales GROUP BY region;",
                "tenant_ids": ["tenant-a"],
                "project_ids": ["project-1"],
                "db_types": ["mysql"],
                "business_domains": ["sales"],
            }
        )

        payload = integration.scope_payload(
            TenantScope(
                tenant_id="tenant-a",
                project_id="project-1",
                db_type="mysql",
                business_domains=["sales"],
            )
        )

        self.assertIn("tenant:tenant-a", payload["isolation_key"])
        self.assertEqual(len(payload["few_shot_prompt"]), len(payload["few_shot_prompt"].strip()))
        self.assertIn("tenant-a-sales", payload["few_shot_prompt"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
