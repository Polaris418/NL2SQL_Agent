from __future__ import annotations

import unittest
from types import SimpleNamespace

from app.rag.access_control import (
    AccessControlPolicy,
    AccessDecision,
    AccessEffect,
    AccessRule,
    AccessRuleRepository,
    AccessScope,
)


class AccessControlPolicyTests(unittest.TestCase):
    def test_allow_rule_matches_tenant_project_connection_table_and_domain(self) -> None:
        repo = AccessRuleRepository(
            [
                AccessRule(
                    rule_id="allow-orders",
                    effect=AccessEffect.ALLOW,
                    priority=10,
                    principal_scope=AccessScope(tenant_id="tenant-a", project_id="project-a", connection_id="conn-1"),
                    resource_scope=AccessScope(
                        connection_id="conn-1",
                        database_name="demo",
                        schema_name="public",
                        db_type="mysql",
                        table_name="orders",
                        business_domains={"sales"},
                    ),
                    reason="Allow tenant-a project-a on sales orders",
                )
            ]
        )
        policy = AccessControlPolicy(repo)

        decision = policy.evaluate(
            {"tenant_id": "tenant-a", "project_id": "project-a", "connection_id": "conn-1"},
            {
                "connection_id": "conn-1",
                "database_name": "demo",
                "schema_name": "public",
                "db_type": "mysql",
                "table_name": "orders",
                "business_domains": ["sales"],
            },
        )

        self.assertIsInstance(decision, AccessDecision)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.effect, AccessEffect.ALLOW)
        self.assertEqual(decision.rule_id, "allow-orders")
        self.assertEqual(decision.reason, "Allow tenant-a project-a on sales orders")
        self.assertEqual(len(decision.matched_rules), 1)

    def test_deny_rule_wins_on_same_priority(self) -> None:
        repo = AccessRuleRepository(
            [
                AccessRule(
                    rule_id="allow-orders",
                    effect=AccessEffect.ALLOW,
                    priority=10,
                    principal_scope=AccessScope(tenant_id="tenant-a"),
                    resource_scope=AccessScope(table_name="orders"),
                    reason="allow",
                ),
                AccessRule(
                    rule_id="deny-orders",
                    effect=AccessEffect.DENY,
                    priority=10,
                    principal_scope=AccessScope(tenant_id="tenant-a"),
                    resource_scope=AccessScope(table_name="orders"),
                    reason="deny",
                ),
            ]
        )
        policy = AccessControlPolicy(repo)

        decision = policy.evaluate({"tenant_id": "tenant-a"}, {"table_name": "orders"})

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.effect, AccessEffect.DENY)
        self.assertEqual(decision.rule_id, "deny-orders")
        self.assertEqual([rule.rule_id for rule in decision.matched_rules], ["deny-orders", "allow-orders"])

    def test_higher_priority_allow_can_override_lower_priority_deny(self) -> None:
        repo = AccessRuleRepository(
            [
                AccessRule(
                    rule_id="deny-orders",
                    effect=AccessEffect.DENY,
                    priority=5,
                    principal_scope=AccessScope(tenant_id="tenant-a"),
                    resource_scope=AccessScope(table_name="orders"),
                    reason="deny low",
                ),
                AccessRule(
                    rule_id="allow-orders",
                    effect=AccessEffect.ALLOW,
                    priority=20,
                    principal_scope=AccessScope(tenant_id="tenant-a"),
                    resource_scope=AccessScope(table_name="orders"),
                    reason="allow high",
                ),
            ]
        )
        policy = AccessControlPolicy(repo)

        decision = policy.evaluate({"tenant_id": "tenant-a"}, {"table_name": "orders"})

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.effect, AccessEffect.ALLOW)
        self.assertEqual(decision.rule_id, "allow-orders")
        self.assertEqual([rule.rule_id for rule in decision.matched_rules], ["allow-orders", "deny-orders"])

    def test_default_deny_when_no_rule_matches(self) -> None:
        policy = AccessControlPolicy(AccessRuleRepository())

        decision = policy.evaluate({"tenant_id": "tenant-x"}, {"table_name": "users"})

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.effect, AccessEffect.DENY)
        self.assertIsNone(decision.rule_id)
        self.assertIn("default deny", decision.reason)
        self.assertEqual(decision.matched_rules, [])

    def test_repository_snapshot_list_delete_and_filter_resources(self) -> None:
        repo = AccessRuleRepository()
        repo.upsert(
            {
                "rule_id": "allow-hr-users",
                "effect": "allow",
                "priority": 10,
                "principal_scope": {
                    "tenant_id": "tenant-a",
                    "project_id": "project-a",
                    "connection_id": "conn-1",
                },
                "resource_scope": {
                    "connection_id": "conn-1",
                    "table_name": "users",
                    "business_domains": ["hr"],
                },
            }
        )
        repo.upsert(
            {
                "rule_id": "deny-finance",
                "effect": "deny",
                "priority": 50,
                "resource_scope": {
                    "business_domains": ["finance"],
                },
            }
        )

        self.assertEqual(len(repo.list()), 2)
        self.assertIsNotNone(repo.get("allow-hr-users"))
        self.assertTrue(repo.delete("deny-finance"))
        self.assertIsNone(repo.get("deny-finance"))
        self.assertEqual(repo.snapshot()["count"], 1)

        policy = AccessControlPolicy(repo)
        resources = [
            {"connection_id": "conn-1", "name": "users", "domain_tags": ["hr"]},
            {"connection_id": "conn-1", "name": "orders", "domain_tags": ["sales"]},
            SimpleNamespace(
                tenant_id="tenant-a",
                project_id="project-a",
                connection_id="conn-1",
                table_name="users",
                business_domains={"hr"},
            ),
            SimpleNamespace(
                tenant_id="tenant-a",
                project_id="project-a",
                connection_id="conn-1",
                table_name="orders",
                business_domains={"sales"},
            ),
        ]

        allowed = policy.filter_resources(
            resources,
            {"tenant_id": "tenant-a", "project_id": "project-a", "connection_id": "conn-1"},
        )

        self.assertEqual(len(allowed), 2)
        labels = []
        for resource in allowed:
            if hasattr(resource, "table_name"):
                labels.append(getattr(resource, "table_name"))
            else:
                labels.append(resource.get("name"))
        self.assertTrue(all(label == "users" for label in labels))
        self.assertNotIn("orders", labels)

    def test_access_scope_aliases_name_and_domain_tags(self) -> None:
        selector = AccessScope.from_any(
            {
                "tenant_id": "tenant-a",
                "table_name": None,
                "name": "users",
                "domain_tags": ["sales"],
            }
        )
        candidate = SimpleNamespace(
            tenant_id="tenant-a",
            name="users",
            business_domains={"sales"},
        )

        self.assertTrue(selector.matches(candidate))
        self.assertEqual(selector.table_name, "users")
        self.assertIn("sales", selector.business_domains)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
