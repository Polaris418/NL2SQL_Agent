from __future__ import annotations

import asyncio
import unittest

from app.rag.access_control import AccessControlPolicy, AccessEffect, AccessRule, AccessRuleRepository, AccessScope
from app.rag.input_validation import InputValidationError, InputValidator
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.sensitive_fields import SensitiveFieldPolicy, sanitize_table_documentation
from app.rag.vector_store import InMemoryVectorStore
from app.schemas.connection import ColumnInfo, TableSchema


def _table(name: str, *columns: ColumnInfo) -> TableSchema:
    return TableSchema(name=name, columns=list(columns))


class SensitiveFieldPolicyTests(unittest.TestCase):
    def test_redacts_sensitive_samples_and_comments_in_table_docs(self) -> None:
        policy = SensitiveFieldPolicy()
        documentation = type(
            "Doc",
            (),
            {
                "table_name": "users",
                "business_summary": "Contains login data and contact details",
                "sample_values": [],
                "domain_tags": ["user"],
                "table_category": "dimension_table",
                "database_name": "demo",
                "schema_name": "public",
                "version": "v1",
                "metadata": {"knowledge_snippets": ["email addresses must stay private"]},
                "columns": [
                    type(
                        "Column",
                        (),
                        {
                            "name": "password",
                            "type": "TEXT",
                            "nullable": False,
                            "default": None,
                            "comment": "hashed password",
                            "is_primary_key": False,
                            "is_foreign_key": False,
                            "sample_values": ["super-secret"],
                            "enum_values": [],
                        },
                    )(),
                    type(
                        "Column",
                        (),
                        {
                            "name": "status",
                            "type": "TEXT",
                            "nullable": True,
                            "default": None,
                            "comment": "user status",
                            "is_primary_key": False,
                            "is_foreign_key": False,
                            "sample_values": ["active"],
                            "enum_values": [],
                        },
                    )(),
                ],
            },
        )()

        sanitized = sanitize_table_documentation(documentation, policy=policy)
        first_column = sanitized.columns[0]

        self.assertEqual(first_column.comment, "")
        self.assertEqual(first_column.sample_values, [])
        self.assertEqual(sanitized.metadata["sensitive_columns"], ["password"])
        self.assertEqual(sanitized.columns[1].sample_values, ["active"])


class InputValidatorTests(unittest.TestCase):
    def test_rejects_risky_sql_like_query(self) -> None:
        validator = InputValidator()
        result = validator.validate_query("show users; DROP TABLE users; --")

        self.assertFalse(result.is_valid)
        issue_codes = {issue.code for issue in result.issues}
        self.assertIn("drop_table", issue_codes)
        self.assertIn("sql_comment", issue_codes)

    def test_input_validation_error_is_raised_when_requested(self) -> None:
        validator = InputValidator()
        with self.assertRaises(InputValidationError):
            validator.validate_query("DROP TABLE users", raise_on_error=True)


class SecurityIntegrationTests(unittest.TestCase):
    def test_orchestrator_filters_out_tables_without_access_rule(self) -> None:
        users = _table("users", ColumnInfo(name="id", type="INTEGER"))
        orders = _table("orders", ColumnInfo(name="id", type="INTEGER"))
        policy = AccessControlPolicy(
            AccessRuleRepository(
                [
                    AccessRule(
                        rule_id="allow-tenant-a-users",
                        effect=AccessEffect.ALLOW,
                        priority=10,
                        principal_scope=AccessScope(tenant_id="tenant-a", connection_id="conn-1"),
                        resource_scope=AccessScope(connection_id="conn-1", table_name="users"),
                        reason="tenant-a can inspect users",
                    )
                ]
            ),
            default_effect=AccessEffect.DENY,
        )
        orchestrator = RetrievalOrchestrator(
            vector_store=InMemoryVectorStore(),
            access_policy=policy,
        )

        async def scenario() -> None:
            await orchestrator.index_schema("conn-1", [users, orders], database_name="demo", schema_name="public")
            result = await orchestrator.retrieve_detailed(
                "show orders",
                "conn-1",
                tenant_scope={"tenant_id": "tenant-a"},
            )

            self.assertEqual([table.name for table in result.tables], ["users"])
            self.assertEqual(result.metadata["access"]["denied_count"], 1)

        asyncio.run(scenario())

    def test_orchestrator_reports_invalid_input_without_retrying_retrieval(self) -> None:
        users = _table("users", ColumnInfo(name="id", type="INTEGER"))
        orchestrator = RetrievalOrchestrator(vector_store=InMemoryVectorStore())

        async def scenario() -> None:
            await orchestrator.index_schema("conn-2", [users], database_name="demo", schema_name="public")
            result = await orchestrator.retrieve_detailed("show users; DROP TABLE users; --", "conn-2")

            self.assertEqual(result.telemetry.failure_category, "input_validation_failed")
            self.assertFalse(result.tables)
            self.assertEqual(result.telemetry.failure_stage, "input_validation")

        asyncio.run(scenario())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
