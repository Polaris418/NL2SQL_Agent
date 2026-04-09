from __future__ import annotations

import asyncio
import unittest

from app.agent.sql_generator import SQLGenerator
from app.rag.business_knowledge import BusinessKnowledgeRepository
from app.rag.fewshot_integration import FewShotIntegration
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.profiling import ColumnProfile, ProfilingSnapshot, ProfilingStore, TableProfile
from app.rag.schema_doc import SchemaDocumentationManager
from app.rag.vector_store import InMemoryVectorStore
from app.schemas.connection import ColumnInfo, TableSchema


class _FakeLLMClient:
    last_cache_hit = False

    async def chat(self, _system_prompt: str, prompt: str) -> tuple[str, float]:
        return prompt, 1.0


def _table(name: str, *columns: str) -> TableSchema:
    return TableSchema(name=name, columns=[ColumnInfo(name=column, type="TEXT") for column in columns])


class ProfilingKnowledgeIntegrationTests(unittest.TestCase):
    def test_schema_documentation_includes_profile_and_business_knowledge(self) -> None:
        profiling_store = ProfilingStore()
        profiling_store.upsert(
            ProfilingSnapshot(
                connection_id="conn-1",
                tables=[
                    TableProfile(
                        table_name="orders",
                        row_count=42,
                        columns=[
                            ColumnProfile(
                                column_name="amount",
                                sample_values=["10", "20"],
                                distinct_count=2,
                                null_ratio=0.0,
                                min_value="10",
                                max_value="20",
                            )
                        ],
                    )
                ],
            )
        )
        knowledge_repo = BusinessKnowledgeRepository()
        knowledge_repo.add(
            "orders-bk",
            "Orders are finalized only when status = paid.",
            connection_id="conn-1",
            table_name="orders",
        )
        manager = SchemaDocumentationManager(
            profiling_store=profiling_store,
            business_knowledge_repository=knowledge_repo,
        )
        table = TableSchema(name="orders", columns=[ColumnInfo(name="amount", type="INTEGER")])

        doc = manager.generate_documentation(table, connection_id="conn-1")
        context_text = manager.to_context_text("orders")

        self.assertIn("Orders are finalized only when status = paid.", doc.metadata["knowledge_snippets"][0])
        self.assertIn("Profile: row_count=42", context_text)
        self.assertIn("distinct=2", context_text)


class FewShotPromptIntegrationTests(unittest.TestCase):
    def test_sql_generator_prefers_scope_aware_few_shots(self) -> None:
        integration = FewShotIntegration()
        integration.register(
            {
                "id": "tenant-hr-mysql",
                "question": "每个部门的员工数量",
                "sql": "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id;",
                "tenant_ids": ["tenant-a"],
                "connection_ids": ["conn-1"],
                "db_types": ["mysql"],
                "business_domains": ["organization"],
                "priority": 5,
            }
        )
        generator = SQLGenerator(_FakeLLMClient(), few_shot_integration=integration)
        prompt = generator._build_prompt(
            "每个部门的员工数量",
            "部门 员工 数量",
            [_table("employees", "id", "department_id")],
            "mysql",
            few_shot_scope={"tenant_id": "tenant-a", "connection_id": "conn-1", "db_type": "mysql"},
            few_shot_domain="organization",
        )

        self.assertIn("tenant-hr-mysql", prompt)
        self.assertTrue(generator.last_few_shot_examples)
        self.assertEqual(generator.last_few_shot_examples[0]["id"], "tenant-hr-mysql")


class MultiTenantCacheIsolationTests(unittest.TestCase):
    def test_orchestrator_cache_is_isolated_by_tenant_scope(self) -> None:
        orchestrator = RetrievalOrchestrator(vector_store=InMemoryVectorStore())
        tables = [_table("orders", "id", "user_id", "amount")]

        async def scenario() -> None:
            await orchestrator.index_schema("conn-1", tables, database_name="demo", schema_name="public")
            first = await orchestrator.retrieve_detailed("show orders", "conn-1", tenant_scope={"tenant_id": "tenant-a"})
            second = await orchestrator.retrieve_detailed("show orders", "conn-1", tenant_scope={"tenant_id": "tenant-b"})
            third = await orchestrator.retrieve_detailed("show orders", "conn-1", tenant_scope={"tenant_id": "tenant-a"})
            self.assertFalse(first.telemetry.cache_hit)
            self.assertFalse(second.telemetry.cache_hit)
            self.assertTrue(third.telemetry.cache_hit)
            self.assertNotEqual(first.telemetry.cache_key, second.telemetry.cache_key)

        asyncio.run(scenario())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
