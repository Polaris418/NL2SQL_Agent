from __future__ import annotations

import unittest

from app.agent.contracts import ColumnInfo, TableSchema
from app.rag.embedding import DeterministicHashEmbedding
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.schema_doc import SchemaDocumentationManager
from app.rag.sharding import SchemaShardPlanner
from app.rag.vector_store import InMemoryVectorStore


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(
        connection_id="conn_1",
        table_name=name,
        table_comment=comment,
        columns=columns,
        description=comment,
    )


class SchemaShardingTests(unittest.TestCase):
    def test_shard_planner_groups_by_domain_and_selects_relevant_bucket(self) -> None:
        users = build_table(
            "users",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="name", type="TEXT"),
            ],
            comment="User dimension table",
        )
        employees = build_table(
            "employees",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_id", type="INTEGER"),
                ColumnInfo(name="employee_count", type="INTEGER", comment="count of employees"),
            ],
            comment="Employee fact table",
        )
        departments = build_table(
            "departments",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="name", type="TEXT"),
            ],
            comment="Department dimension table",
        )
        orders = build_table(
            "orders",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True, foreign_table="users"),
                ColumnInfo(name="amount", type="DECIMAL"),
            ],
            comment="Order fact table",
        )
        products = build_table(
            "products",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="name", type="TEXT"),
            ],
            comment="Product dimension table",
        )

        manager = SchemaDocumentationManager()
        docs = manager.generate_collection([users, employees, departments, orders, products])
        planner = SchemaShardPlanner(max_shard_size=2, max_query_shards=1)
        shards = planner.build("conn_1", [users, employees, departments, orders, products], {doc.table_name: doc for doc in docs})

        self.assertGreaterEqual(len(shards), 2)

        employee_shard = planner.select("统计部门员工数量", shards)
        order_shard = planner.select("统计订单金额", shards)

        self.assertEqual(len(employee_shard), 1)
        self.assertEqual(len(order_shard), 1)
        self.assertIn("employees", employee_shard[0].table_names)
        self.assertTrue({"orders", "products", "users"}.intersection(order_shard[0].table_names))

    def test_retrieval_cache_hits_and_invalidates_on_schema_change(self) -> None:
        users = build_table(
            "users",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="name", type="TEXT"),
            ],
            comment="User dimension table",
        )
        orders = build_table(
            "orders",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True, foreign_table="users"),
                ColumnInfo(name="amount", type="DECIMAL"),
            ],
            comment="Order fact table",
        )
        products = build_table(
            "products",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="name", type="TEXT"),
            ],
            comment="Product dimension table",
        )

        orchestrator = RetrievalOrchestrator(
            embedding_model=DeterministicHashEmbedding(),
            vector_store=InMemoryVectorStore(),
        )

        import asyncio

        async def exercise() -> tuple[str | None, str | None, bool, bool]:
            await orchestrator.index_schema("conn_1", [users, orders], database_name="demo", schema_name="public")
            first = await orchestrator.retrieve_detailed("统计订单金额", "conn_1", top_k=2)
            second = await orchestrator.retrieve_detailed("统计订单金额", "conn_1", top_k=2)
            await orchestrator.index_schema("conn_1", [users, orders, products], database_name="demo", schema_name="public")
            third = await orchestrator.retrieve_detailed("统计订单金额", "conn_1", top_k=2)
            return first.schema_version, third.schema_version, second.telemetry.cache_hit, third.telemetry.cache_hit

        first_version, third_version, second_hit, third_hit = asyncio.run(exercise())

        self.assertIsNotNone(first_version)
        self.assertIsNotNone(third_version)
        self.assertNotEqual(first_version, third_version)
        self.assertTrue(second_hit)
        self.assertFalse(third_hit)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
