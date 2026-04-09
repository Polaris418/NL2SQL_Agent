import asyncio
import unittest

from app.agent.contracts import ColumnInfo, TableSchema
from app.rag.embedding import DeterministicHashEmbedding
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.vector_store import InMemoryVectorStore


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(
        connection_id="conn_relationships",
        table_name=name,
        table_comment=comment,
        columns=columns,
        description=comment,
    )


class RelationshipAwareRetrievalTests(unittest.TestCase):
    def test_fact_table_retrieval_brings_related_dimension_table(self) -> None:
        users = build_table(
            "users",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="name", type="TEXT", comment="user name"),
            ],
            comment="User dimension table",
        )
        orders = build_table(
            "orders",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True, foreign_table="users"),
                ColumnInfo(name="amount", type="DECIMAL", comment="order amount"),
            ],
            comment="Order fact table",
        )

        orchestrator = RetrievalOrchestrator(
            embedding_model=DeterministicHashEmbedding(),
            vector_store=InMemoryVectorStore(),
        )

        async def scenario():
            await orchestrator.index_schema("conn_relationships", [users, orders], database_name="demo")
            return await orchestrator.retrieve_detailed("统计用户订单金额", "conn_relationships", top_k=1)

        result = asyncio.run(scenario())
        names = [table.table_name for table in result.tables]
        self.assertIn("orders", names)
        self.assertIn("users", names)
        self.assertTrue(result.metadata["relationship_clues"])
        self.assertGreaterEqual(result.telemetry.relationship_count, 1)

    def test_column_level_annotations_mark_relevant_columns(self) -> None:
        users = build_table(
            "users",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="email", type="TEXT", comment="user email"),
                ColumnInfo(name="nickname", type="TEXT"),
            ],
            comment="User dimension table",
        )
        orders = build_table(
            "orders",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True, foreign_table="users"),
                ColumnInfo(name="amount", type="DECIMAL", comment="order amount"),
                ColumnInfo(name="created_at", type="DATETIME"),
            ],
            comment="Order fact table",
        )

        orchestrator = RetrievalOrchestrator(
            embedding_model=DeterministicHashEmbedding(),
            vector_store=InMemoryVectorStore(),
        )

        async def scenario():
            await orchestrator.index_schema("conn_columns", [users, orders], database_name="demo")
            return await orchestrator.retrieve_detailed("查询用户邮箱和订单金额", "conn_columns", top_k=2)

        result = asyncio.run(scenario())
        annotations = result.metadata["column_annotations"]
        self.assertIn("users", annotations)
        self.assertIn("orders", annotations)
        user_columns = {item["column_name"] for item in annotations["users"]}
        order_columns = {item["column_name"] for item in annotations["orders"]}
        self.assertIn("email", user_columns)
        self.assertIn("amount", order_columns)
        self.assertGreaterEqual(result.telemetry.column_annotation_count, 2)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
