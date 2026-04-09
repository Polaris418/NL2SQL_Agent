import asyncio
import unittest

from app.rag.column_retriever import ColumnLevelRetriever
from app.rag.embedding import DeterministicHashEmbedding
from app.rag.orchestrator import RetrievalOrchestrator
from app.rag.vector_store import InMemoryVectorStore
from app.schemas.connection import ColumnInfo, TableSchema


def _table(name: str, comment: str, columns: list[ColumnInfo]) -> TableSchema:
    return TableSchema(name=name, comment=comment, columns=columns)


class ColumnLevelRetrieverTests(unittest.TestCase):
    def test_rank_columns_prefers_metric_and_foreign_key_fields(self) -> None:
        table = _table(
            "employees",
            "Employee fact table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_id", type="INTEGER", is_foreign_key=True),
                ColumnInfo(name="employee_count", type="INTEGER", comment="count of employees"),
                ColumnInfo(name="updated_at", type="TIMESTAMP"),
            ],
        )

        retriever = ColumnLevelRetriever(top_k=3)
        ranked = retriever.rank_columns("统计部门员工数量", table)

        self.assertGreaterEqual(len(ranked), 2)
        ranked_names = [match.column_name for match in ranked]
        self.assertIn("employee_count", ranked_names)
        self.assertIn("department_id", ranked_names)


class RelationshipAndColumnIntegrationTests(unittest.TestCase):
    def test_orchestrator_exposes_relationship_and_column_metadata(self) -> None:
        employees = _table(
            "employees",
            "Employee fact table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_id", type="INTEGER"),
                ColumnInfo(name="employee_count", type="INTEGER", comment="count of employees"),
            ],
        )
        departments = _table(
            "departments",
            "Department dimension table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_name", type="TEXT"),
            ],
        )

        orchestrator = RetrievalOrchestrator(
            embedding_model=DeterministicHashEmbedding(),
            vector_store=InMemoryVectorStore(),
            shard_threshold=2,
        )

        async def scenario() -> None:
            await orchestrator.index_schema("conn-1", [employees, departments], database_name="demo", schema_name="public")
            result = await orchestrator.retrieve_detailed("统计部门员工数量", "conn-1", top_k=1)

            self.assertTrue(result.tables)
            self.assertIn("employees", {table.name for table in result.tables})
            self.assertIn("departments", {table.name for table in result.tables})

            column_annotations = result.metadata.get("column_annotations") or {}
            relationship_clues = result.metadata.get("relationship_clues") or []

            self.assertIn("employees", column_annotations)
            self.assertTrue(any(item["column_name"] == "employee_count" for item in column_annotations["employees"]))
            self.assertTrue(any(item["column_name"] == "department_id" for item in column_annotations["employees"]))
            self.assertTrue(any(clue["target_table"] == "departments" for clue in relationship_clues))

        asyncio.run(scenario())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
