from __future__ import annotations

import asyncio
import unittest

from app.agent.sql_generator import SQLGenerator
from app.rag.context_packer import SchemaContextPacker
from app.schemas.connection import ColumnInfo, TableSchema


def _table(name: str, comment: str, columns: list[ColumnInfo]) -> TableSchema:
    return TableSchema(name=name, comment=comment, columns=columns)


class RecordingLLMClient:
    def __init__(self, response: str = "SELECT 1") -> None:
        self.response = response
        self.prompts: list[str] = []
        self.system_prompts: list[str] = []

    async def chat(self, system_prompt: str, user_prompt: str) -> tuple[str, float]:
        self.system_prompts.append(system_prompt)
        self.prompts.append(user_prompt)
        return self.response, 0.01


class SchemaContextPackerRegressionTests(unittest.TestCase):
    def test_pack_prefers_pk_fk_related_and_annotated_columns(self) -> None:
        employees = _table(
            "employees",
            "employee fact table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_id", type="INTEGER", is_foreign_key=True),
                ColumnInfo(name="employee_count", type="INTEGER", comment="count of employees"),
                ColumnInfo(name="updated_at", type="TIMESTAMP"),
                ColumnInfo(name="notes", type="TEXT"),
            ],
        )
        departments = _table(
            "departments",
            "department dimension table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_name", type="TEXT"),
            ],
        )

        packer = SchemaContextPacker(max_tables=2, max_columns_per_table=3, max_relationship_clues=4, max_chars=5000)
        packed = packer.pack(
            "统计每个部门的员工数量",
            [employees, departments],
            db_type="mysql",
            relationship_clues=[
                {
                    "source_table": "employees",
                    "source_column": "department_id",
                    "target_table": "departments",
                    "target_column": "id",
                    "confidence": 0.95,
                    "reason": "foreign_key",
                }
            ],
            column_annotations={
                "employees": [
                    {"column_name": "department_id", "score": 9.8, "reason": "relationship_column"},
                    {"column_name": "employee_count", "score": 9.1, "reason": "metric"},
                ]
            },
        )

        self.assertIn("Packing Rules: Keep PK/FK, relationship columns, and annotated columns first.", packed.packed_text)
        self.assertIn("Relationship Clues:", packed.packed_text)
        self.assertIn("employees.department_id -> departments.id", packed.packed_text)

        employees_context = next(table for table in packed.tables if table.table_name == "employees")
        selected_columns = [column.name for column in employees_context.columns]
        self.assertIn("id", selected_columns)
        self.assertIn("department_id", selected_columns)
        self.assertIn("employee_count", selected_columns)
        self.assertNotIn("notes", selected_columns)
        self.assertNotIn("updated_at", selected_columns)
        self.assertEqual(set(selected_columns[:2]), {"id", "department_id"})
        self.assertEqual(selected_columns[2], "employee_count")


class SQLGeneratorPromptPackingTests(unittest.TestCase):
    def test_generate_includes_packed_relationship_context(self) -> None:
        employees = _table(
            "employees",
            "employee fact table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_id", type="INTEGER", is_foreign_key=True),
                ColumnInfo(name="employee_count", type="INTEGER", comment="count of employees"),
                ColumnInfo(name="notes", type="TEXT"),
            ],
        )
        departments = _table(
            "departments",
            "department dimension table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="department_name", type="TEXT"),
            ],
        )
        relationship_clues = [
            {
                "source_table": "employees",
                "source_column": "department_id",
                "target_table": "departments",
                "target_column": "id",
                "confidence": 0.95,
                "reason": "foreign_key",
            }
        ]
        column_annotations = {
            "employees": [
                {"column_name": "department_id", "score": 9.8, "reason": "relationship_column"},
                {"column_name": "employee_count", "score": 9.1, "reason": "metric"},
            ]
        }
        packer = SchemaContextPacker(max_tables=2, max_columns_per_table=3, max_relationship_clues=4, max_chars=5000)
        packed = packer.pack(
            "统计每个部门的员工数量",
            [employees, departments],
            db_type="mysql",
            relationship_clues=relationship_clues,
            column_annotations=column_annotations,
        )
        llm = RecordingLLMClient(response="SELECT department_id, COUNT(*) AS employee_count FROM employees GROUP BY department_id")
        generator = SQLGenerator(llm)

        sql, latency = asyncio.run(
            generator.generate(
                "统计每个部门的员工数量",
                "统计每个部门的员工数量",
                [employees, departments],
                "mysql",
                packed_schema_context=packed.packed_text,
                retrieval_metadata={
                    "relationship_clues": relationship_clues,
                    "column_annotations": column_annotations,
                },
            )
        )

        self.assertTrue(sql.lower().startswith("select department_id"))
        self.assertGreaterEqual(latency, 0.0)
        self.assertTrue(llm.prompts)
        prompt = llm.prompts[-1]
        self.assertIn("Generation Rule: Prefer PK/FK columns, relationship hints, and annotated columns when joins or filters are needed.", prompt)
        self.assertIn("Packing Rules: Keep PK/FK, relationship columns, and annotated columns first.", prompt)
        self.assertIn("Relationship Clues:", prompt)
        self.assertIn("employees.department_id -> departments.id", prompt)
        self.assertIn("Relevant Columns:", prompt)
        self.assertIn("- employees: department_id, employee_count", prompt)
        self.assertIn("Available Tables: employees, departments", prompt)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
