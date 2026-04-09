from __future__ import annotations

import unittest

from app.schemas.connection import ColumnInfo, TableSchema
from app.schemas.query import ExecutionResult


class ModelRoundTripTests(unittest.TestCase):
    def test_table_schema_round_trip(self) -> None:
        schema = TableSchema(
            name="orders",
            comment="Orders table",
            columns=[
                ColumnInfo(name="id", type="INT", is_primary_key=True),
                ColumnInfo(name="total_amount", type="DECIMAL", nullable=False),
            ],
            description="Stores customer orders",
        )

        restored = TableSchema.model_validate(schema.model_dump(mode="json"))

        self.assertEqual(restored.name, schema.name)
        self.assertEqual(restored.comment, schema.comment)
        self.assertEqual([column.name for column in restored.columns], ["id", "total_amount"])
        self.assertTrue(restored.columns[0].is_primary_key)

    def test_execution_result_round_trip(self) -> None:
        result = ExecutionResult(
            columns=["id", "total_amount"],
            rows=[{"id": 1, "total_amount": 99.5}],
            row_count=1,
            truncated=False,
            db_latency_ms=12.3,
        )

        restored = ExecutionResult.model_validate(result.model_dump(mode="json"))

        self.assertEqual(restored.columns, result.columns)
        self.assertEqual(restored.rows, result.rows)
        self.assertEqual(restored.row_count, 1)
        self.assertAlmostEqual(restored.db_latency_ms or 0.0, 12.3)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
