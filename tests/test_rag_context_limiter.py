from __future__ import annotations

import unittest

from app.rag.context_packer import SchemaContextPacker
from app.schemas.connection import ColumnInfo, TableSchema


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(name=name, comment=comment, description=comment, columns=columns)


class ContextLimiterTests(unittest.TestCase):
    def test_context_limiter_enforces_token_budget_and_reports_drops(self) -> None:
        tables = [
            build_table(
                "orders",
                [
                    ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True),
                    ColumnInfo(name="amount", type="DECIMAL", comment="order amount total gross revenue"),
                    ColumnInfo(name="status", type="TEXT", comment="order lifecycle current status"),
                    ColumnInfo(name="created_at", type="TIMESTAMP"),
                ],
                comment="customer orders and revenue metrics",
            ),
            build_table(
                "users",
                [
                    ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="email", type="TEXT", comment="registered email address"),
                    ColumnInfo(name="nickname", type="TEXT", comment="display nickname alias"),
                    ColumnInfo(name="country", type="TEXT", comment="signup country region"),
                ],
                comment="application users and account metadata",
            ),
            build_table(
                "payments",
                [
                    ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="order_id", type="INTEGER", is_foreign_key=True),
                    ColumnInfo(name="provider", type="TEXT"),
                    ColumnInfo(name="settled_at", type="TIMESTAMP"),
                ],
                comment="payment settlement records",
            ),
        ]
        packer = SchemaContextPacker(
            max_tables=3,
            max_columns_per_table=4,
            max_relationship_clues=4,
            max_chars=450,
            max_tokens=55,
        )

        packed = packer.pack(
            "统计用户订单金额和支付情况",
            tables,
            db_type="mysql",
            relationship_clues=[
                {"source_table": "orders", "source_column": "user_id", "target_table": "users", "target_column": "id", "confidence": 0.9},
                {"source_table": "payments", "source_column": "order_id", "target_table": "orders", "target_column": "id", "confidence": 0.85},
            ],
            column_annotations={
                "orders": [{"column_name": "amount", "score": 0.95}],
                "users": [{"column_name": "email", "score": 0.8}],
            },
        )

        self.assertLessEqual(packed.token_count, 55)
        self.assertLessEqual(packed.char_count, 450)
        self.assertTrue(packed.limiter_metadata)
        self.assertIn("budget", packed.limiter_metadata)
        self.assertTrue(
            packed.truncated
            or packed.dropped_tables
            or packed.dropped_columns
            or packed.dropped_relationship_clues
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
