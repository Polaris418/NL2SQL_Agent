from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path

from backend.scripts.create_sample_db import main as build_sample_db


class SampleDbTests(unittest.TestCase):
    def test_sample_database_is_created_with_expected_tables(self) -> None:
        original_path = Path("backend/tests/fixtures/sample_ecommerce.db")
        if original_path.exists():
            original_path.unlink()

        build_sample_db()

        self.assertTrue(original_path.exists())
        conn = sqlite3.connect(original_path)
        try:
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            counts = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("users", "categories", "products", "orders", "reviews")
            }
        finally:
            conn.close()

        self.assertTrue({"users", "orders", "products", "reviews", "categories"}.issubset(tables))
        self.assertGreaterEqual(sum(counts.values()), 100)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
