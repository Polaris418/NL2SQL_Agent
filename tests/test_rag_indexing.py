from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.rag.indexing import IndexingSystem
from app.rag.schema_version import SchemaVersionManager
from app.schemas.connection import ColumnInfo, TableSchema


def build_table(name: str, columns: list[ColumnInfo], comment: str = "") -> TableSchema:
    return TableSchema(name=name, comment=comment, description=comment, columns=columns)


class SchemaVersionManagerTests(unittest.TestCase):
    def test_compute_version_is_stable_for_table_reordering(self) -> None:
        manager = SchemaVersionManager()
        tables_a = [
            build_table("orders", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
        ]
        tables_b = list(reversed(tables_a))

        self.assertEqual(manager.compute_version(tables_a), manager.compute_version(tables_b))

    def test_persist_and_reload_version_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            storage = Path(tmp_dir)
            manager = SchemaVersionManager(storage)
            tables = [
                build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            ]

            first = manager.save_version("conn-1", tables, metadata={"source": "unit-test"})
            second = manager.save_version("conn-1", tables, metadata={"source": "unit-test-2"})

            reloaded = SchemaVersionManager(storage)
            history = reloaded.get_version_history("conn-1")

            self.assertEqual(len(history), 2)
            self.assertEqual(reloaded.get_current_version("conn-1").version, second.version)
            self.assertEqual(history[0].metadata["source"], "unit-test")
            self.assertEqual(first.schema_fingerprint, second.schema_fingerprint)


class IndexingSystemTests(unittest.TestCase):
    def test_detect_changes_identifies_added_updated_removed_and_unchanged(self) -> None:
        system = IndexingSystem()
        initial = [
            build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            build_table("orders", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
        ]
        system.incremental_update("conn-1", initial)

        updated = [
            build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            build_table(
                "orders",
                [
                    ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="amount", type="DECIMAL"),
                ],
            ),
            build_table("payments", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
        ]
        plan = system.detect_changes("conn-1", updated)

        self.assertEqual([item.table_name for item in plan.added], ["payments"])
        self.assertEqual([item.table_name for item in plan.updated], ["orders"])
        self.assertEqual([item.table_name for item in plan.unchanged], ["users"])
        self.assertEqual(plan.removed, [])
        self.assertIn("orders", plan.tables_to_index)
        self.assertIn("payments", plan.tables_to_index)

    def test_incremental_update_tracks_last_indexed_time_and_versions(self) -> None:
        system = IndexingSystem()
        tables_v1 = [
            build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            build_table("orders", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
        ]
        result_v1 = system.incremental_update("conn-1", tables_v1)
        users_state_v1 = system.get_table_last_indexed_at("conn-1", "users")
        orders_state_v1 = system.get_table_last_indexed_at("conn-1", "orders")

        tables_v2 = [
            build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            build_table(
                "orders",
                [
                    ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="amount", type="DECIMAL"),
                ],
            ),
        ]
        result_v2 = system.incremental_update("conn-1", tables_v2)

        self.assertIsNotNone(users_state_v1)
        self.assertIsNotNone(orders_state_v1)
        self.assertEqual(result_v1.schema_version, system.get_version_history("conn-1")[0])
        self.assertEqual(result_v2.indexed_tables, ["orders"])
        self.assertIn("users", result_v2.skipped_tables)
        self.assertGreaterEqual(len(system.get_version_history("conn-1")), 2)
        self.assertEqual(system.get_current_schema_version("conn-1"), result_v2.schema_version)

    def test_force_update_indexes_all_tables(self) -> None:
        system = IndexingSystem()
        tables = [
            build_table("users", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
            build_table("orders", [ColumnInfo(name="id", type="INTEGER", is_primary_key=True)]),
        ]
        system.incremental_update("conn-1", tables)
        forced = system.incremental_update("conn-1", tables, force=True)

        self.assertEqual(sorted(forced.indexed_tables), ["orders", "users"])
        self.assertEqual(forced.force_rebuild, True)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
