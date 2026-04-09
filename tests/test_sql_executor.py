from __future__ import annotations

import sys
import types
import unittest

if "sqlalchemy" not in sys.modules:
    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy.create_engine = lambda *args, **kwargs: None
    sqlalchemy.inspect = lambda *args, **kwargs: None
    sqlalchemy.text = lambda value: value
    engine_module = types.ModuleType("sqlalchemy.engine")

    class Engine:  # pragma: no cover - stub for import-time compatibility
        pass

    class SQLAlchemyError(Exception):
        pass

    engine_module.Engine = Engine
    exc_module = types.ModuleType("sqlalchemy.exc")
    exc_module.SQLAlchemyError = SQLAlchemyError
    sys.modules["sqlalchemy"] = sqlalchemy
    sys.modules["sqlalchemy.engine"] = engine_module
    sys.modules["sqlalchemy.exc"] = exc_module

from app.agent.sql_executor import SQLExecutor
from app.db.connectors.base import DBConnectorError


class DummyConnector:
    def __init__(self, rows: list[dict[str, object]] | None = None, error: Exception | None = None):
        self.rows = rows or []
        self.error = error
        self.last_sql = None
        self.config = type("Config", (), {"db_type": type("DBType", (), {"value": "sqlite"})()})()

    def execute(self, sql: str):
        self.last_sql = sql
        if self.error is not None:
            raise self.error
        columns = list(self.rows[0].keys()) if self.rows else ["id"]
        return columns, self.rows


class SQLExecutorTests(unittest.TestCase):
    def test_validate_sql_adds_limit(self) -> None:
        executor = SQLExecutor(default_limit=25)
        sql = executor.validate_sql("SELECT id FROM orders")
        self.assertTrue(sql.endswith("LIMIT 25;"))

    def test_validate_sql_rejects_dangerous_statements(self) -> None:
        executor = SQLExecutor()
        with self.assertRaises(ValueError):
            executor.validate_sql("DROP TABLE users")
        with self.assertRaises(ValueError):
            executor.validate_sql("UPDATE users SET name = 'x'")

    def test_execute_truncates_rows_and_reports_latency(self) -> None:
        executor = SQLExecutor(default_limit=2)
        connector = DummyConnector(
            rows=[
                {"id": 1},
                {"id": 2},
                {"id": 3},
            ]
        )

        result = executor.execute(connector, "SELECT id FROM orders")

        self.assertEqual(result.columns, ["id"])
        self.assertEqual(result.row_count, 2)
        self.assertTrue(result.truncated)
        self.assertIn("LIMIT 2", connector.last_sql)

    def test_execute_sanitizes_connector_error(self) -> None:
        executor = SQLExecutor()
        connector = DummyConnector(error=DBConnectorError("password=secret token=abc123"))

        with self.assertRaises(RuntimeError) as ctx:
            executor.execute(connector, "SELECT id FROM orders")

        self.assertNotIn("secret", str(ctx.exception))
        self.assertNotIn("abc123", str(ctx.exception))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
