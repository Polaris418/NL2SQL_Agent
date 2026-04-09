from __future__ import annotations

import asyncio
import sys
import tempfile
import types
import unittest
from pathlib import Path

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

from app.agent.error_reflector import ErrorReflector
from app.agent.query_rewriter import QueryRewriter
from app.agent.sql_executor import SQLExecutor
from app.agent.utils import build_follow_up_prompt_context, build_user_friendly_error, execution_rows_to_csv, sanitize_error_message
from app.db.metadata import MetadataDB
from app.db.repositories.connection_repo import DBConnectionRepository
from app.db.repositories.history_repo import QueryHistoryRepository
from app.schemas.connection import ColumnInfo, ConnectionConfig, DatabaseType, TableSchema
from app.schemas.query import AgentStep, QueryResult, QueryStatus


class FakeLLMClient:
    async def chat(self, system_prompt: str, user_prompt: str):
        return "", 0.0


class FinalRegressionTests(unittest.TestCase):
    def test_follow_up_context_and_rewriter_fallback_keep_history_signal(self) -> None:
        prompt = build_follow_up_prompt_context(
            question="Show revenue by day",
            previous_question="Show total revenue",
            previous_sql="SELECT revenue FROM orders;",
            previous_tables=["orders", "customers"],
            instruction="group by day",
        )

        rewritten = asyncio.run(QueryRewriter(FakeLLMClient()).rewrite(prompt))[0]

        self.assertIn("Follow-up instruction: group by day", prompt)
        self.assertIn("Previous SQL: SELECT revenue FROM orders", prompt)
        self.assertIn("show", rewritten.lower())
        self.assertIn("revenue", rewritten.lower())
        self.assertIn("day", rewritten.lower())

    def test_error_reflector_and_executor_sanitize_common_failure_signals(self) -> None:
        self.assertEqual(sanitize_error_message("password=secret token=abc123 /tmp/data.csv"), "password=*** token=*** <path>")
        self.assertEqual(
            build_user_friendly_error("syntax error near from")[0],
            "syntax_error",
        )
        self.assertEqual(
            build_user_friendly_error("no such table: orders")[0],
            "table_not_found",
        )
        self.assertEqual(
            build_user_friendly_error("column amount not found")[0],
            "column_error",
        )
        reflector = ErrorReflector(FakeLLMClient())
        executor = SQLExecutor()
        sanitized = executor._sanitize_error("password=secret token=abc123")
        self.assertNotIn("secret", sanitized)
        self.assertNotIn("abc123", sanitized)

    def test_error_reflector_extracts_sql_from_verbose_fix_response(self) -> None:
        class VerboseFixLLMClient:
            async def chat(self, system_prompt: str, user_prompt: str):
                return (
                    "错误原因：MySQL 不支持在 IN 子查询中使用 LIMIT。\n\n"
                    "修复后的 SQL:\n"
                    "```sql\nWITH popular_tools AS (\n"
                    "  SELECT tool_id, COUNT(*) AS usage_count FROM t_tool_usage GROUP BY tool_id LIMIT 10\n"
                    ")\nSELECT * FROM popular_tools\n```",
                    0.1,
                )

        reflector = ErrorReflector(VerboseFixLLMClient())
        analysis, corrected_sql, _latency = asyncio.run(
            reflector.reflect(
                "给我列出用户表数据",
                "SELECT * FROM broken_sql",
                "This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'",
                [TableSchema(name="t_tool_usage", columns=[ColumnInfo(name="tool_id", type="INT")])],
            )
        )

        self.assertIn("Try a simpler query", analysis)
        self.assertTrue(corrected_sql.startswith("WITH popular_tools AS"))
        self.assertNotIn("错误原因", corrected_sql)
        self.assertNotIn("修复后的 SQL", corrected_sql)
        self.assertNotIn("```", corrected_sql)

    def test_csv_export_serializes_rows_and_blank_values(self) -> None:
        csv_text = execution_rows_to_csv(
            ["id", "name", "amount"],
            [
                {"id": 1, "name": "Alice", "amount": 10.5},
                {"id": 2, "name": "Bob"},
            ],
        )

        self.assertIn("id,name,amount", csv_text)
        self.assertIn("1,Alice,10.5", csv_text)
        self.assertIn("2,Bob,", csv_text)

    def test_history_repository_context_offset_and_error_distribution(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        try:
            db_path = Path(temp_dir.name) / "metadata.sqlite3"
            metadata_db = MetadataDB(str(db_path))
            metadata_db.initialize()

            class SequencedHistoryRepo(QueryHistoryRepository):
                def __init__(self, metadata_db, timestamps):
                    super().__init__(metadata_db)
                    self._timestamps = iter(timestamps)

                def utcnow(self):
                    return next(self._timestamps)

            repo = SequencedHistoryRepo(
                metadata_db,
                [
                    "2026-04-06T10:00:00",
                    "2026-04-06T10:00:01",
                    "2026-04-06T10:01:00",
                    "2026-04-06T10:01:01",
                ],
            )
            connection_repo = DBConnectionRepository(metadata_db)
            connection = connection_repo.create(
                ConnectionConfig(name="local", db_type=DatabaseType.SQLITE, database="demo.db"),
                is_online=True,
            )

            success_result = QueryResult(
                question="top orders",
                rewritten_query="orders top",
                retrieved_tables=[TableSchema(name="orders", columns=[ColumnInfo(name="id", type="INT")])],
                sql="SELECT id FROM orders LIMIT 10;",
                status=QueryStatus.SUCCESS,
                retry_count=1,
                llm_latency_ms=18.0,
                db_latency_ms=5.0,
                steps=[AgentStep(step_type="rewrite", content="orders top")],
            )
            failed_result = QueryResult(
                question="broken query",
                rewritten_query="broken query",
                retrieved_tables=[TableSchema(name="orders", columns=[ColumnInfo(name="id", type="INT")])],
                sql="SELECT bad FROM orders;",
                status=QueryStatus.FAILED,
                retry_count=3,
                llm_latency_ms=22.0,
                db_latency_ms=None,
                error_message="access denied for user",
                steps=[AgentStep(step_type="retry", content="retry 1")],
            )

            first_id = repo.save_query_result(connection.id, success_result)
            second_id = repo.save_query_result(connection.id, failed_result)

            history = repo.list_history(limit=1, offset=1)
            context = repo.build_follow_up_context(first_id)
            analytics = repo.analytics_report()

            self.assertEqual(len(history), 1)
            self.assertEqual(history[0].id, first_id)
            self.assertIsNotNone(context)
            self.assertIn("Question: top orders", context.context_text)
            self.assertIn("Final SQL: SELECT id FROM orders LIMIT 10;", context.context_text)
            self.assertIn("Retrieved tables: orders", context.context_text)
            self.assertTrue(any(item.type == "permission_error" for item in analytics.errors))
            self.assertTrue(any(item.table == "orders" for item in analytics.top_tables))
            self.assertEqual(context.history_id, first_id)
        finally:
            temp_dir.cleanup()

    def test_sql_executor_pagination_and_total_count_metadata(self) -> None:
        class PagingConnector:
            def __init__(self):
                self.config = types.SimpleNamespace(db_type=types.SimpleNamespace(value="sqlite"))
                self.calls: list[str] = []

            def execute(self, sql: str):
                self.calls.append(sql)
                if "count(" in sql.lower():
                    return ["total_count"], [{"total_count": 3}]
                return ["id", "name"], [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"},
                    {"id": 3, "name": "Cara"},
                ]

        executor = SQLExecutor(default_limit=1000, query_timeout_seconds=5)
        connector = PagingConnector()
        result = executor.execute(
            connector,
            "SELECT id, name FROM users",
            page_number=1,
            page_size=2,
            include_total_count=True,
        )

        self.assertGreaterEqual(len(connector.calls), 2)
        self.assertIn("LIMIT 2 OFFSET 0", connector.calls[0])
        self.assertIn("COUNT(*) AS total_count", connector.calls[1])
        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.total_row_count, 3)
        self.assertTrue(result.truncated)
        self.assertTrue(result.pagination.has_more)
        self.assertEqual(result.pagination.page_number, 1)
        self.assertEqual(result.pagination.page_size, 2)
        self.assertEqual(result.pagination.offset, 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
