from __future__ import annotations

import asyncio
import sys
import types
import unittest
from dataclasses import dataclass

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

from app.agent.chart_suggester import ChartSuggester
from app.agent.error_reflector import ErrorReflector
from app.agent.query_rewriter import QueryRewriter
from app.agent.schema_retriever import SchemaRetriever
from app.agent.sql_executor import SQLExecutor
from app.agent.sql_generator import SQLGenerator
from app.llm.client import LLMClient
from app.schemas.connection import ColumnInfo, TableSchema
from app.schemas.query import ExecutionResult


@dataclass
class FakeSettings:
    max_concurrent_queries: int = 5


class FakeConnector:
    def __init__(self):
        self.config = types.SimpleNamespace(db_type=types.SimpleNamespace(value="sqlite"))
        self.execute_calls = 0

    def execute(self, sql: str):
        self.execute_calls += 1
        return ["id", "total_amount"], [{"id": 1, "total_amount": 100.0}]


@dataclass
class FakeColumn:
    name: str
    type: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    comment: str | None = None


@dataclass
class FakeTable:
    name: str
    columns: list[FakeColumn]
    comment: str | None = None
    description: str | None = None

    @property
    def table_name(self) -> str:
        return self.name


class FakeSchemaCache:
    def __init__(self, tables):
        self.tables = tables
        self.updated_at = "2026-04-06T00:00:00"


class FakeDBManager:
    def __init__(self):
        self.settings = FakeSettings()
        self.metadata_db = types.SimpleNamespace(initialize=lambda: None)
        self.connector = FakeConnector()
        self.tables = [
            FakeTable(
                name="orders",
                columns=[
                    FakeColumn(name="id", type="INT", is_primary_key=True),
                    FakeColumn(name="total_amount", type="DECIMAL"),
                ],
            )
        ]

    def get_connector(self, connection_id: str):
        return self.connector

    def get_schema_cache(self, connection_id: str):
        return FakeSchemaCache(self.tables)

    def refresh_schema_cache(self, connection_id: str):
        return FakeSchemaCache(self.tables)


class FakeHistoryRepo:
    saved = []

    def __init__(self, *_args, **_kwargs):
        pass

    def save_query_result(self, connection_id: str, result):
        type(self).saved.append((connection_id, result))
        return "history-1"


class FakeLLMClient:
    async def chat(self, system_prompt: str, user_prompt: str):
        if "rewrite" in system_prompt.lower():
            return "orders total_amount", 0.1
        if "generate" in system_prompt.lower():
            return "SELECT id, total_amount FROM orders", 0.2
        if "fix" in system_prompt.lower():
            return "SELECT id, total_amount FROM orders", 0.3
        return "", 0.0


class QueryRewriterTests(unittest.TestCase):
    def test_rewrite_uses_cache(self) -> None:
        class CountingClient(FakeLLMClient):
            def __init__(self):
                self.calls = 0

            async def chat(self, system_prompt: str, user_prompt: str):
                self.calls += 1
                return "orders total_amount", 0.1

        client = CountingClient()
        rewriter = QueryRewriter(client)

        first = asyncio.run(rewriter.rewrite("show orders"))
        second = asyncio.run(rewriter.rewrite("show orders"))

        self.assertEqual(first[0], "orders total_amount")
        self.assertEqual(second[0], "orders total_amount")
        self.assertEqual(client.calls, 1)


class AgentPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fake_module = types.ModuleType("app.db.manager")
        fake_module.DBManager = FakeDBManager
        sys.modules["app.db.manager"] = fake_module
        from app.agent import nl2sql_agent as agent_module

        cls.agent_module = agent_module
        cls.original_save = agent_module.QueryHistoryRepository.save_query_result
        agent_module.QueryHistoryRepository.save_query_result = FakeHistoryRepo.save_query_result
        agent_module.QueryHistoryRepository.saved = []

    @classmethod
    def tearDownClass(cls) -> None:
        self_module = cls.agent_module
        self_module.QueryHistoryRepository.save_query_result = cls.original_save
        sys.modules.pop("app.db.manager", None)

    def test_process_query_success_path_emits_steps(self) -> None:
        fake_db = FakeDBManager()
        agent = self.agent_module.NL2SQLAgent(
            db_manager=fake_db,
            query_rewriter=QueryRewriter(FakeLLMClient()),
            schema_retriever=SchemaRetriever(persist_directory="/tmp/schema-test"),
            sql_generator=SQLGenerator(FakeLLMClient()),
            sql_executor=SQLExecutor(),
            error_reflector=ErrorReflector(FakeLLMClient()),
            chart_suggester=ChartSuggester(),
        )

        result = asyncio.run(agent.process_query("show orders", "conn-1"))

        self.assertEqual(result.status.value if hasattr(result.status, "value") else result.status, "success")
        self.assertEqual([step.step_type for step in result.steps[:4]], ["rewrite", "schema_retrieval", "sql_generation", "sql_execution"])
        self.assertEqual(result.chart.chart_type.value if hasattr(result.chart.chart_type, "value") else result.chart.chart_type, "pie")

    def test_process_query_retry_path_retries_once(self) -> None:
        fake_db = FakeDBManager()

        class FlakyExecutor:
            def __init__(self):
                self.calls = 0

            def execute(self, connector, sql: str, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("syntax error near FROM")
                return ExecutionResult(columns=["id"], rows=[{"id": 1}], row_count=1, db_latency_ms=1.0)

        class FixedReflector:
            async def reflect(self, question: str, failed_sql: str, error_message: str, schema_context):
                return "syntax issue", "SELECT id FROM orders", 0.05

        agent = self.agent_module.NL2SQLAgent(
            db_manager=fake_db,
            query_rewriter=QueryRewriter(FakeLLMClient()),
            schema_retriever=SchemaRetriever(persist_directory="/tmp/schema-test"),
            sql_generator=SQLGenerator(FakeLLMClient()),
            sql_executor=FlakyExecutor(),
            error_reflector=FixedReflector(),
            chart_suggester=ChartSuggester(),
            max_retries=3,
        )

        result = asyncio.run(agent.process_query("show orders", "conn-1"))

        self.assertEqual(result.retry_count, 1)
        self.assertEqual(result.status.value if hasattr(result.status, "value") else result.status, "success")
        self.assertIn("retry", [step.step_type for step in result.steps])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
