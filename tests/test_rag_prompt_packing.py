from __future__ import annotations

import asyncio
import sys
import types
import unittest

if "sqlalchemy" not in sys.modules:
    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy.create_engine = lambda *args, **kwargs: None
    sqlalchemy.inspect = lambda *args, **kwargs: None
    sqlalchemy.text = lambda value: value
    engine_module = types.ModuleType("sqlalchemy.engine")

    class Engine:  # pragma: no cover - import stub
        pass

    class SQLAlchemyError(Exception):
        pass

    engine_module.Engine = Engine
    exc_module = types.ModuleType("sqlalchemy.exc")
    exc_module.SQLAlchemyError = SQLAlchemyError
    sys.modules["sqlalchemy"] = sqlalchemy
    sys.modules["sqlalchemy.engine"] = engine_module
    sys.modules["sqlalchemy.exc"] = exc_module

if "app.db.manager" not in sys.modules:
    fake_db_manager_module = types.ModuleType("app.db.manager")

    class DBManager:  # pragma: no cover - import stub
        pass

    fake_db_manager_module.DBManager = DBManager
    sys.modules["app.db.manager"] = fake_db_manager_module

from app.agent.chart_suggester import ChartSuggester
from app.agent.nl2sql_agent import NL2SQLAgent
from app.agent.sql_generator import SQLGenerator
from app.rag.context_packer import SchemaContextPacker
from app.schemas.connection import ColumnInfo, TableSchema
from app.schemas.query import ExecutionResult


def build_table(name: str, comment: str, columns: list[ColumnInfo]) -> TableSchema:
    return TableSchema(name=name, comment=comment, description=comment, columns=columns)


class CapturingLLMClient:
    def __init__(self, response: str = "SELECT 1") -> None:
        self.response = response
        self.last_user_prompt: str = ""
        self.last_system_prompt: str = ""
        self.last_cache_hit = False

    async def chat(self, system_prompt: str, user_prompt: str):
        self.last_system_prompt = system_prompt
        self.last_user_prompt = user_prompt
        return self.response, 0.25


class SchemaContextPackerTests(unittest.TestCase):
    def test_pack_includes_relevant_columns_and_join_hints(self) -> None:
        users = build_table(
            "users",
            "User dimension table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="email", type="TEXT", comment="user email"),
            ],
        )
        orders = build_table(
            "orders",
            "Order fact table",
            [
                ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True),
                ColumnInfo(name="amount", type="DECIMAL", comment="order amount"),
            ],
        )

        packed = SchemaContextPacker(max_tables=2, max_columns_per_table=3).pack(
            "查询用户邮箱和订单金额",
            [users, orders],
            db_type="mysql",
            relationship_clues=[
                {
                    "source_table": "orders",
                    "target_table": "users",
                    "source_column": "user_id",
                    "target_column": "id",
                    "confidence": 0.91,
                    "reason": "foreign_key",
                }
            ],
            column_annotations={
                "users": [{"column_name": "email", "reason": "semantic_match", "score": 10.0}],
                "orders": [{"column_name": "amount", "reason": "semantic_match", "score": 9.0}],
            },
        )

        self.assertIn("Focus: user_id(fk/relationship_column), id(pk), amount(column_annotation)", packed.packed_text)
        self.assertIn("Focus: id(pk/relationship_column), email(column_annotation)", packed.packed_text)
        self.assertIn("Joins: user_id->users.id", packed.packed_text)
        self.assertIn("Relationship Clues:", packed.packed_text)


class SQLGeneratorPromptTests(unittest.TestCase):
    def test_generate_includes_packed_context_and_retrieval_metadata(self) -> None:
        llm = CapturingLLMClient("SELECT id, amount FROM orders")
        generator = SQLGenerator(llm)
        tables = [
            build_table(
                "orders",
                "Order fact table",
                [
                    ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                    ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True),
                    ColumnInfo(name="amount", type="DECIMAL"),
                ],
            )
        ]

        asyncio.run(
            generator.generate(
                "查询订单金额",
                "订单 金额",
                tables,
                "mysql",
                packed_schema_context="DB Type: mysql\nQuestion: 查询订单金额\nSchema Context:\n- Table: orders\n  Key Columns: id, amount",
                retrieval_metadata={
                    "relationship_clues": [
                        {
                            "source_table": "orders",
                            "target_table": "users",
                            "source_column": "user_id",
                            "target_column": "id",
                        }
                    ],
                    "column_annotations": {
                        "orders": [
                            {"column_name": "amount", "reason": "semantic_match"},
                            {"column_name": "user_id", "reason": "join_key"},
                        ]
                    },
                },
            )
        )

        self.assertIn("Schema:\nDB Type: mysql", llm.last_user_prompt)
        self.assertIn("Join Relationships:", llm.last_user_prompt)
        self.assertIn("- orders.user_id -> users.id", llm.last_user_prompt)
        self.assertIn("Relevant Columns:\n- orders: amount, user_id", llm.last_user_prompt)


class AgentPromptPackingIntegrationTests(unittest.TestCase):
    def test_agent_passes_packed_context_into_generation_chain(self) -> None:
        class FakeDBManager:
            def __init__(self):
                self.settings = types.SimpleNamespace(max_concurrent_queries=4, query_cache_ttl_seconds=60)
                self.metadata_db = types.SimpleNamespace(initialize=lambda: None)
                self.connector = types.SimpleNamespace(config=types.SimpleNamespace(db_type=types.SimpleNamespace(value="mysql")))
                self.tables = [
                    build_table(
                        "users",
                        "User dimension table",
                        [
                            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                            ColumnInfo(name="email", type="TEXT", comment="user email"),
                        ],
                    ),
                    build_table(
                        "orders",
                        "Order fact table",
                        [
                            ColumnInfo(name="id", type="INTEGER", is_primary_key=True),
                            ColumnInfo(name="user_id", type="INTEGER", is_foreign_key=True),
                            ColumnInfo(name="amount", type="DECIMAL", comment="order amount"),
                        ],
                    ),
                ]

            def get_connector(self, _connection_id: str):
                return self.connector

            def get_schema_cache(self, _connection_id: str):
                return types.SimpleNamespace(tables=self.tables)

            def refresh_schema_cache(self, _connection_id: str):
                return types.SimpleNamespace(tables=self.tables)

        class FakeRewriter:
            last_cache_hit = False

            async def rewrite(self, _question: str):
                return "用户 邮箱 订单 金额", 0.1

        class FakeRetriever:
            def __init__(self, tables: list[TableSchema]):
                self.tables = tables
                self.retrieval_backend = "hybrid-local"
                self.embedding_backend = "test-embedding"
                self.last_stats = {
                    "candidates": 4,
                    "selected": 2,
                    "top_score": 0.98,
                    "retrieval_backend": "hybrid-local",
                    "embedding_backend": "test-embedding",
                    "relationship_count": 1,
                    "relationship_tables": ["users"],
                    "column_annotation_count": 3,
                }

            async def index_schema(self, *_args, **_kwargs):
                return None

            async def retrieve_detailed(self, *_args, **_kwargs):
                return types.SimpleNamespace(
                    tables=self.tables,
                    metadata={
                        "relationship_clues": [
                            {
                                "source_table": "orders",
                                "target_table": "users",
                                "source_column": "user_id",
                                "target_column": "id",
                                "confidence": 0.92,
                                "reason": "foreign_key",
                            }
                        ],
                        "column_annotations": {
                            "users": [{"column_name": "email", "reason": "semantic_match", "score": 9.5}],
                            "orders": [{"column_name": "amount", "reason": "semantic_match", "score": 9.0}],
                        },
                    },
                )

        class FakeExecutor:
            def execute(self, *_args, **_kwargs):
                return ExecutionResult(columns=["email", "amount"], rows=[{"email": "a@example.com", "amount": 99}], row_count=1, db_latency_ms=3.5)

        class FakeReflector:
            last_cache_hit = False

            async def reflect(self, *_args, **_kwargs):
                return "fixed", "SELECT 1", 0.1

        class FakeHistoryRepo:
            def __init__(self, *_args, **_kwargs):
                pass

            @staticmethod
            def save_query_result(*_args, **_kwargs):
                return "history-1"

        fake_db = FakeDBManager()
        llm = CapturingLLMClient("SELECT users.email, orders.amount FROM orders JOIN users ON orders.user_id = users.id")
        generator = SQLGenerator(llm)
        agent = NL2SQLAgent(
            db_manager=fake_db,
            query_rewriter=FakeRewriter(),
            schema_retriever=FakeRetriever(fake_db.tables),
            sql_generator=generator,
            sql_executor=FakeExecutor(),
            error_reflector=FakeReflector(),
            chart_suggester=ChartSuggester(),
        )

        from app.agent import nl2sql_agent as agent_module

        original_history_repo = agent_module.QueryHistoryRepository
        agent_module.QueryHistoryRepository = FakeHistoryRepo
        try:
            result = asyncio.run(agent.process_query("查询用户邮箱和订单金额", "conn-1"))
        finally:
            agent_module.QueryHistoryRepository = original_history_repo

        self.assertEqual(result.status.value if hasattr(result.status, "value") else result.status, "success")
        self.assertIn("Relationship Clues:", llm.last_user_prompt)
        self.assertIn("Focus:", llm.last_user_prompt)
        self.assertIn("email", llm.last_user_prompt)
        self.assertIn("amount", llm.last_user_prompt)
        self.assertEqual(result.telemetry.relationship_count, 1)
        self.assertEqual(result.telemetry.column_annotation_count, 3)
        self.assertGreaterEqual(result.telemetry.retrieval_selected or 0, 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
