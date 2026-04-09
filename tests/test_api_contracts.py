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

    class Engine:  # pragma: no cover - compatibility shim
        pass

    class SQLAlchemyError(Exception):
        pass

    engine_module.Engine = Engine
    exc_module = types.ModuleType("sqlalchemy.exc")
    exc_module.SQLAlchemyError = SQLAlchemyError
    sys.modules["sqlalchemy"] = sqlalchemy
    sys.modules["sqlalchemy.engine"] = engine_module
    sys.modules["sqlalchemy.exc"] = exc_module


def _ensure_fastapi_stub() -> None:
    try:
        import fastapi  # type: ignore  # noqa: F401
        from fastapi import responses as _responses  # type: ignore  # noqa: F401
        return
    except Exception:
        pass

    if "fastapi" in sys.modules:
        return

    fastapi = types.ModuleType("fastapi")
    responses = types.ModuleType("fastapi.responses")
    middleware = types.ModuleType("fastapi.middleware")
    cors = types.ModuleType("fastapi.middleware.cors")
    status = types.ModuleType("fastapi.status")

    class FastAPI:
        def __init__(self, *args, **kwargs):
            self.routes = []

        def add_middleware(self, *args, **kwargs):
            return None

        def include_router(self, *args, **kwargs):
            return None

        def exception_handler(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    class Request:  # pragma: no cover - stub for import-time compatibility
        pass

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail=None):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class Depends:  # pragma: no cover - stub
        def __init__(self, dependency=None):
            self.dependency = dependency

    class APIRouter:
        def __init__(self, *args, **kwargs):
            self.routes = []

        def _decorator(self, *args, **kwargs):
            def wrapper(func):
                self.routes.append((args, kwargs, func))
                return func

            return wrapper

        post = get = delete = _decorator

    class Response:
        def __init__(self, content=b"", media_type: str | None = None, headers=None):
            self.content = content
            self.media_type = media_type
            self.headers = headers or {}

        @property
        def body(self):
            return self.content if isinstance(self.content, bytes) else str(self.content).encode("utf-8")

    class StreamingResponse(Response):
        def __init__(self, content, media_type: str | None = None, headers=None):
            super().__init__(b"", media_type=media_type, headers=headers)
            self.body_iterator = content

    class JSONResponse(Response):
        def __init__(self, content=None, status_code: int = 200, headers=None):
            super().__init__(content=content or {}, media_type="application/json", headers=headers)
            self.status_code = status_code

    class CORSMiddleware:  # pragma: no cover - stub for import-time compatibility
        def __init__(self, *args, **kwargs):
            pass

    fastapi.FastAPI = FastAPI
    fastapi.APIRouter = APIRouter
    fastapi.Depends = Depends
    fastapi.HTTPException = HTTPException
    fastapi.Request = Request
    status.HTTP_201_CREATED = 201
    status.HTTP_204_NO_CONTENT = 204
    status.HTTP_400_BAD_REQUEST = 400
    status.HTTP_404_NOT_FOUND = 404
    responses.Response = Response
    responses.StreamingResponse = StreamingResponse
    responses.JSONResponse = JSONResponse
    cors.CORSMiddleware = CORSMiddleware
    middleware.cors = cors

    sys.modules["fastapi"] = fastapi
    sys.modules["fastapi.responses"] = responses
    sys.modules["fastapi.middleware"] = middleware
    sys.modules["fastapi.middleware.cors"] = cors
    sys.modules["fastapi.status"] = status


_ensure_fastapi_stub()

if "app.core.factory" not in sys.modules:
    factory_module = types.ModuleType("app.core.factory")

    def get_container():
        return types.SimpleNamespace(metadata_db=None)

    factory_module.get_container = get_container
    sys.modules["app.core.factory"] = factory_module

if "app.core.config" not in sys.modules:
    config_module = types.ModuleType("app.core.config")

    class Settings:  # pragma: no cover - stub for import-time compatibility
        app_name = "Text-to-SQL Agent"
        app_version = "0.0.0"
        debug = False
        cors_origins = ["*"]
        api_prefix = "/api"
        default_query_limit = 1000
        chroma_persist_directory = "./chroma"

    def get_settings():
        return Settings()

    config_module.Settings = Settings
    config_module.get_settings = get_settings
    sys.modules["app.core.config"] = config_module

dependencies_module = sys.modules.get("app.core.dependencies")
if dependencies_module is None:
    dependencies_module = types.ModuleType("app.core.dependencies")
    sys.modules["app.core.dependencies"] = dependencies_module
if not hasattr(dependencies_module, "get_db_manager"):
    dependencies_module.get_db_manager = lambda: None
if not hasattr(dependencies_module, "get_agent"):
    dependencies_module.get_agent = lambda: None
if not hasattr(dependencies_module, "get_metadata_db"):
    dependencies_module.get_metadata_db = lambda: None
if not hasattr(dependencies_module, "get_service_container"):
    dependencies_module.get_service_container = lambda: types.SimpleNamespace(metadata_db=None)
if not hasattr(dependencies_module, "get_rag_index_manager"):
    dependencies_module.get_rag_index_manager = lambda: None

from app.api import analytics as analytics_api  # noqa: E402
from app.api import connections as connections_api  # noqa: E402
from app.api import history as history_api  # noqa: E402
from app.api import query as query_api  # noqa: E402
from app.db.metadata import MetadataDB  # noqa: E402
from app.db.repositories.connection_repo import DBConnectionRepository  # noqa: E402
from app.db.repositories.history_repo import QueryHistoryRepository  # noqa: E402
from app.schemas.connection import ColumnInfo, ConnectionConfig, ConnectionStatus, DatabaseType, SchemaCacheEntry, TableSchema  # noqa: E402
from app.schemas.query import AgentStep, AnalyticsReport, ChartSuggestion, ExecutionResult, QueryResult, QueryStatus  # noqa: E402


class FakeDBManager:
    def __init__(self, metadata_db):
        self.metadata_db = metadata_db
        self.created = []
        self.connections = []
        self.schema_cache = SchemaCacheEntry(
            connection_id="conn-1",
            tables=[
                TableSchema(
                    name="orders",
                    columns=[ColumnInfo(name="id", type="INTEGER")],
                )
            ],
        )

    def create_connection(self, config):
        self.created.append(config)
        return ConnectionStatus(
            id="conn-1",
            name=config.name,
            db_type=config.db_type,
            database=config.database,
            is_online=True,
        )

    def list_connections(self):
        return [
            ConnectionStatus(id="conn-1", name="demo", db_type=DatabaseType.SQLITE, database="demo.db", is_online=True)
        ]

    def delete_connection(self, connection_id: str):
        return connection_id == "conn-1"

    def refresh_schema_cache(self, connection_id: str):
        return self.schema_cache

    def get_schema_cache(self, connection_id: str):
        return self.schema_cache if connection_id == "conn-1" else None

    def get_connection_status(self, connection_id: str):
        if connection_id != "conn-1":
            raise KeyError(connection_id)
        return ConnectionStatus(id="conn-1", name="demo", db_type=DatabaseType.SQLITE, database="demo.db", is_online=True)

    def get_connector(self, connection_id: str):
        return types.SimpleNamespace(config=types.SimpleNamespace(db_type=types.SimpleNamespace(value="sqlite")))


class FakeAgent:
    def __init__(self):
        self.db_manager = types.SimpleNamespace(
            get_connector=lambda connection_id: types.SimpleNamespace(config=types.SimpleNamespace(db_type=types.SimpleNamespace(value="sqlite"))),
            settings=types.SimpleNamespace(query_timeout_seconds=30),
        )
        self.sql_executor = types.SimpleNamespace(
            execute=lambda connector, sql, **kwargs: ExecutionResult(
                columns=["id", "total_amount"],
                rows=[{"id": 1, "total_amount": 99.0}],
                row_count=1,
                db_latency_ms=12.0,
                total_row_count=1,
                source_sql=sql,
            )
        )
        self.chart_suggester = types.SimpleNamespace(
            suggest=lambda execution: ChartSuggestion(chart_type="table", x_axis=None, y_axis=None, reason="ok")
        )

    async def process_query(self, question: str, connection_id: str, **kwargs):
        if kwargs.get("step_handler") is not None:
            await kwargs["step_handler"](AgentStep(step_type="rewrite", content="rewrite"))
            await kwargs["step_handler"](AgentStep(step_type="done", content="done"))
        return QueryResult(
            question=question,
            rewritten_query="rewrite",
            retrieved_tables=[TableSchema(name="orders", columns=[ColumnInfo(name="id", type="INTEGER")])],
            sql="SELECT id FROM orders LIMIT 1;",
            result=ExecutionResult(columns=["id"], rows=[{"id": 1}], row_count=1, db_latency_ms=12.0),
            chart=ChartSuggestion(chart_type="table", x_axis=None, y_axis=None, reason="ok"),
            steps=[AgentStep(step_type="rewrite", content="rewrite")],
            status=QueryStatus.SUCCESS,
            retry_count=0,
            llm_latency_ms=1.0,
            db_latency_ms=12.0,
            context_source_query_id=kwargs.get("previous_query_id"),
        )


class ApiContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "metadata.sqlite3"
        self.metadata_db = MetadataDB(str(self.db_path))
        self.metadata_db.initialize()

        self.original_get_db_manager = connections_api.get_db_manager
        self.original_get_agent = query_api.get_agent
        self.original_get_metadata_db_history = history_api.get_metadata_db
        self.original_get_metadata_db_analytics = analytics_api.get_metadata_db

        fake_manager = FakeDBManager(self.metadata_db)
        fake_agent = FakeAgent()

        connections_api.get_db_manager = lambda: fake_manager
        query_api.get_agent = lambda: fake_agent
        history_api.get_metadata_db = lambda: self.metadata_db
        analytics_api.get_metadata_db = lambda: self.metadata_db

        self.connection_repo = DBConnectionRepository(self.metadata_db)
        self.history_repo = QueryHistoryRepository(self.metadata_db)

        stored = self.connection_repo.create(
            ConnectionConfig(name="demo", db_type=DatabaseType.SQLITE, database="demo.db"),
            is_online=True,
        )
        result = QueryResult(
            question="orders",
            rewritten_query="orders",
            retrieved_tables=[TableSchema(name="orders", columns=[ColumnInfo(name="id", type="INTEGER")])],
            sql="SELECT id FROM orders LIMIT 1;",
            result=ExecutionResult(columns=["id"], rows=[{"id": 1}], row_count=1, db_latency_ms=12.0),
            chart=types.SimpleNamespace(chart_type="table", x_axis=None, y_axis=None, reason="ok"),
            steps=[AgentStep(step_type="rewrite", content="rewrite")],
            status=QueryStatus.SUCCESS,
            retry_count=0,
            llm_latency_ms=1.0,
            db_latency_ms=12.0,
        )
        self.history_id = self.history_repo.save_query_result(stored.id, result)

    def tearDown(self) -> None:
        connections_api.get_db_manager = self.original_get_db_manager
        query_api.get_agent = self.original_get_agent
        history_api.get_metadata_db = self.original_get_metadata_db_history
        analytics_api.get_metadata_db = self.original_get_metadata_db_analytics
        self.temp_dir.cleanup()

    def test_connection_api_returns_friendly_error_shape(self):
        class FailingManager:
            def create_connection(self, config):
                raise ValueError("boom")

        fake_rag_index_manager = types.SimpleNamespace(schedule_rebuild=lambda *_args, **_kwargs: None)
        connections_api.get_db_manager = lambda: FailingManager()
        with self.assertRaises(Exception) as ctx:
            asyncio.run(
                connections_api.create_connection(
                    ConnectionConfig(name="x", db_type=DatabaseType.SQLITE, database="d.db"),
                    db_manager=FailingManager(),
                    rag_index_manager=fake_rag_index_manager,
                )
            )
        self.assertTrue(getattr(ctx.exception, "detail", None))

    def test_connection_api_round_trip_and_schema(self):
        fake_rag_index_manager = types.SimpleNamespace(schedule_rebuild=lambda *_args, **_kwargs: None)
        created = asyncio.run(
            connections_api.create_connection(
                ConnectionConfig(name="demo", db_type=DatabaseType.SQLITE, database="demo.db"),
                db_manager=FakeDBManager(self.metadata_db),
                rag_index_manager=fake_rag_index_manager,
            )
        )
        listed = asyncio.run(connections_api.list_connections(FakeDBManager(self.metadata_db)))
        schema = asyncio.run(connections_api.get_schema("conn-1", FakeDBManager(self.metadata_db)))
        refreshed = asyncio.run(
            connections_api.refresh_schema(
                "conn-1",
                FakeDBManager(self.metadata_db),
                rag_index_manager=fake_rag_index_manager,
            )
        )
        deleted = asyncio.run(connections_api.delete_connection("conn-1", FakeDBManager(self.metadata_db)))

        self.assertEqual(created.id, "conn-1")
        self.assertGreaterEqual(len(listed), 1)
        self.assertEqual(schema.connection_id, "conn-1")
        self.assertEqual(refreshed.connection_id, "conn-1")
        self.assertTrue(deleted is None)

    def test_history_and_analytics_endpoints_return_data(self):
        history = asyncio.run(history_api.list_history())
        detail = asyncio.run(history_api.get_history_detail(self.history_id))
        context = asyncio.run(history_api.get_follow_up_context(self.history_id))
        summary = asyncio.run(analytics_api.analytics_summary())
        errors = asyncio.run(analytics_api.analytics_errors())
        tables = asyncio.run(analytics_api.analytics_top_tables())
        report = asyncio.run(analytics_api.analytics_report())

        self.assertEqual(len(history), 1)
        self.assertEqual(detail.id, self.history_id)
        self.assertIsNotNone(detail.result)
        self.assertEqual(detail.result.row_count, 1)
        self.assertEqual(detail.result.rows[0]["id"], 1)
        self.assertIsNotNone(detail.chart)
        self.assertIn("Retrieved tables: orders", context.context_text)
        self.assertIn("Final SQL: SELECT id FROM orders LIMIT 1;", context.context_text)
        self.assertIn('"id": 1', context.context_text)
        self.assertGreaterEqual(summary.recent_success_rate, 0.0)
        self.assertIsInstance(report, AnalyticsReport)
        self.assertIsInstance(errors, list)
        self.assertIsInstance(tables, list)

    def test_query_export_and_streaming_contract(self):
        result = asyncio.run(query_api.run_query(query_api.QueryRequest(question="orders", connection_id="conn-1"), query_api.get_agent()))
        export = asyncio.run(query_api.export_query(query_api.QueryRequest(question="orders", connection_id="conn-1"), query_api.get_agent()))
        sql_export = asyncio.run(query_api.export_sql(query_api.SQLExecutionRequest(connection_id="conn-1", sql="SELECT id FROM orders"), query_api.get_agent()))
        stream = asyncio.run(query_api.stream_query(query_api.QueryRequest(question="orders", connection_id="conn-1"), query_api.get_agent()))
        stream_payloads = asyncio.run(_collect_stream(stream.body_iterator))

        self.assertEqual(result.status.value if hasattr(result.status, "value") else result.status, "success")
        self.assertIn("text/csv", export.media_type)
        self.assertIn("text/csv", sql_export.media_type)
        self.assertEqual(stream.media_type, "text/event-stream")
        self.assertTrue(any("event: step" in item for item in stream_payloads))
        self.assertTrue(any("event: result" in item for item in stream_payloads))


async def _collect_stream(iterator):
    chunks = []
    async for chunk in iterator:
        chunks.append(chunk.decode("utf-8") if isinstance(chunk, bytes) else str(chunk))
    return chunks


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
