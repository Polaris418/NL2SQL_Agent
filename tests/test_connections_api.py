from __future__ import annotations

import asyncio
import sys
import tempfile
import types
import unittest
from pathlib import Path

fastapi = sys.modules.get("fastapi")
if fastapi is None:
    fastapi = types.ModuleType("fastapi")
    sys.modules["fastapi"] = fastapi

if not hasattr(fastapi, "HTTPException"):

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail=None):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi.HTTPException = HTTPException

if not hasattr(fastapi, "APIRouter"):

    class APIRouter:
        def __init__(self, *args, **kwargs):
            pass

        @staticmethod
        def _decorator(*args, **kwargs):
            def wrap(func):
                return func

            return wrap

        post = get = delete = _decorator

    fastapi.APIRouter = APIRouter

if not hasattr(fastapi, "Depends"):

    def Depends(value=None):
        return value

    fastapi.Depends = Depends

status = getattr(fastapi, "status", None)
if status is None:
    status = types.ModuleType("fastapi.status")
    fastapi.status = status
    sys.modules["fastapi.status"] = status

for name, value in {
    "HTTP_201_CREATED": 201,
    "HTTP_204_NO_CONTENT": 204,
    "HTTP_400_BAD_REQUEST": 400,
    "HTTP_404_NOT_FOUND": 404,
}.items():
    setattr(status, name, value)

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

if "app.core.config" not in sys.modules:
    config_module = types.ModuleType("app.core.config")

    class Settings:  # pragma: no cover - compatibility shim
        pass

    config_module.Settings = Settings
    sys.modules["app.core.config"] = config_module

if "app.core.dependencies" not in sys.modules:
    dependencies_module = types.ModuleType("app.core.dependencies")
    dependencies_module.get_db_manager = lambda: None
    sys.modules["app.core.dependencies"] = dependencies_module
else:
    dependencies_module = sys.modules["app.core.dependencies"]

if not hasattr(dependencies_module, "get_rag_index_manager"):
    dependencies_module.get_rag_index_manager = lambda: None

from fastapi import HTTPException

from app.api import connections as connections_api
from app.db.connectors.base import DBConnectorError
from app.db.manager import DBManager
from app.db.metadata import MetadataDB
from app.db.repositories.connection_repo import DBConnectionRepository
from app.schemas.connection import ConnectionConfig, ConnectionTestResult, DatabaseType, SchemaCacheEntry, TableSchema


class ConnectionsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.metadata_db = MetadataDB(str(Path(self.temp_dir.name) / "metadata.sqlite3"))
        self.metadata_db.initialize()
        self.manager = DBManager(self.metadata_db, settings=types.SimpleNamespace())

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _create_connection(self, name: str = "local") -> str:
        repo = DBConnectionRepository(self.metadata_db)
        status = repo.create(ConnectionConfig(name=name, db_type=DatabaseType.SQLITE, database="demo.db"))
        return status.id

    def test_manager_test_connection_updates_status(self) -> None:
        connection_id = self._create_connection()

        class FakeConnector:
            def test_connection(self):
                return True

        self.manager._build_connector = lambda config: FakeConnector()  # type: ignore[method-assign]

        result = self.manager.test_connection(connection_id)
        refreshed = DBConnectionRepository(self.metadata_db).get_status(connection_id)

        self.assertTrue(result.success)
        self.assertGreaterEqual(result.latency_ms, 0.0)
        self.assertTrue(refreshed.is_online)

    def test_manager_test_connection_marks_offline_on_failure(self) -> None:
        connection_id = self._create_connection("broken")

        class FakeConnector:
            def test_connection(self):
                raise DBConnectorError("could not connect to server")

        self.manager._build_connector = lambda config: FakeConnector()  # type: ignore[method-assign]

        with self.assertRaises(DBConnectorError):
            self.manager.test_connection(connection_id)

        refreshed = DBConnectionRepository(self.metadata_db).get_status(connection_id)
        self.assertFalse(refreshed.is_online)

    def test_connection_api_returns_friendly_error_detail(self) -> None:
        connection_id = self._create_connection("api-test")
        result = ConnectionTestResult(connection_id=connection_id, success=True, message="ok", latency_ms=1.2)
        expected_id = connection_id

        class FakeManager:
            def test_connection(self, incoming_connection_id: str):
                assert incoming_connection_id == expected_id
                return result

        response = asyncio.run(connections_api.test_connection(connection_id, db_manager=FakeManager()))

        self.assertEqual(response, result)

    def test_connection_api_not_found_is_404(self) -> None:
        class FakeManager:
            def test_connection(self, incoming_connection_id: str):
                raise KeyError(incoming_connection_id)

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(connections_api.test_connection("missing", db_manager=FakeManager()))

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(ctx.exception.detail["error_type"], "not_found")

    def test_schema_api_distinguishes_missing_connection_and_cache(self) -> None:
        connection_id = self._create_connection("schema-test")
        schema = SchemaCacheEntry(
            connection_id=connection_id,
            tables=[TableSchema(name="orders", columns=[])],
        )
        expected_id = connection_id

        class FakeManager:
            def get_connection_status(self, incoming_connection_id: str):
                assert incoming_connection_id == expected_id
                return object()

            def get_schema_cache(self, incoming_connection_id: str):
                assert incoming_connection_id == expected_id
                return schema

        response = asyncio.run(connections_api.get_schema(connection_id, db_manager=FakeManager()))

        self.assertEqual(response, schema)

        class MissingManager:
            def get_connection_status(self, incoming_connection_id: str):
                raise KeyError(incoming_connection_id)

            def get_schema_cache(self, incoming_connection_id: str):
                raise AssertionError("should not be called")

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(connections_api.get_schema("missing", db_manager=MissingManager()))

        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(ctx.exception.detail["error_type"], "not_found")

    def test_refresh_schema_returns_friendly_error(self) -> None:
        class FakeManager:
            def refresh_schema_cache(self, incoming_connection_id: str):
                raise DBConnectorError("access denied for user")

        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(connections_api.refresh_schema("broken", db_manager=FakeManager()))

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail["error_type"], "permission_error")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
