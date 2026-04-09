from __future__ import annotations

from time import perf_counter

from app.core.config import Settings
from app.db.connectors.base import DBConnector
from app.db.connectors.mysql import MySQLConnector
from app.db.connectors.postgresql import PostgreSQLConnector
from app.db.connectors.sqlite import SQLiteConnector
from app.db.metadata import MetadataDB
from app.db.repositories.connection_repo import DBConnectionRepository
from app.schemas.connection import ConnectionConfig, ConnectionStatus, ConnectionTestResult, SchemaCacheEntry


class DBManager:
    def __init__(self, metadata_db: MetadataDB, settings: Settings):
        self.metadata_db = metadata_db
        self.settings = settings
        self._connectors: dict[str, DBConnector] = {}

    def create_connection(self, config: ConnectionConfig) -> ConnectionStatus:
        connector = self._build_connector(config)
        is_online = connector.test_connection()
        repo = DBConnectionRepository(self.metadata_db)
        status = repo.create(config, is_online=is_online)
        self._connectors[status.id] = connector
        self.refresh_schema_cache(status.id)
        return repo.get_status(status.id)

    def list_connections(self) -> list[ConnectionStatus]:
        return DBConnectionRepository(self.metadata_db).list_all()

    def get_connection_status(self, connection_id: str) -> ConnectionStatus:
        return DBConnectionRepository(self.metadata_db).get_status(connection_id)

    def test_connection(self, connection_id: str) -> ConnectionTestResult:
        repo = DBConnectionRepository(self.metadata_db)
        config = repo.get_config(connection_id)
        if config is None:
            raise KeyError(f"Connection {connection_id} not found")

        connector = self._build_connector(config)
        started_at = perf_counter()
        try:
            is_online = connector.test_connection()
        except Exception:
            repo.set_online_status(connection_id, False)
            raise

        repo.set_online_status(connection_id, is_online)
        if is_online:
            self._connectors[connection_id] = connector
            message = "Connection is reachable."
        else:
            message = "Connection test failed."
        return ConnectionTestResult(
            connection_id=connection_id,
            success=is_online,
            message=message,
            latency_ms=(perf_counter() - started_at) * 1000.0,
        )

    def delete_connection(self, connection_id: str) -> bool:
        connector = self._connectors.pop(connection_id, None)
        if connector is not None:
            connector.close()
        from app.db.repositories.rag_repo import RAGIndexRepository

        deleted = DBConnectionRepository(self.metadata_db).delete(connection_id)
        RAGIndexRepository(self.metadata_db).delete_state(connection_id)
        return deleted

    def get_connector(self, connection_id: str) -> DBConnector:
        if connection_id in self._connectors:
            return self._connectors[connection_id]
        repo = DBConnectionRepository(self.metadata_db)
        config = repo.get_config(connection_id)
        if config is None:
            raise KeyError(f"Connection {connection_id} not found")
        connector = self._build_connector(config)
        self._connectors[connection_id] = connector
        return connector

    def refresh_schema_cache(self, connection_id: str) -> SchemaCacheEntry:
        connector = self.get_connector(connection_id)
        repo = DBConnectionRepository(self.metadata_db)
        from app.db.repositories.rag_repo import RAGIndexRepository
        from app.schemas.rag import RAGHealthStatus, RAGIndexMode, RAGIndexStatus
        from hashlib import sha256
        import json

        tables = connector.get_schema()
        repo.set_online_status(connection_id, True)
        schema_cache = repo.upsert_schema_cache(connection_id, tables)
        rag_repo = RAGIndexRepository(self.metadata_db)
        schema_version = sha256(
            json.dumps([table.model_dump(mode="json") for table in tables], ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        rag_repo.upsert_state(
            connection_id=connection_id,
            schema_version=schema_version,
            index_status=RAGIndexStatus.PENDING,
            health_status=RAGHealthStatus.DEGRADED,
            index_mode=RAGIndexMode.HYBRID,
            is_indexed=False,
            table_count=len(tables),
            vector_count=len(tables),
        )
        return schema_cache

    def get_schema_cache(self, connection_id: str) -> SchemaCacheEntry | None:
        return DBConnectionRepository(self.metadata_db).get_schema_cache(connection_id)

    def _build_connector(self, config: ConnectionConfig) -> DBConnector:
        if config.db_type == "postgresql":
            return PostgreSQLConnector(config)
        if config.db_type == "mysql":
            return MySQLConnector(config)
        if config.db_type == "sqlite":
            return SQLiteConnector(config)
        raise ValueError(f"Unsupported database type: {config.db_type}")
