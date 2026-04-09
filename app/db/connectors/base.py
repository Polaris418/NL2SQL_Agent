from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.schemas.connection import ColumnInfo, ConnectionConfig, TableSchema


class DBConnectorError(RuntimeError):
    pass


class DBConnector(ABC):
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._engine: Engine | None = None

    @abstractmethod
    def build_url(self) -> str:
        raise NotImplementedError

    def connect(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.build_url(), pool_pre_ping=True)
        return self._engine

    @contextmanager
    def session(self):
        with self.connect().connect() as connection:
            yield connection

    def test_connection(self) -> bool:
        try:
            with self.session() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as exc:
            raise DBConnectorError(str(exc)) from exc

    def execute(self, sql: str) -> tuple[list[str], list[dict[str, Any]]]:
        try:
            with self.session() as connection:
                result = connection.execute(text(sql))
                rows = [dict(row._mapping) for row in result]
                return list(result.keys()), rows
        except SQLAlchemyError as exc:
            raise DBConnectorError(str(exc)) from exc

    def get_schema(self) -> list[TableSchema]:
        try:
            inspector = inspect(self.connect())
            tables: list[TableSchema] = []
            for table_name in inspector.get_table_names():
                pk = set(inspector.get_pk_constraint(table_name).get("constrained_columns", []))
                fk_columns = {
                    column
                    for fk in inspector.get_foreign_keys(table_name)
                    for column in fk.get("constrained_columns", [])
                }
                columns = [
                    ColumnInfo(
                        name=column["name"],
                        type=str(column["type"]),
                        nullable=bool(column.get("nullable", True)),
                        default=None if column.get("default") is None else str(column["default"]),
                        comment=column.get("comment"),
                        is_primary_key=column["name"] in pk,
                        is_foreign_key=column["name"] in fk_columns,
                    )
                    for column in inspector.get_columns(table_name)
                ]
                tables.append(
                    TableSchema(
                        name=table_name,
                        comment=self._get_table_comment(inspector, table_name),
                        columns=columns,
                    )
                )
            return tables
        except SQLAlchemyError as exc:
            raise DBConnectorError(str(exc)) from exc

    def _get_table_comment(self, inspector, table_name: str) -> str | None:
        try:
            return inspector.get_table_comment(table_name).get("text")
        except Exception:
            return None

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
