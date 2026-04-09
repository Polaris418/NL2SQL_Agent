from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from .base import SchemaModel
from .pydantic_compat import Field, model_validator


class DatabaseType(str, Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


class ConnectionConfig(SchemaModel):
    name: str
    db_type: DatabaseType
    host: str | None = None
    port: int | None = None
    username: str | None = None
    password: str | None = None
    database: str
    file_path: str | None = None

    @model_validator(mode="after")
    def validate_required_fields(self):
        if self.db_type in {DatabaseType.POSTGRESQL, DatabaseType.MYSQL}:
            missing = [
                name
                for name in ("host", "port", "username", "password", "database")
                if getattr(self, name) in (None, "")
            ]
            if missing:
                raise ValueError(f"Missing required fields for {self.db_type.value}: {', '.join(missing)}")
        if self.db_type == DatabaseType.SQLITE and not (self.file_path or self.database):
            raise ValueError("SQLite connections require file_path or database")
        return self


class ConnectionCreate(ConnectionConfig):
    pass


class ColumnInfo(SchemaModel):
    name: str
    type: str
    nullable: bool = True
    default: str | None = None
    comment: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False


class TableSchema(SchemaModel):
    name: str
    comment: str | None = None
    columns: list[ColumnInfo] = Field(default_factory=list)
    description: str | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ConnectionStatus(SchemaModel):
    id: str
    name: str
    db_type: DatabaseType
    database: str
    host: str | None = None
    port: int | None = None
    username: str | None = None
    file_path: str | None = None
    is_online: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    schema_updated_at: datetime | None = None


class ConnectionTestResult(SchemaModel):
    connection_id: str
    success: bool
    message: str
    latency_ms: float = 0.0
    tested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SchemaCacheEntry(SchemaModel):
    connection_id: str
    tables: list[TableSchema] = Field(default_factory=list)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
