from __future__ import annotations

from app.db.repositories.base import RepositoryBase
from app.db.security import PasswordCipher
from app.schemas.connection import ConnectionConfig, ConnectionStatus, SchemaCacheEntry, TableSchema


class DBConnectionRepository(RepositoryBase):
    def __init__(self, metadata_db, cipher: PasswordCipher | None = None):
        super().__init__(metadata_db)
        self.cipher = cipher or PasswordCipher.from_env()

    def create(self, config: ConnectionConfig, is_online: bool = False) -> ConnectionStatus:
        connection_id = self.metadata_db.path.stem + "_" + config.name.lower().replace(" ", "_")
        now = self.utcnow()
        db_type = getattr(config.db_type, "value", config.db_type)
        with self.metadata_db.connect() as conn:
            conn.execute(
                """
                INSERT INTO db_connections (
                    id, name, db_type, host, port, username, password_encrypted,
                    database_name, file_path, is_online, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    connection_id,
                    config.name,
                    db_type,
                    config.host,
                    config.port,
                    config.username,
                    self.cipher.encrypt(config.password),
                    config.database,
                    config.file_path,
                    int(is_online),
                    now,
                    now,
                ),
            )
        return self.get_status(connection_id)

    def list_all(self) -> list[ConnectionStatus]:
        rows = self.fetch_all("SELECT * FROM db_connections ORDER BY created_at DESC")
        return [self._to_status(row) for row in rows]

    def delete(self, connection_id: str) -> bool:
        with self.metadata_db.connect() as conn:
            conn.execute("DELETE FROM schema_cache WHERE connection_id = ?", (connection_id,))
            result = conn.execute("DELETE FROM db_connections WHERE id = ?", (connection_id,))
        return result.rowcount > 0

    def get_config(self, connection_id: str) -> ConnectionConfig | None:
        row = self.fetch_one("SELECT * FROM db_connections WHERE id = ?", (connection_id,))
        if row is None:
            return None
        return ConnectionConfig(
            name=row["name"],
            db_type=row["db_type"],
            host=row.get("host"),
            port=row.get("port"),
            username=row.get("username"),
            password=self.cipher.decrypt(row.get("password_encrypted")),
            database=row["database_name"],
            file_path=row.get("file_path"),
        )

    def get_status(self, connection_id: str) -> ConnectionStatus:
        row = self.fetch_one("SELECT * FROM db_connections WHERE id = ?", (connection_id,))
        if row is None:
            raise KeyError(f"Connection {connection_id} not found")
        return self._to_status(row)

    def set_online_status(self, connection_id: str, is_online: bool) -> None:
        with self.metadata_db.connect() as conn:
            conn.execute(
                "UPDATE db_connections SET is_online = ?, updated_at = ? WHERE id = ?",
                (int(is_online), self.utcnow(), connection_id),
            )

    def upsert_schema_cache(self, connection_id: str, tables: list[TableSchema]) -> SchemaCacheEntry:
        now = self.utcnow()
        payload_json = self.dumps([table.model_dump(mode="json") for table in tables])
        with self.metadata_db.connect() as conn:
            conn.execute("DELETE FROM schema_cache WHERE connection_id = ?", (connection_id,))
            conn.execute(
                "INSERT INTO schema_cache (connection_id, payload_json, updated_at) VALUES (?, ?, ?)",
                (connection_id, payload_json, now),
            )
            conn.execute(
                "UPDATE db_connections SET schema_updated_at = ?, updated_at = ? WHERE id = ?",
                (now, now, connection_id),
            )
        return SchemaCacheEntry(connection_id=connection_id, tables=tables, updated_at=now)

    def get_schema_cache(self, connection_id: str) -> SchemaCacheEntry | None:
        row = self.fetch_one("SELECT * FROM schema_cache WHERE connection_id = ?", (connection_id,))
        if row is None:
            return None
        return SchemaCacheEntry(
            connection_id=connection_id,
            tables=[TableSchema.model_validate(item) for item in self.loads(row["payload_json"], default=[])],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _to_status(row: dict) -> ConnectionStatus:
        return ConnectionStatus(
            id=row["id"],
            name=row["name"],
            db_type=row["db_type"],
            database=row["database_name"],
            host=row.get("host"),
            port=row.get("port"),
            username=row.get("username"),
            file_path=row.get("file_path"),
            is_online=bool(row.get("is_online")),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            schema_updated_at=row.get("schema_updated_at"),
        )
