from pathlib import Path

from app.db.connectors.base import DBConnector


class SQLiteConnector(DBConnector):
    def build_url(self) -> str:
        file_path = self.config.file_path or self.config.database
        return f"sqlite:///{Path(file_path).expanduser().resolve()}"
