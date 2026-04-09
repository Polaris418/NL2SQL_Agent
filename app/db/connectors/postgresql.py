from urllib.parse import quote_plus

from app.db.connectors.base import DBConnector


class PostgreSQLConnector(DBConnector):
    def build_url(self) -> str:
        password = quote_plus(self.config.password or "")
        return (
            f"postgresql+psycopg://{self.config.username}:{password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
        )
