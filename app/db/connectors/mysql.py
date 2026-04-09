from urllib.parse import quote_plus

from app.db.connectors.base import DBConnector


class MySQLConnector(DBConnector):
    def build_url(self) -> str:
        password = quote_plus(self.config.password or "")
        return (
            f"mysql+pymysql://{self.config.username}:{password}"
            f"@{self.config.host}:{self.config.port}/{self.config.database}"
            f"?charset=utf8mb4"
        )
