from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import re
import time

from app.db.connectors.base import DBConnector, DBConnectorError
from app.agent.utils import (
    build_user_friendly_error,
    contains_dangerous_keyword,
    sanitize_error_message,
    sanitize_sql,
)
from app.schemas.query import ExecutionResult, PaginationInfo


class SQLExecutor:
    _executor = ThreadPoolExecutor(max_workers=4)

    def __init__(self, default_limit: int = 1000, query_timeout_seconds: int = 30):
        self.default_limit = default_limit
        self.query_timeout_seconds = query_timeout_seconds

    def validate_sql(self, sql: str) -> str:
        normalized = sanitize_sql(sql)
        lowered = normalized.lower()
        if not lowered.startswith("select") and not lowered.startswith("with"):
            raise ValueError("Only SELECT statements are allowed")
        if contains_dangerous_keyword(lowered):
            raise ValueError("Dangerous SQL operations are not allowed")
        if ";" in normalized:
            raise ValueError("Only single-statement SQL is allowed")
        if "limit" not in lowered:
            normalized += f" LIMIT {self.default_limit}"
        return normalized + ";"

    def execute(
        self,
        connector: DBConnector,
        sql: str,
        *,
        page_number: int = 1,
        page_size: int | None = None,
        include_total_count: bool = False,
    ) -> ExecutionResult:
        safe_sql = self.validate_sql(sql)
        paginated_sql, pagination = self._paginate_sql(safe_sql, page_number=page_number, page_size=page_size)
        start = time.perf_counter()
        total_row_count = None
        try:
            future = self._executor.submit(connector.execute, paginated_sql)
            columns, rows = future.result(timeout=self.query_timeout_seconds)
            if include_total_count:
                count_sql = f"SELECT COUNT(*) AS total_count FROM ({safe_sql.rstrip(';')}) AS count_query;"
                count_future = self._executor.submit(connector.execute, count_sql)
                count_columns, count_rows = count_future.result(timeout=self.query_timeout_seconds)
                if count_rows:
                    count_row = count_rows[0]
                    total_row_count = int(count_row.get(count_columns[0], count_row.get("total_count", 0)) or 0)
        except DBConnectorError as exc:
            error_type, friendly_message, suggestion = build_user_friendly_error(str(exc))
            raise RuntimeError(f"{friendly_message} {suggestion or ''}".strip()) from exc
        except FuturesTimeoutError as exc:
            error_type, friendly_message, suggestion = build_user_friendly_error(f"Query timed out after {self.query_timeout_seconds} seconds")
            raise RuntimeError(f"{friendly_message} {suggestion or ''}".strip()) from exc
        latency_ms = (time.perf_counter() - start) * 1000
        rows = rows[: pagination.page_size]
        has_more = False
        if total_row_count is not None:
            has_more = total_row_count > pagination.offset + len(rows)
        else:
            has_more = len(rows) >= pagination.page_size
        return ExecutionResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            truncated=has_more,
            db_latency_ms=latency_ms,
            total_row_count=total_row_count,
            pagination=pagination.model_copy(update={"has_more": has_more, "total_row_count": total_row_count}),
            source_sql=paginated_sql,
        )

    def _paginate_sql(self, sql: str, *, page_number: int, page_size: int | None) -> tuple[str, PaginationInfo]:
        page_number = max(1, page_number)
        effective_page_size = max(1, page_size or self.default_limit)
        offset = (page_number - 1) * effective_page_size
        paginated_sql = (
            f"SELECT * FROM ({sql.rstrip(';')}) AS paginated_query "
            f"LIMIT {effective_page_size} OFFSET {offset};"
        )
        return paginated_sql, PaginationInfo(
            page_number=page_number,
            page_size=effective_page_size,
            offset=offset,
            has_more=False,
            total_row_count=None,
            applied_limit=effective_page_size,
        )

    @staticmethod
    def _sanitize_error(error_message: str) -> str:
        return sanitize_error_message(error_message)
