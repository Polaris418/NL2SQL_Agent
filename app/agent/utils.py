from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from io import StringIO
import csv
import logging
from typing import Any, Iterable, Sequence
import re

from .contracts import ColumnInfo, TableSchema, contains_any, normalize_whitespace, tokenize_text


TIME_PATTERNS: tuple[str, ...] = (
    "today",
    "yesterday",
    "last week",
    "last month",
    "last 7 days",
    "last 30 days",
    "recent",
    "近7天",
    "最近7天",
    "最近30天",
    "上个月",
    "本月",
    "今年",
    "昨日",
    "今日",
)

LOGGER = logging.getLogger(__name__)

AGGREGATE_HINTS = {
    "count": {"count", "number", "how many", "数量", "多少", "总数", "统计"},
    "sum": {"sum", "total", "amount", "金额", "总金额", "消费", "销售额"},
    "avg": {"avg", "average", "mean", "平均", "均值"},
    "max": {"max", "highest", "largest", "最大", "最高"},
    "min": {"min", "lowest", "smallest", "最小", "最低"},
}

DATE_HINTS = {"date", "time", "created_at", "updated_at", "datetime", "timestamp", "日期", "时间"}

NUMERIC_HINTS = {
    "int",
    "integer",
    "bigint",
    "smallint",
    "decimal",
    "numeric",
    "float",
    "double",
    "real",
    "money",
}


def column_type(column: Any) -> str:
    return str(getattr(column, "type", None) or getattr(column, "data_type", None) or "")


def column_comment(column: Any) -> str:
    return str(getattr(column, "comment", None) or "")


def column_foreign_table(column: Any) -> str | None:
    return getattr(column, "foreign_table", None) or getattr(column, "foreign_key_reference", None)


def table_name(table: Any) -> str:
    return str(getattr(table, "name", None) or getattr(table, "table_name", "") or "")


def table_comment(table: Any) -> str:
    return str(getattr(table, "comment", None) or getattr(table, "table_comment", None) or "")


def table_description(table: Any) -> str:
    return str(getattr(table, "description", None) or "")


def best_table_match(question: str, tables: Sequence[TableSchema]) -> TableSchema | None:
    if not tables:
        return None
    question_tokens = set(tokenize_text(question))
    best_score = -1
    best_table: TableSchema | None = None
    for table in tables:
        score = 0
        table_tokens = set(tokenize_text(table_name(table)))
        comment_tokens = set(tokenize_text(table_comment(table)))
        description_tokens = set(tokenize_text(table_description(table)))
        column_tokens = set()
        for column in getattr(table, "columns", []):
            column_tokens.update(tokenize_text(column.name))
            column_tokens.update(tokenize_text(column_comment(column)))
        score += len(question_tokens & table_tokens) * 6
        score += len(question_tokens & comment_tokens) * 3
        score += len(question_tokens & description_tokens) * 2
        score += len(question_tokens & column_tokens)
        if score > best_score:
            best_score = score
            best_table = table
    return best_table or tables[0]


def guess_numeric_columns(table: TableSchema) -> list[ColumnInfo]:
    result: list[ColumnInfo] = []
    for column in getattr(table, "columns", []):
        declared = column_type(column).lower()
        if any(hint in declared for hint in NUMERIC_HINTS):
            result.append(column)
    return result


def guess_datetime_columns(table: TableSchema) -> list[ColumnInfo]:
    result: list[ColumnInfo] = []
    for column in getattr(table, "columns", []):
        combined = f"{column.name} {column_comment(column)} {column_type(column)}".lower()
        if any(hint.lower() in combined for hint in DATE_HINTS):
            result.append(column)
    return result


def guess_dimension_columns(table: TableSchema) -> list[ColumnInfo]:
    dimensions: list[ColumnInfo] = []
    for column in getattr(table, "columns", []):
        declared = column_type(column).lower()
        if any(hint in declared for hint in NUMERIC_HINTS):
            continue
        if any(hint.lower() in column.name.lower() for hint in DATE_HINTS):
            continue
        dimensions.append(column)
    return dimensions


def detect_aggregate(question: str) -> str | None:
    normalized = question.lower()
    for name, hints in AGGREGATE_HINTS.items():
        if any(hint in normalized for hint in hints):
            return name
    return None


def detect_aggregate_metrics(question: str) -> list[str]:
    normalized = question.lower()
    metrics: list[str] = []
    for name, hints in AGGREGATE_HINTS.items():
        if any(hint in normalized for hint in hints):
            metrics.append(name)
    return metrics


def detect_time_filter(question: str) -> str | None:
    normalized = question.lower()
    for phrase in TIME_PATTERNS:
        if phrase.lower() in normalized:
            return phrase
    return None


def detect_limit(question: str, default: int = 1000) -> int:
    match = re.search(r"(top|前)\s*(\d+)", question.lower())
    if match:
        try:
            return max(1, min(default, int(match.group(2))))
        except ValueError:
            return default
    return default


def sanitize_sql(sql: str) -> str:
    text = normalize_whitespace(sql)
    text = re.sub(r"^sql\s*:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE)
    text = text.replace("```", "")
    return text.strip().rstrip(";")


def sanitize_error_message(error_message: str | None) -> str:
    if not error_message:
        return ""
    text = str(error_message)
    text = re.sub(r"(password|secret|token|api_key|apikey)=\S+", r"\1=***", text, flags=re.IGNORECASE)
    text = re.sub(r"(?:[A-Za-z]:)?[\\/][^\s'\"]+", "<path>", text)
    text = re.sub(r"https?://[^\s'\"]+", "<url>", text)
    text = re.sub(r"(?i)(from|join)\s+[^\s;]+", r"\1 <table>", text)
    text = re.sub(r"(?i)\bselect\b.*?(?=error|$)", "SELECT ...", text, count=1)
    return normalize_whitespace(text)[:500]


def categorize_error_message(error_message: str | None) -> str:
    lowered = (error_message or "").lower()
    if "current database may not match" in lowered or "database may not match this question" in lowered:
        return "database_mismatch"
    if any(token in lowered for token in ("timeout", "timed out")):
        return "timeout_error"
    if any(token in lowered for token in ("connect", "connection", "refused", "could not translate host")):
        return "connection_error"
    if "syntax" in lowered or "parse" in lowered:
        return "syntax_error"
    if "permission" in lowered or "access denied" in lowered or "unauthorized" in lowered:
        return "permission_error"
    if "no such table" in lowered or "table" in lowered and "not found" in lowered:
        return "table_not_found"
    if any(token in lowered for token in ("column", "no such column", "unknown column")):
        return "column_error"
    return "execution_error"


def error_suggestion(error_type: str) -> str | None:
    suggestions = {
        "database_mismatch": "Switch to a database that contains the required business tables, or ask a question that matches the current schema.",
        "connection_error": "Check the database host, port, username, password, and network connectivity.",
        "syntax_error": "Review the generated SQL for invalid syntax or unsupported functions.",
        "permission_error": "Verify the database account has permission to read the target tables.",
        "table_not_found": "Refresh schema cache and confirm the table name exists in the connected database.",
        "column_error": "Refresh schema cache and confirm the selected columns exist in the target table.",
        "timeout_error": "Try narrowing the query scope, adding filters, or increasing the timeout limit.",
        "execution_error": "Try a simpler query or refresh the schema cache before retrying.",
    }
    return suggestions.get(error_type)


def build_user_friendly_error(error_message: str | None) -> tuple[str, str, str | None]:
    error_type = categorize_error_message(error_message)
    clean_message = sanitize_error_message(error_message) or "The request could not be completed."
    friendly = {
        "database_mismatch": "The current database may not match this question.",
        "connection_error": "Database connection failed.",
        "syntax_error": "The SQL could not be parsed.",
        "permission_error": "The database user does not have permission to run this query.",
        "table_not_found": "The requested table was not found.",
        "column_error": "The requested column was not found.",
        "timeout_error": "The query took too long to finish.",
        "execution_error": "The query failed while running on the database.",
    }.get(error_type, "The request could not be completed.")
    if clean_message and clean_message != friendly:
        friendly = f"{friendly} Details: {clean_message}"
    return error_type, friendly, error_suggestion(error_type)


def normalize_identifier(value: str) -> str:
    return value.strip().strip("`").strip('"').strip("'").split(".")[-1].lower()


def extract_sql_table_references(sql: str) -> list[str]:
    if not sql:
        return []
    matches = re.findall(r"(?i)\b(?:from|join)\s+([`\"'\w\.]+)", sql)
    seen: list[str] = []
    for match in matches:
        normalized = normalize_identifier(match)
        if normalized and normalized not in seen:
            seen.append(normalized)
    return seen


def detect_database_mismatch(
    *,
    question: str,
    schema_tables: Sequence[Any],
    sql: str = "",
    error_message: str | None = None,
) -> tuple[bool, str | None]:
    available_tables = {normalize_identifier(table_name(table)) for table in schema_tables if table_name(table)}
    if not available_tables:
        return False, None

    referenced_tables = [table for table in extract_sql_table_references(sql) if table]
    unknown_tables = [table for table in referenced_tables if table not in available_tables]
    lowered_error = (error_message or "").lower()

    if any(token in lowered_error for token in ("timeout", "timed out", "retrieval timeout", "queue timeout", "concurrency")):
        return False, None

    if unknown_tables and (
        not error_message
        or "no such table" in lowered_error
        or ("table" in lowered_error and "not found" in lowered_error)
    ):
        available_preview = ", ".join(sorted(available_tables)[:6])
        missing_preview = ", ".join(unknown_tables[:3])
        message = (
            "The current database may not match this question. "
            f"The generated SQL referenced table(s) [{missing_preview}], "
            f"but the current database only exposes tables such as [{available_preview}]."
        )
        return True, message

    if referenced_tables and not unknown_tables:
        return False, None

    question_tokens = set(tokenize_text(question))
    schema_tokens: set[str] = set()
    for table in schema_tables:
        schema_tokens.update(tokenize_text(table_name(table)))
        schema_tokens.update(tokenize_text(table_comment(table)))
        schema_tokens.update(tokenize_text(table_description(table)))
    overlap = question_tokens & schema_tokens
    if question_tokens and len(overlap) == 0 and len(available_tables) > 0:
        message = (
            "The current database may not match this question. "
            "The question keywords do not match the current schema very well."
        )
        return True, message

    return False, None


def log_exception_details(message: str, *, exc: Exception | None = None, extra: dict[str, Any] | None = None) -> None:
    payload = extra or {}
    if exc is not None:
        LOGGER.exception("%s | extra=%s", message, payload, exc_info=exc)
    else:
        LOGGER.error("%s | extra=%s", message, payload)


def contains_dangerous_keyword(sql: str) -> bool:
    lower = sql.lower()
    dangerous = ("insert", "update", "delete", "drop", "alter", "truncate", "grant", "revoke", "replace")
    return any(re.search(rf"\b{keyword}\b", lower) for keyword in dangerous)


def extract_identifier_candidates(text: str) -> list[str]:
    raw_tokens = tokenize_text(text)
    candidates = []
    for token in raw_tokens:
        if len(token) >= 2 and token not in candidates:
            candidates.append(token)
    return candidates


def render_table_brief(table: TableSchema) -> str:
    columns = ", ".join(column.name for column in getattr(table, "columns", [])[:12]) or "no columns"
    return f"{table_name(table)} ({columns})"


def score_text_overlap(left: str, right: str) -> int:
    left_tokens = set(tokenize_text(left))
    right_tokens = set(tokenize_text(right))
    return len(left_tokens & right_tokens)


def sample_rows_text(rows: Sequence[Sequence[Any]], limit: int = 3) -> list[str]:
    result: list[str] = []
    for row in rows[:limit]:
        result.append(" | ".join("" if value is None else str(value) for value in row))
    return result


def infer_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def build_follow_up_prompt_context(
    question: str,
    previous_question: str | None = None,
    previous_sql: str | None = None,
    previous_tables: Sequence[str] | None = None,
    instruction: str | None = None,
) -> str:
    parts = [f"Question: {question.strip()}"]
    if instruction:
        parts.append(f"Follow-up instruction: {instruction.strip()}")
    if previous_question:
        parts.append(f"Previous question: {previous_question.strip()}")
    if previous_sql:
        parts.append(f"Previous SQL: {sanitize_sql(previous_sql)}")
    if previous_tables:
        tables = ", ".join(table for table in previous_tables if table)
        if tables:
            parts.append(f"Previous tables: {tables}")
    return "\n".join(parts)


def execution_rows_to_csv(columns: Sequence[str], rows: Sequence[dict[str, Any]]) -> str:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(columns))
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column) for column in columns})
    return buffer.getvalue()


def stable_hash(parts: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update((part or "").encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def schema_fingerprint(tables: Sequence[Any]) -> str:
    parts: list[str] = []
    for table in tables:
        parts.append(table_name(table))
        parts.append(table_comment(table))
        parts.append(table_description(table))
        for column in getattr(table, "columns", []):
            parts.append(getattr(column, "name", ""))
            parts.append(column_type(column))
            parts.append(column_comment(column))
            parts.append("pk" if getattr(column, "is_primary_key", False) else "nopk")
            parts.append("fk" if getattr(column, "is_foreign_key", False) else "nofk")
    return stable_hash(parts)


def query_cache_key(
    *,
    question: str,
    connection_id: str,
    schema_fp: str,
    db_type: str = "",
    page_number: int = 1,
    page_size: int = 1000,
    include_total_count: bool = False,
    follow_up_instruction: str | None = None,
    previous_query_id: str | None = None,
) -> str:
    parts = [
        connection_id,
        question.strip(),
        schema_fp,
        db_type.lower(),
        str(page_number),
        str(page_size),
        "1" if include_total_count else "0",
        follow_up_instruction or "",
        previous_query_id or "",
    ]
    return stable_hash(parts)


def log_query_audit(message: str, *, extra: dict[str, Any] | None = None) -> str:
    audit_id = stable_hash([message, json.dumps(extra or {}, sort_keys=True, ensure_ascii=False, default=str)])
    LOGGER.info("audit_id=%s %s | extra=%s", audit_id, message, extra or {})
    return audit_id
