from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

from app.agent.utils import (
    build_user_friendly_error,
    categorize_error_message,
    extract_sql_table_references,
    normalize_identifier,
    sanitize_error_message,
)
from app.rag.concurrency import (
    ConcurrencyLimitExceeded,
    OperationKind,
    OperationTimeoutError,
    RetrievalConcurrencyError,
)
from app.rag.input_validation import InputValidationError
from app.rag.vector_store import VectorStoreTimeoutError, VectorStoreUnavailableError


_TIMEOUT_TOKENS = ("timeout", "timed out", "deadline exceeded", "time limit", "wait_for")
_RETRIEVAL_TOKENS = ("retriev", "recall", "search", "vector store", "bm25", "schema", "rank")
_EMBEDDING_TOKENS = ("embed", "embedding", "sentence-transformer", "vectorize")
_RERANKER_TOKENS = ("rerank", "cross encoder", "cross-encoder", "reranker")
_VECTOR_STORE_TOKENS = ("chroma", "vector store", "collection", "persist", "embeddings")
_RECALL_TOKENS = (
    "no candidate",
    "no matching table",
    "no relevant table",
    "recall error",
    "recall failed",
    "召回失败",
    "检索失败",
    "nothing found",
    "empty retrieval",
)
_MISMATCH_TOKENS = (
    "current database may not match",
    "database may not match this question",
    "database mismatch",
    "schema mismatch",
)
_NO_SUCH_TABLE_RE = re.compile(r"(?i)no such table[:\s]+([`\"'\w\.]+)")
_RELATION_NOT_EXIST_RE = re.compile(r"(?i)relation\s+[`\"']?([`\w\.]+)[`\"']?\s+does not exist")


@dataclass(slots=True)
class FailureClassification:
    error_type: str
    stage: str | None = None
    message: str = ""
    suggestion: str | None = None
    confidence: float = 0.5
    is_retrieval_failure: bool = False
    is_recall_error: bool = False
    matched_rules: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def category(self) -> str:
        return self.error_type

    @property
    def retryable(self) -> bool:
        return self.error_type in {
            "concurrency_limit",
            "retrieval_timeout",
            "embedding_timeout",
            "reranker_timeout",
            "timeout_error",
            "vector_store_unavailable",
            "retrieval_failure",
            "recall_error",
            "connection_error",
            "unknown",
        }

    @property
    def details(self) -> dict[str, Any]:
        return dict(self.metadata)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FailureClassifier:
    """Classify retrieval/runtime failures with lightweight remediation hints."""

    def classify_exception(
        self,
        error: Exception | str | None,
        *,
        stage: str | None = None,
        query: str | None = None,
        available_tables: Iterable[str] | None = None,
        candidate_tables: Iterable[str] | None = None,
        retrieved_tables: Iterable[str] | None = None,
    ) -> FailureClassification:
        return self.classify(
            error,
            stage=stage,
            query=query,
            available_tables=available_tables,
            candidate_tables=candidate_tables,
            retrieved_tables=retrieved_tables,
        )

    def classify_message(
        self,
        message: str | None,
        *,
        stage: str | None = None,
        query: str | None = None,
        available_tables: Iterable[str] | None = None,
        candidate_tables: Iterable[str] | None = None,
        retrieved_tables: Iterable[str] | None = None,
        default_category: str | None = None,
    ) -> FailureClassification:
        classification = self.classify(
            message,
            stage=stage,
            query=query,
            available_tables=available_tables,
            candidate_tables=candidate_tables,
            retrieved_tables=retrieved_tables,
        )
        if default_category and classification.error_type == "unknown":
            classification.error_type = default_category
        return classification

    def classify(
        self,
        error: Exception | str | None,
        *,
        stage: str | None = None,
        query: str | None = None,
        available_tables: Iterable[str] | None = None,
        candidate_tables: Iterable[str] | None = None,
        retrieved_tables: Iterable[str] | None = None,
    ) -> FailureClassification:
        error_type, message, suggestion, resolved_stage, rules, confidence = self._classify_core(
            error,
            stage=stage,
            query=query,
            available_tables=available_tables,
            candidate_tables=candidate_tables,
            retrieved_tables=retrieved_tables,
        )
        is_retrieval_failure = error_type in {
            "retrieval_failure",
            "recall_error",
            "database_mismatch",
            "retrieval_timeout",
            "embedding_timeout",
            "reranker_timeout",
            "vector_store_unavailable",
            "concurrency_limit",
        }
        is_recall_error = error_type in {
            "recall_error",
            "database_mismatch",
            "table_not_found",
        }
        return FailureClassification(
            error_type=error_type,
            stage=resolved_stage,
            message=message,
            suggestion=suggestion,
            confidence=confidence,
            is_retrieval_failure=is_retrieval_failure,
            is_recall_error=is_recall_error,
            matched_rules=rules,
            metadata={
                "query": query,
                "available_tables": [normalize_identifier(item) for item in available_tables or []],
                "candidate_tables": [normalize_identifier(item) for item in candidate_tables or []],
                "retrieved_tables": [normalize_identifier(item) for item in retrieved_tables or []],
            },
        )

    def _classify_core(
        self,
        error: Exception | str | None,
        *,
        stage: str | None = None,
        query: str | None = None,
        available_tables: Iterable[str] | None = None,
        candidate_tables: Iterable[str] | None = None,
        retrieved_tables: Iterable[str] | None = None,
    ) -> tuple[str, str, str | None, str | None, list[str], float]:
        available_set = {normalize_identifier(item) for item in available_tables or [] if normalize_identifier(item)}
        candidate_set = {normalize_identifier(item) for item in candidate_tables or [] if normalize_identifier(item)}
        retrieved_set = {normalize_identifier(item) for item in retrieved_tables or [] if normalize_identifier(item)}
        normalized_query = (query or "").strip()
        raw_message, exception_stage = self._extract_message_and_stage(error)
        resolved_stage = (stage or exception_stage or "").lower() or None
        sanitized = sanitize_error_message(raw_message)
        lower = sanitized.lower()
        if not sanitized:
            sanitized = "The request could not be completed."
            lower = sanitized.lower()

        rules: list[str] = []
        confidence = 0.45

        if isinstance(error, VectorStoreUnavailableError):
            rules.append("vector_store_exception")
            return (
                "vector_store_unavailable",
                sanitized,
                "Fallback to BM25-only mode and rebuild or repair the vector index.",
                resolved_stage or "retrieval",
                rules,
                0.98,
            )

        if isinstance(error, VectorStoreTimeoutError):
            rules.append("vector_store_timeout")
            return (
                "timeout_error",
                sanitized,
                "Reduce top_k, trim schema context, or raise the retrieval timeout.",
                resolved_stage or "retrieval",
                rules,
                0.95,
            )

        if self._is_timeout(error, lower, resolved_stage):
            error_type = self._timeout_error_type(error, lower, resolved_stage)
            suggestion = self._timeout_suggestion(error_type)
            rules.append("timeout")
            return error_type, sanitized, suggestion, resolved_stage, rules, 0.9

        if isinstance(error, (ConcurrencyLimitExceeded, RetrievalConcurrencyError)) or "concurrency limit" in lower:
            rules.append("concurrency_limit")
            return (
                "concurrency_limit",
                sanitized,
                "Queue the request or increase the concurrency limit for this stage.",
                resolved_stage or getattr(error, "operation_kind", None) and str(getattr(error, "operation_kind")),
                rules,
                0.98,
            )

        if isinstance(error, InputValidationError) or "input validation failed" in lower:
            rules.append("input_validation")
            return (
                "input_validation_failed",
                sanitized,
                "Shorten the query and remove risky SQL or prompt-injection content before retrying.",
                resolved_stage or "input_validation",
                rules,
                0.98,
            )

        if "access denied" in lower or "permission denied" in lower:
            rules.append("access_control")
            return (
                "access_denied",
                sanitized,
                "Use a connection or tenant scope with access to the required tables, or adjust the access-control rules.",
                resolved_stage or "authorization",
                rules,
                0.96,
            )

        if any(token in lower for token in _MISMATCH_TOKENS):
            rules.append("database_mismatch:explicit")
            return (
                "database_mismatch",
                sanitized,
                "Switch to a database that contains the required tables, or rephrase the question to match the current schema.",
                resolved_stage,
                rules,
                0.96,
            )

        if any(token in lower for token in _VECTOR_STORE_TOKENS) and any(token in lower for token in ("unavailable", "failed", "error", "connection")):
            rules.append("vector_store")
            return (
                "vector_store_unavailable",
                sanitized,
                "Fallback to BM25-only mode and rebuild or repair the vector index.",
                resolved_stage or "retrieval",
                rules,
                0.9,
            )

        observed_tables = self._extract_tables_from_message(raw_message)
        referenced_tables = set(observed_tables) | {normalize_identifier(item) for item in extract_sql_table_references(raw_message)}
        if candidate_set:
            referenced_tables |= candidate_set
        if retrieved_set:
            referenced_tables |= retrieved_set

        user_error_type, friendly_message, user_suggestion = build_user_friendly_error(raw_message)
        if user_error_type != "execution_error" or user_suggestion:
            rules.append(f"user_error:{user_error_type}")
            return (
                user_error_type,
                friendly_message,
                user_suggestion,
                resolved_stage,
                rules,
                0.88 if user_error_type != "execution_error" else 0.72,
            )

        if any(token in lower for token in _RECALL_TOKENS):
            rules.append("recall_token")
            return (
                "recall_error",
                sanitized,
                "Increase top_k, refresh the schema cache, or refine the query to use tables present in the current database.",
                resolved_stage or "retrieval",
                rules,
                0.95,
            )

        if referenced_tables and available_set:
            missing = {table for table in referenced_tables if table not in available_set}
            if missing and resolved_stage in {None, "retrieval", "schema_retrieval", "rewrite"}:
                rules.append("database_mismatch:missing_tables")
                return (
                    "database_mismatch",
                    sanitized,
                    "The current database may not match this question. Switch to a database with the needed tables or refresh the schema cache.",
                    resolved_stage or "retrieval",
                    rules,
                    0.92,
                )

        if resolved_stage in {"retrieval", "schema_retrieval", "rewrite"} and not (candidate_set or retrieved_set):
            rules.append("empty_retrieval")
            return (
                "recall_error",
                sanitized,
                "Increase top_k, refresh the schema index, or simplify the query so retrieval can find a relevant table.",
                resolved_stage,
                rules,
                0.84,
            )

        if resolved_stage in {"reranker", "ranking"} and any(token in lower for token in _RERANKER_TOKENS):
            rules.append("reranker_failure")
            return (
                "reranker_failure",
                sanitized,
                "Disable reranking temporarily or reduce the number of candidates to rerank.",
                resolved_stage,
                rules,
                0.82,
            )

        if resolved_stage in {"embedding"} and any(token in lower for token in _EMBEDDING_TOKENS):
            rules.append("embedding_failure")
            return (
                "embedding_failure",
                sanitized,
                "Use a lighter embedding model, precompute embeddings, or raise the embedding timeout.",
                resolved_stage,
                rules,
                0.82,
            )

        if resolved_stage in {"retrieval"} and any(token in lower for token in _RETRIEVAL_TOKENS):
            rules.append("retrieval_failure")
            return (
                "retrieval_failure",
                sanitized,
                "Check the schema index, increase top_k, or switch to BM25-only fallback if the vector store is degraded.",
                resolved_stage,
                rules,
                0.8,
            )

        if self._looks_like_sql_table_error(lower, available_set):
            rules.append("table_not_found")
            return (
                "table_not_found",
                sanitized,
                "Refresh schema cache and confirm the table name exists in the connected database.",
                resolved_stage,
                rules,
                0.87,
            )

        if normalized_query and available_set and self._looks_like_domain_mismatch(normalized_query, available_set):
            rules.append("query_domain_mismatch")
            return (
                "database_mismatch",
                sanitized,
                "The current database may not match this question. Switch to a database that contains matching business tables.",
                resolved_stage,
                rules,
                0.78,
            )

        rules.append("fallback")
        return (
            categorize_error_message(raw_message) if raw_message else "unknown",
            sanitized,
            "Inspect the retrieval logs, schema index status, and query rewrite output before retrying.",
            resolved_stage,
            rules,
            confidence,
        )

    @staticmethod
    def _extract_message_and_stage(error: Exception | str | None) -> tuple[str, str | None]:
        if error is None:
            return "", None
        if isinstance(error, OperationTimeoutError):
            return str(error), error.operation_kind.value
        if isinstance(error, ConcurrencyLimitExceeded):
            return str(error), error.operation_kind.value
        if isinstance(error, Exception):
            stage = getattr(error, "operation_kind", None) or getattr(error, "stage", None)
            if isinstance(stage, OperationKind):
                stage = stage.value
            return str(error), str(stage) if stage else None
        return str(error), None

    @staticmethod
    def _is_timeout(error: Exception | str | None, lower_message: str, stage: str | None) -> bool:
        if isinstance(error, OperationTimeoutError):
            return True
        if stage in {OperationKind.EMBEDDING.value, OperationKind.RERANKER.value, OperationKind.RETRIEVAL.value}:
            return any(token in lower_message for token in _TIMEOUT_TOKENS)
        return any(token in lower_message for token in _TIMEOUT_TOKENS)

    @staticmethod
    def _timeout_error_type(
        error: Exception | str | None,
        lower_message: str,
        stage: str | None,
    ) -> str:
        if isinstance(error, OperationTimeoutError):
            operation = error.operation_kind.value
            if operation == OperationKind.EMBEDDING.value:
                return "embedding_timeout"
            if operation == OperationKind.RERANKER.value:
                return "reranker_timeout"
            return "retrieval_timeout"
        if stage == OperationKind.EMBEDDING.value or any(token in lower_message for token in _EMBEDDING_TOKENS):
            return "embedding_timeout"
        if stage == OperationKind.RERANKER.value or any(token in lower_message for token in _RERANKER_TOKENS):
            return "reranker_timeout"
        if stage in {OperationKind.RETRIEVAL.value, "schema_retrieval", "retrieval"} or any(
            token in lower_message for token in _RETRIEVAL_TOKENS
        ):
            return "retrieval_timeout"
        return "timeout_error"

    @staticmethod
    def _timeout_suggestion(error_type: str) -> str:
        return {
            "embedding_timeout": "Use a lighter embedding model, precompute embeddings, or raise the embedding timeout.",
            "reranker_timeout": "Reduce the candidate count or disable reranking for very large retrieval sets.",
            "retrieval_timeout": "Reduce top_k, trim schema context, or raise the retrieval timeout.",
            "timeout_error": "Try narrowing the query scope or increasing the timeout limit.",
        }.get(error_type, "Try narrowing the query scope or increasing the timeout limit.")

    @staticmethod
    def _extract_tables_from_message(message: str) -> set[str]:
        tables: set[str] = set()
        if not message:
            return tables
        for regex in (_NO_SUCH_TABLE_RE, _RELATION_NOT_EXIST_RE):
            for match in regex.findall(message):
                normalized = normalize_identifier(match)
                if normalized:
                    tables.add(normalized)
        return tables

    @staticmethod
    def _looks_like_sql_table_error(lower_message: str, available_tables: set[str]) -> bool:
        if "no such table" in lower_message or "table not found" in lower_message:
            return True
        if "relation" in lower_message and "does not exist" in lower_message:
            return True
        if available_tables and "table" in lower_message and "not found" in lower_message:
            return True
        return False

    @staticmethod
    def _looks_like_domain_mismatch(query: str, available_tables: set[str]) -> bool:
        query_tokens = set(re.findall(r"[a-z0-9\u4e00-\u9fff]+", query.lower()))
        table_tokens = set()
        for table in available_tables:
            table_tokens.update(re.findall(r"[a-z0-9\u4e00-\u9fff]+", table.lower()))
        if not query_tokens:
            return False
        overlap = len(query_tokens & table_tokens)
        return overlap == 0
