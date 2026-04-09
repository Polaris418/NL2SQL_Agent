from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import re


_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\u202a-\u202e\u2066-\u2069]")
_MULTI_SPACE_RE = re.compile(r"[ \t]+")
_BLANK_LINES_RE = re.compile(r"\n{3,}")
_PROMPT_INJECTION_PATTERNS: tuple[tuple[str, re.Pattern[str], str, int], ...] = (
    (
        "prompt_injection",
        re.compile(r"(?i)\b(?:ignore|disregard|bypass|override)\b.{0,40}\b(?:previous|above|all)\b.{0,40}\b(?:instruction|prompt|rule|system)\b"),
        "high",
        5,
    ),
    ("prompt_injection", re.compile(r"(?i)\b(?:system prompt|developer mode|jailbreak|dan mode|do anything now)\b"), "high", 5),
)
_SQL_COMMENT_PATTERNS: tuple[tuple[str, re.Pattern[str], str, int], ...] = (
    ("sql_comment", re.compile(r"(--|/\*|\*/)", re.MULTILINE), "high", 4),
    ("sql_union", re.compile(r"(?i)\bunion\b\s+\bselect\b"), "high", 5),
    ("sql_exec", re.compile(r"(?i)\b(?:xp_cmdshell|benchmark|sleep|load_file|information_schema)\b"), "high", 4),
)
_SQL_STATEMENT_PATTERNS: tuple[tuple[str, re.Pattern[str], str, int], ...] = (
    ("drop_table", re.compile(r"(?i)\bdrop\s+table\b"), "high", 5),
    ("drop_database", re.compile(r"(?i)\bdrop\s+database\b"), "high", 5),
    ("drop_view", re.compile(r"(?i)\bdrop\s+view\b"), "high", 5),
    ("drop_index", re.compile(r"(?i)\bdrop\s+index\b"), "high", 5),
    ("delete_from", re.compile(r"(?i)\bdelete\s+from\b"), "high", 5),
    ("insert_into", re.compile(r"(?i)\binsert\s+into\b"), "high", 5),
    ("update_set", re.compile(r"(?i)\bupdate\s+\w+\s+set\b"), "high", 5),
    ("alter_table", re.compile(r"(?i)\balter\s+table\b"), "high", 5),
    ("truncate_table", re.compile(r"(?i)\btruncate\s+table\b"), "high", 5),
    ("grant_privilege", re.compile(r"(?i)\bgrant\b.{0,40}\bon\b"), "high", 4),
    ("revoke_privilege", re.compile(r"(?i)\brevoke\b.{0,40}\bon\b"), "high", 4),
)


@dataclass(slots=True)
class ValidationIssue:
    code: str
    message: str
    severity: str = "high"
    matched_text: str | None = None
    suggestion: str | None = None


@dataclass(slots=True)
class ValidationResult:
    kind: str
    original_text: str
    normalized_text: str
    sanitized_text: str
    issues: list[ValidationIssue] = field(default_factory=list)
    truncated: bool = False
    risk_score: int = 0
    max_length: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    valid: bool = True

    @property
    def is_valid(self) -> bool:
        return self.valid

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "original_text": self.original_text,
            "normalized_text": self.normalized_text,
            "sanitized_text": self.sanitized_text,
            "issues": [
                {
                    "code": issue.code,
                    "message": issue.message,
                    "severity": issue.severity,
                    "matched_text": issue.matched_text,
                    "suggestion": issue.suggestion,
                }
                for issue in self.issues
            ],
            "truncated": self.truncated,
            "risk_score": self.risk_score,
            "max_length": self.max_length,
            "metadata": dict(self.metadata),
            "valid": self.valid,
        }


class InputValidationError(ValueError):
    def __init__(self, result: ValidationResult):
        self.result = result
        self.stage = "input_validation"
        super().__init__(self._build_message(result))

    @staticmethod
    def _build_message(result: ValidationResult) -> str:
        if result.issues:
            return "; ".join(issue.message for issue in result.issues)
        return "Input validation failed"


@dataclass(slots=True)
class InputValidationConfig:
    max_query_length: int = 2048
    max_schema_length: int = 12000
    max_context_length: int = 8000
    reject_risky_input: bool = True
    strip_control_chars: bool = True
    collapse_whitespace: bool = True
    preserve_newlines_for_schema: bool = True
    max_issues: int = 12


class InputValidator:
    """Validate and sanitize user query / schema text before it enters prompts."""

    def __init__(self, config: InputValidationConfig | None = None):
        self.config = config or InputValidationConfig()

    def validate_query(self, text: str, *, max_length: int | None = None, raise_on_error: bool = False) -> ValidationResult:
        return self.validate(text, kind="query", max_length=max_length or self.config.max_query_length, raise_on_error=raise_on_error)

    def validate_schema_text(self, text: str, *, max_length: int | None = None, raise_on_error: bool = False) -> ValidationResult:
        return self.validate(text, kind="schema", max_length=max_length or self.config.max_schema_length, raise_on_error=raise_on_error)

    def validate_context_text(self, text: str, *, max_length: int | None = None, raise_on_error: bool = False) -> ValidationResult:
        return self.validate(text, kind="context", max_length=max_length or self.config.max_context_length, raise_on_error=raise_on_error)

    def validate(
        self,
        text: str,
        *,
        kind: str = "query",
        max_length: int | None = None,
        raise_on_error: bool = False,
    ) -> ValidationResult:
        original_text = "" if text is None else str(text)
        preserve_newlines = kind in {"schema", "context"} and self.config.preserve_newlines_for_schema
        normalized = self.normalize_text(original_text, preserve_newlines=preserve_newlines)
        sanitized = self.sanitize_text(original_text, kind=kind, max_length=max_length)
        issues, risk_score = self._detect_issues(normalized, kind=kind, max_length=max_length)
        valid = not any(issue.severity == "high" for issue in issues)
        result = ValidationResult(
            kind=kind,
            original_text=original_text,
            normalized_text=normalized,
            sanitized_text=sanitized,
            issues=issues[: self.config.max_issues],
            truncated=len(sanitized) < len(normalized),
            risk_score=risk_score,
            max_length=max_length or self._default_max_length(kind),
            metadata={
                "original_length": len(original_text),
                "normalized_length": len(normalized),
                "sanitized_length": len(sanitized),
            },
            valid=valid,
        )
        if raise_on_error and not result.is_valid:
            raise InputValidationError(result)
        return result

    def sanitize_text(self, text: str, *, kind: str = "query", max_length: int | None = None) -> str:
        preserve_newlines = kind in {"schema", "context"} and self.config.preserve_newlines_for_schema
        normalized = self.normalize_text(text, preserve_newlines=preserve_newlines)
        max_length = max_length or self._default_max_length(kind)
        if len(normalized) <= max_length:
            return normalized
        if preserve_newlines:
            return self._truncate_preserving_lines(normalized, max_length)
        return normalized[:max_length].rstrip()

    def normalize_text(self, text: str, *, preserve_newlines: bool = False) -> str:
        text = "" if text is None else str(text)
        if self.config.strip_control_chars:
            text = _CONTROL_CHARS_RE.sub(" ", text)
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        if preserve_newlines:
            lines = []
            for raw_line in text.split("\n"):
                line = raw_line.strip()
                if self.config.collapse_whitespace:
                    line = _MULTI_SPACE_RE.sub(" ", line)
                if line:
                    lines.append(line)
            normalized = "\n".join(lines)
            normalized = _BLANK_LINES_RE.sub("\n\n", normalized)
            return normalized.strip()
        normalized = text.replace("\n", " ")
        if self.config.collapse_whitespace:
            normalized = _MULTI_SPACE_RE.sub(" ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def validate_pair(
        self,
        query_text: str,
        *,
        schema_text: str | None = None,
        context_text: str | None = None,
    ) -> dict[str, ValidationResult]:
        results = {"query": self.validate_query(query_text)}
        if schema_text is not None:
            results["schema"] = self.validate_schema_text(schema_text)
        if context_text is not None:
            results["context"] = self.validate_context_text(context_text)
        return results

    def _detect_issues(
        self,
        text: str,
        *,
        kind: str,
        max_length: int | None,
    ) -> tuple[list[ValidationIssue], int]:
        issues: list[ValidationIssue] = []
        risk_score = 0
        limit = max_length or self._default_max_length(kind)

        if len(text) > limit:
            issues.append(
                ValidationIssue(
                    code="length_exceeded",
                    message=f"{kind} text exceeds the maximum length of {limit} characters.",
                    severity="high",
                    matched_text=text[:80],
                    suggestion="Trim the text before passing it into the retrieval or generation pipeline.",
                )
            )
            risk_score += 5

        for code, pattern, severity, score in _PROMPT_INJECTION_PATTERNS:
            match = pattern.search(text)
            if match:
                issues.append(
                    ValidationIssue(
                        code=code,
                        message="Prompt injection style instruction detected.",
                        severity=severity,
                        matched_text=match.group(0),
                        suggestion="Remove instructions that ask the model to ignore rules or reveal prompts.",
                    )
                )
                risk_score += score

        for code, pattern, severity, score in _SQL_COMMENT_PATTERNS:
            match = pattern.search(text)
            if match:
                issues.append(
                    ValidationIssue(
                        code=code,
                        message="SQL comment or execution primitive detected in plain text input.",
                        severity=severity,
                        matched_text=match.group(0),
                        suggestion="Strip SQL comments and execution primitives from user input.",
                    )
                )
                risk_score += score

        for code, pattern, severity, score in _SQL_STATEMENT_PATTERNS:
            match = pattern.search(text)
            if match:
                issues.append(
                    ValidationIssue(
                        code=code,
                        message="Potential SQL statement detected inside free-form input.",
                        severity=severity,
                        matched_text=match.group(0),
                        suggestion="Only pass natural language questions or schema text into the RAG pipeline.",
                    )
                )
                risk_score += score

        if ";" in text and re.search(r"(?i)\b(?:select|insert|update|delete|drop|alter|truncate|grant|revoke)\b", text):
            issues.append(
                ValidationIssue(
                    code="multiple_statements",
                    message="Multiple SQL statements may be embedded in the input.",
                    severity="high",
                    matched_text=";",
                    suggestion="Keep the input to a single natural language question or schema description.",
                )
            )
            risk_score += 4

        if _CONTROL_CHARS_RE.search(text):
            issues.append(
                ValidationIssue(
                    code="control_chars",
                    message="Control characters were present in the input.",
                    severity="medium",
                    matched_text="control character",
                    suggestion="Remove control characters and zero-width direction overrides.",
                )
            )
            risk_score += 1

        if len(issues) > self.config.max_issues:
            issues = issues[: self.config.max_issues]
        return issues, risk_score

    def _default_max_length(self, kind: str) -> int:
        if kind == "schema":
            return self.config.max_schema_length
        if kind == "context":
            return self.config.max_context_length
        return self.config.max_query_length

    def _truncate_preserving_lines(self, text: str, max_length: int) -> str:
        if len(text) <= max_length:
            return text
        lines = text.split("\n")
        kept: list[str] = []
        current = 0
        for line in lines:
            candidate_len = len(line) + (1 if kept else 0)
            if current + candidate_len > max_length:
                break
            kept.append(line)
            current += candidate_len
        if kept:
            return "\n".join(kept).rstrip()
        return text[:max_length].rstrip()


DEFAULT_INPUT_VALIDATOR = InputValidator()

# Backward-compatible aliases used by the rest of the RAG pipeline.
ValidationConfig = InputValidationConfig


def validate_input(
    text: str,
    *,
    kind: str = "query",
    max_length: int | None = None,
    config: InputValidationConfig | None = None,
    raise_on_error: bool = False,
) -> ValidationResult:
    validator = InputValidator(config)
    if kind == "schema":
        result = validator.validate_schema_text(text, max_length=max_length)
    elif kind == "context":
        result = validator.validate_context_text(text, max_length=max_length)
    else:
        result = validator.validate_query(text, max_length=max_length)
    if raise_on_error and not result.is_valid:
        raise InputValidationError(result)
    return result


__all__ = [
    "DEFAULT_INPUT_VALIDATOR",
    "InputValidationConfig",
    "InputValidationError",
    "InputValidator",
    "ValidationConfig",
    "ValidationIssue",
    "ValidationResult",
    "validate_input",
]
