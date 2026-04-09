from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable
import copy
import re


_IDENTIFIER_SPLIT_RE = re.compile(r"[^a-z0-9]+")
_CAMEL_CASE_RE = re.compile(r"(?<!^)(?=[A-Z])")
_CONTROL_VALUE_RE = re.compile(r"[\x00-\x1f\x7f-\x9f\u202a-\u202e\u2066-\u2069]")
_HEXISH_TOKEN_RE = re.compile(r"(?<!\w)(?:[A-Fa-f0-9]{16,}|[A-Za-z0-9/+_-]{24,})(?!\w)")
_SENSITIVE_KV_PATTERNS = (
    re.compile(
        r"(?i)\b(password|passwd|passphrase|pwd|secret|token|api[_-]?key|access[_-]?key|refresh[_-]?token|private[_-]?key|client[_-]?secret|auth[_-]?(?:token|secret|key))\b\s*[:=]\s*([^\s,;]+)"
    ),
    re.compile(
        r"(?i)\b(email|e[-_]?mail|phone|mobile|telephone|tel|ssn|social[_-]?security|national[_-]?id|id[_-]?card|passport|card(?:_?number)?|credit[_-]?card|bank[_-]?account|iban|routing[_-]?number|tax[_-]?id)\b\s*[:=]\s*([^\s,;]+)"
    ),
)


_CATEGORY_PATTERNS: dict[str, tuple[str, ...]] = {
    "credentials": (
        r"\bpassword\b",
        r"\bpassphrase\b",
        r"\bpasswd\b",
        r"\bpwd\b",
        r"\bsecret\b",
        r"\btoken\b",
        r"\bapi[_-]?key\b",
        r"\baccess[_-]?key\b",
        r"\brefresh[_-]?token\b",
        r"\bprivate[_-]?key\b",
        r"\bclient[_-]?secret\b",
        r"\bauth[_-]?(?:token|secret|key)\b",
        r"\bcredential(?:s)?\b",
    ),
    "pii": (
        r"\bemail\b",
        r"\be[-_]?mail\b",
        r"\bphone\b",
        r"\bmobile\b",
        r"\btelephone\b",
        r"\btel\b",
        r"\bssn\b",
        r"\bsocial[_-]?security\b",
        r"\bnational[_-]?id\b",
        r"\bid[_-]?card\b",
        r"\bpassport\b",
        r"\bcard(?:_?number)?\b",
        r"\bcredit[_-]?card\b",
        r"\bbank[_-]?account\b",
        r"\biban\b",
        r"\brouting[_-]?number\b",
        r"\btax[_-]?id\b",
    ),
    "session": (
        r"\bcookie\b",
        r"\bsession\b",
        r"\bcsrf\b",
        r"\bjwt\b",
        r"\bbearer\b",
        r"\boauth\b",
        r"\bcsrf[_-]?token\b",
    ),
}


def _split_identifier(value: str) -> list[str]:
    if not value:
        return []
    pieces: list[str] = []
    for segment in re.split(r"[_\-\s./:]+", value):
        if not segment:
            continue
        pieces.extend(part for part in _CAMEL_CASE_RE.split(segment) if part)
    return [piece.lower() for piece in pieces if piece]


def _join_identifier(value: str) -> str:
    return " ".join(_split_identifier(value))


def _get_attr(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _set_attr(item: Any, name: str, value: Any) -> None:
    if isinstance(item, dict):
        item[name] = value
    else:
        setattr(item, name, value)


@dataclass(slots=True)
class SensitiveFieldFinding:
    field_name: str
    category: str
    severity: str = "medium"
    matched_terms: list[str] = field(default_factory=list)
    score: int = 0
    source: str = "name"
    reason: str = ""
    should_redact: bool = True


@dataclass(slots=True)
class FieldAccessPolicy:
    """Simple field-level access control for schema docs and packed context."""

    allow_sensitive_values: bool = False
    allow_sensitive_comments: bool = False
    allow_sensitive_statistics: bool = True
    allowed_fields: set[str] = field(default_factory=set)
    blocked_fields: set[str] = field(default_factory=set)
    allowed_categories: set[str] = field(default_factory=lambda: {"technical", "operational"})

    def allows(self, finding: SensitiveFieldFinding | None, *, field_name: str | None = None) -> bool:
        if field_name and field_name in self.blocked_fields:
            return False
        if field_name and field_name in self.allowed_fields:
            return True
        if finding is None:
            return True
        if finding.category in self.allowed_categories:
            return True
        if finding.category in {"credentials", "session"}:
            return self.allow_sensitive_values
        if finding.category == "pii":
            return self.allow_sensitive_comments and self.allow_sensitive_values
        return self.allow_sensitive_values


@dataclass(slots=True)
class SensitiveFieldPolicy:
    min_score: int = 2
    redact_sample_values: bool = True
    redact_comments: bool = True
    redact_statistics: bool = False
    redaction_placeholder: str = "[REDACTED]"
    keep_visible_prefix: int = 2
    keep_visible_suffix: int = 2
    mask_char: str = "*"
    categories: dict[str, tuple[str, ...]] = field(default_factory=lambda: dict(_CATEGORY_PATTERNS))

    def analyze(self, field_name: str, comment: str | None = None) -> SensitiveFieldFinding | None:
        text = " ".join(part for part in (field_name, comment or "") if part).strip()
        normalized = _join_identifier(text)
        compact = re.sub(r"[^a-z0-9]+", "", text.lower())
        if not normalized:
            return None

        best: SensitiveFieldFinding | None = None
        for category, patterns in self.categories.items():
            for pattern in patterns:
                match = None
                matched_source = "name"
                for candidate, source in (
                    (text, "name"),
                    (normalized, "name"),
                    (compact, "name"),
                    (comment or "", "comment"),
                ):
                    if not candidate:
                        continue
                    match = re.search(pattern, candidate, flags=re.IGNORECASE)
                    if match:
                        matched_source = source
                        break
                if not match:
                    continue
                term = match.group(0)
                score = self._score(category, field_name, comment, term)
                if score < self.min_score:
                    continue
                finding = SensitiveFieldFinding(
                    field_name=field_name,
                    category=category,
                    severity=self._severity_for(category, score),
                    matched_terms=[term],
                    score=score,
                    source=matched_source,
                    reason=self._reason_for(category, term, field_name, comment),
                    should_redact=self._should_redact(category, score),
                )
                if best is None or finding.score > best.score:
                    best = finding
        return best

    # Compatibility alias for older call sites.
    def detect(self, field_name: str, *, comment: str | None = None, field_type: str | None = None) -> SensitiveFieldFinding | None:
        hint = " ".join(part for part in [comment or "", field_type or ""] if part).strip() or None
        return self.analyze(field_name, hint)

    def is_sensitive(self, field_name: str, comment: str | None = None) -> bool:
        return self.analyze(field_name, comment) is not None

    def find_sensitive_fields(self, fields: Iterable[Any]) -> list[SensitiveFieldFinding]:
        findings: list[SensitiveFieldFinding] = []
        for field in fields:
            name = str(_get_attr(field, "name", _get_attr(field, "field_name", "")) or "")
            comment = str(_get_attr(field, "comment", "") or "")
            finding = self.analyze(name, comment)
            if finding is not None:
                findings.append(finding)
        return findings

    def sanitize_value(
        self,
        value: Any,
        *,
        field_name: str | None = None,
        comment: str | None = None,
        finding: SensitiveFieldFinding | None = None,
    ) -> str:
        finding = finding or (self.analyze(field_name or "", comment) if field_name else None)
        text = "" if value is None else str(value)
        if finding is None or not finding.should_redact:
            return text

        category = finding.category
        if category in {"credentials", "session"}:
            return self.redaction_placeholder
        if category == "pii":
            if "@" in text and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", text):
                local, domain = text.split("@", 1)
                local = local[:1] + self.mask_char * max(2, min(6, len(local)))
                return f"{local}@{domain}"
            digits = re.sub(r"\D", "", text)
            if digits:
                return self.mask_char * max(4, max(0, len(digits) - 4)) + digits[-4:]
            return self.redaction_placeholder
        if category == "financial":
            digits = re.sub(r"\D", "", text)
            if digits:
                return self.mask_char * max(4, max(0, len(digits) - 4)) + digits[-4:]
            return self.redaction_placeholder
        return self._mask_generic(text)

    # Compatibility alias for older call sites.
    def redact_value(
        self,
        value: Any,
        *,
        field_name: str | None = None,
        comment: str | None = None,
        finding: SensitiveFieldFinding | None = None,
    ) -> str:
        return self.sanitize_value(value, field_name=field_name, comment=comment, finding=finding)

    def sanitize_sample_values(
        self,
        values: Iterable[Any],
        *,
        field_name: str | None = None,
        comment: str | None = None,
        finding: SensitiveFieldFinding | None = None,
        limit: int = 5,
    ) -> list[str]:
        finding = finding or (self.analyze(field_name or "", comment) if field_name else None)
        samples: list[str] = []
        for value in values:
            if len(samples) >= limit:
                break
            samples.append(self.sanitize_value(value, field_name=field_name, comment=comment, finding=finding))
        return list(dict.fromkeys(samples))

    def sanitize_text(self, text: str) -> str:
        if not text:
            return ""
        sanitized = _CONTROL_VALUE_RE.sub(" ", text)
        sanitized = _HEXISH_TOKEN_RE.sub(self.redaction_placeholder, sanitized)
        for pattern in _SENSITIVE_KV_PATTERNS:
            sanitized = pattern.sub(lambda match: f"{match.group(1)}={self.redaction_placeholder}", sanitized)
        sanitized = re.sub(r"\s+", " ", sanitized).strip()
        return sanitized

    # Compatibility alias for older call sites.
    def redact_text(self, text: str) -> str:
        return self.sanitize_text(text)

    def sanitize_field_documentation(self, field_doc: Any, *, access_policy: FieldAccessPolicy | None = None) -> Any:
        access_policy = access_policy or FieldAccessPolicy()
        field_name = str(_get_attr(field_doc, "name", _get_attr(field_doc, "field_name", "")) or "")
        comment = str(_get_attr(field_doc, "comment", "") or "")
        finding = self.analyze(field_name, comment)
        redacted = copy.deepcopy(field_doc)
        if finding is None:
            if isinstance(redacted, dict):
                redacted["sample_values"] = list(_get_attr(redacted, "sample_values", []) or [])
            return redacted

        if not access_policy.allows(finding, field_name=field_name):
            _set_attr(redacted, "sample_values", [])
            if not access_policy.allow_sensitive_comments:
                _set_attr(redacted, "comment", "")
            if not access_policy.allow_sensitive_statistics:
                for key in ("distinct_count", "null_ratio", "min_value", "max_value"):
                    if isinstance(redacted, dict):
                        redacted[key] = None
                    elif hasattr(redacted, key):
                        setattr(redacted, key, None)
            business_meaning = "Sensitive field"
            if finding.category:
                business_meaning = f"Sensitive field ({finding.category})"
            if isinstance(redacted, dict):
                redacted["business_meaning"] = business_meaning
            elif hasattr(redacted, "business_meaning"):
                setattr(redacted, "business_meaning", business_meaning)
            return redacted

        if self.redact_sample_values:
            samples = self.sanitize_sample_values(
                _get_attr(redacted, "sample_values", []) or [],
                field_name=field_name,
                comment=comment,
                finding=finding,
            )
            _set_attr(redacted, "sample_values", samples)
        if self.redact_comments and not access_policy.allow_sensitive_comments:
            _set_attr(redacted, "comment", "")
        if self.redact_statistics and not access_policy.allow_sensitive_statistics:
            for key in ("distinct_count", "null_ratio", "min_value", "max_value"):
                if isinstance(redacted, dict):
                    redacted[key] = None
                elif hasattr(redacted, key):
                    setattr(redacted, key, None)
        business_meaning = _get_attr(redacted, "business_meaning", "")
        if not business_meaning:
            business_meaning = f"Sensitive field ({finding.category})"
            if isinstance(redacted, dict):
                redacted["business_meaning"] = business_meaning
            elif hasattr(redacted, "business_meaning"):
                setattr(redacted, "business_meaning", business_meaning)
        return redacted

    def sanitize_table_documentation(
        self,
        table_doc: Any,
        *,
        access_policy: FieldAccessPolicy | None = None,
    ) -> Any:
        access_policy = access_policy or FieldAccessPolicy()
        redacted = copy.deepcopy(table_doc)
        columns = list(_get_attr(redacted, "columns", []) or [])
        sanitized_columns = [self.sanitize_field_documentation(column, access_policy=access_policy) for column in columns]
        _set_attr(redacted, "columns", sanitized_columns)
        table_name = str(_get_attr(redacted, "table_name", "") or "")
        summary = str(_get_attr(redacted, "business_summary", "") or "")
        if sanitized_columns and not summary:
            summary = f"Schema documentation for {table_name}"
            _set_attr(redacted, "business_summary", summary)
        metadata = dict(_get_attr(redacted, "metadata", {}) or {})
        sensitive_columns = [
            str(_get_attr(column, "name", _get_attr(column, "field_name", "")) or "")
            for column in columns
            if self.analyze(str(_get_attr(column, "name", _get_attr(column, "field_name", "")) or ""), str(_get_attr(column, "comment", "") or ""))
        ]
        metadata["sensitive_columns"] = sensitive_columns
        _set_attr(redacted, "metadata", metadata)
        return redacted

    def redact_schema_text(self, text: str, *, field_name: str | None = None, comment: str | None = None) -> str:
        finding = self.analyze(field_name or "", comment)
        if finding is None:
            return self.sanitize_text(text)
        sanitized = self.sanitize_text(text)
        if finding.category in {"credentials", "session"}:
            return re.sub(r"(?i)\b(sample|example|demo)?\s*value\s*[:=]\s*[^\s,;]+", f"value={self.redaction_placeholder}", sanitized)
        return sanitized

    def _mask_generic(self, value: str) -> str:
        if len(value) <= 4:
            return self.mask_char * max(3, len(value))
        prefix = value[: self.keep_visible_prefix]
        suffix = value[-self.keep_visible_suffix :] if self.keep_visible_suffix > 0 else ""
        middle = self.mask_char * max(4, len(value) - len(prefix) - len(suffix))
        return f"{prefix}{middle}{suffix}"

    def _score(self, category: str, field_name: str, comment: str | None, term: str) -> int:
        score = 1
        normalized_name = _join_identifier(field_name)
        normalized_comment = _join_identifier(comment or "")
        if term.lower() in normalized_name.lower():
            score += 2
        if comment and term.lower() in normalized_comment.lower():
            score += 1
        if category in {"credentials", "session"}:
            score += 2
        if term.lower().replace(" ", "_") == field_name.lower():
            score += 1
        return score

    def _severity_for(self, category: str, score: int) -> str:
        if score >= 5 or category in {"credentials", "session"}:
            return "high"
        if score >= 3:
            return "medium"
        return "low"

    def _reason_for(self, category: str, term: str, field_name: str, comment: str | None) -> str:
        if comment and term.lower() in _join_identifier(comment).lower():
            return f"Matched {category} keyword in column comment: {term}"
        return f"Matched {category} keyword in column name: {term}"

    def _should_redact(self, category: str, score: int) -> bool:
        return category in {"credentials", "session"} or score >= self.min_score


class SensitiveFieldSanitizer(SensitiveFieldPolicy):
    """Compatibility wrapper for orchestrator/factory integration."""


AccessControlPolicy = FieldAccessPolicy
DEFAULT_SENSITIVE_POLICY = SensitiveFieldPolicy()
DEFAULT_FIELD_ACCESS_POLICY = FieldAccessPolicy()
DEFAULT_SENSITIVE_SANITIZER = SensitiveFieldSanitizer()


def sanitize_table_documentation(table_doc: Any, *, policy: SensitiveFieldPolicy | None = None, access_policy: FieldAccessPolicy | None = None) -> Any:
    return (policy or DEFAULT_SENSITIVE_POLICY).sanitize_table_documentation(table_doc, access_policy=access_policy)


def sanitize_schema_context(text: str, *, policy: SensitiveFieldPolicy | None = None, field_name: str | None = None, comment: str | None = None) -> str:
    return (policy or DEFAULT_SENSITIVE_POLICY).redact_schema_text(text, field_name=field_name, comment=comment)


__all__ = [
    "AccessControlPolicy",
    "DEFAULT_FIELD_ACCESS_POLICY",
    "DEFAULT_SENSITIVE_POLICY",
    "DEFAULT_SENSITIVE_SANITIZER",
    "FieldAccessPolicy",
    "SensitiveFieldFinding",
    "SensitiveFieldPolicy",
    "SensitiveFieldSanitizer",
    "sanitize_schema_context",
    "sanitize_table_documentation",
]
