from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import json
import re

from app.agent.utils import stable_hash


DEFAULT_SYNONYM_PATH = Path(__file__).resolve().parents[2] / "config" / "synonyms.json"


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _normalize_mapping(raw_mapping: dict[str, Any]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for canonical, synonyms in raw_mapping.items():
        canonical_key = str(canonical).strip()
        if not canonical_key:
            continue
        normalized[canonical_key] = _coerce_str_list(synonyms)
    return normalized


def _replace_variant(text: str, variant: str, canonical: str) -> str:
    if not variant or variant == canonical:
        return text
    if re.search(r"[A-Za-z0-9_]", variant):
        pattern = re.compile(rf"(?<!\w){re.escape(variant)}(?!\w)", flags=re.IGNORECASE)
        return pattern.sub(canonical, text)
    return text.replace(variant, canonical)


def _contains_variant(text: str, variant: str) -> bool:
    if not variant:
        return False
    if re.search(r"[A-Za-z0-9_]", variant):
        return re.search(rf"(?<!\w){re.escape(variant)}(?!\w)", text, flags=re.IGNORECASE) is not None
    return variant in text


@dataclass(slots=True)
class QueryRewriteResult:
    original_query: str
    rewritten_query: str
    matched_canonicals: list[str] = field(default_factory=list)
    matched_variants: list[str] = field(default_factory=list)
    scopes: list[str] = field(default_factory=list)
    source_path: str | None = None
    applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "original_query": self.original_query,
            "rewritten_query": self.rewritten_query,
            "matched_canonicals": list(self.matched_canonicals),
            "matched_variants": list(self.matched_variants),
            "scopes": list(self.scopes),
            "source_path": self.source_path,
            "applied": self.applied,
        }


@dataclass(slots=True)
class SynonymDictionary:
    global_entries: dict[str, list[str]] = field(default_factory=dict)
    connection_entries: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    domain_entries: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    source_path: str | None = None

    @classmethod
    def from_file(cls, path: str | Path | None) -> "SynonymDictionary":
        if not path:
            return cls()
        resolved = Path(path)
        if not resolved.exists():
            return cls(source_path=str(resolved))
        payload = json.loads(resolved.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Synonym dictionary must be a JSON object")
        if "global" not in payload and "connections" not in payload and "domains" not in payload:
            payload = {"global": payload}
        return cls(
            global_entries=_normalize_mapping(payload.get("global") or {}),
            connection_entries={
                str(connection_id): _normalize_mapping(entries)
                for connection_id, entries in (payload.get("connections") or {}).items()
                if isinstance(entries, dict)
            },
            domain_entries={
                str(domain): _normalize_mapping(entries)
                for domain, entries in (payload.get("domains") or {}).items()
                if isinstance(entries, dict)
            },
            source_path=str(resolved),
        )

    @classmethod
    def from_default(cls) -> "SynonymDictionary":
        return cls.from_file(DEFAULT_SYNONYM_PATH)

    @property
    def is_empty(self) -> bool:
        return not (self.global_entries or self.connection_entries or self.domain_entries)

    @property
    def signature(self) -> str:
        payload = {
            "global": self.global_entries,
            "connections": self.connection_entries,
            "domains": self.domain_entries,
            "source_path": self.source_path or "",
        }
        return stable_hash([json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)])

    def rewrite(
        self,
        text: str,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
        max_terms: int = 12,
    ) -> str:
        return self.expand(text, connection_id=connection_id, domain=domain, max_terms=max_terms).rewritten_query

    def expand(
        self,
        text: str,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
        max_terms: int = 12,
    ) -> QueryRewriteResult:
        original = text.strip()
        if not original or self.is_empty:
            return QueryRewriteResult(original_query=text, rewritten_query=text, source_path=self.source_path, applied=False)

        scopes: list[tuple[str, dict[str, list[str]]]] = [
            ("global", self.global_entries),
            ("connection", self.connection_entries.get(connection_id or "", {})),
            ("domain", self.domain_entries.get(domain or "", {})),
        ]

        matched_canonicals: list[str] = []
        matched_variants: list[str] = []
        hints: list[str] = []

        for scope_name, mapping in scopes:
            if not mapping:
                continue
            for canonical, synonyms in mapping.items():
                variants = sorted({canonical, *synonyms}, key=len, reverse=True)
                matched = [variant for variant in variants if _contains_variant(original, variant)]
                if not matched:
                    continue
                if canonical not in matched_canonicals:
                    matched_canonicals.append(canonical)
                for variant in matched:
                    if variant not in matched_variants:
                        matched_variants.append(variant)
                hint_terms: list[str] = []
                for variant in variants:
                    if variant not in hint_terms:
                        hint_terms.append(variant)
                    if len(hint_terms) >= max_terms:
                        break
                hints.append(f"{canonical}({', '.join(hint_terms)})")

        if not hints:
            return QueryRewriteResult(
                original_query=text,
                rewritten_query=text,
                source_path=self.source_path,
                applied=False,
            )

        prefix = original if original.endswith(("。", ".", "!", "?", "！", "？")) else f"{original}。"
        rewritten = f"{prefix} 检索提示: {'; '.join(hints)}"
        return QueryRewriteResult(
            original_query=text,
            rewritten_query=rewritten,
            matched_canonicals=matched_canonicals,
            matched_variants=matched_variants,
            scopes=[scope for scope, mapping in scopes if mapping],
            source_path=self.source_path,
            applied=True,
        )


@dataclass(slots=True)
class QueryRewriteEngine:
    dictionary: SynonymDictionary = field(default_factory=SynonymDictionary)
    enabled: bool = True
    max_terms: int = 12

    @classmethod
    def from_file(cls, path: str | Path | None, *, enabled: bool = True, max_terms: int = 12) -> "QueryRewriteEngine":
        return cls(dictionary=SynonymDictionary.from_file(path), enabled=enabled, max_terms=max_terms)

    @classmethod
    def from_default(cls, *, enabled: bool = True, max_terms: int = 12) -> "QueryRewriteEngine":
        return cls.from_file(DEFAULT_SYNONYM_PATH, enabled=enabled, max_terms=max_terms)

    @property
    def signature(self) -> str:
        return self.dictionary.signature

    def rewrite(
        self,
        text: str,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
        max_terms: int | None = None,
    ) -> QueryRewriteResult:
        if not self.enabled:
            return QueryRewriteResult(original_query=text, rewritten_query=text, source_path=self.dictionary.source_path, applied=False)
        return self.dictionary.expand(
            text,
            connection_id=connection_id,
            domain=domain,
            max_terms=max_terms or self.max_terms,
        )

