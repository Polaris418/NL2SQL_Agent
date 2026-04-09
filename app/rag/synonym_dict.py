from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import re
from typing import Any


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _replace_variant(text: str, variant: str, canonical: str) -> str:
    if not variant or variant == canonical:
        return text
    if re.search(r"[A-Za-z0-9_]", variant):
        pattern = re.compile(rf"(?<!\w){re.escape(variant)}(?!\w)", flags=re.IGNORECASE)
        return pattern.sub(canonical, text)
    return text.replace(variant, canonical)


@dataclass(slots=True)
class SynonymEntry:
    canonical: str
    synonyms: list[str] = field(default_factory=list)
    scope: str | None = "global"
    domain: str | None = None


class SynonymDictionary:
    """Runtime synonym dictionary with file reload support."""

    def __init__(self, file_path: str | Path | None = None):
        self.file_path = str(file_path) if file_path else None
        self.global_entries: dict[str, list[str]] = {}
        self.connection_entries: dict[str, dict[str, list[str]]] = {}
        self.domain_entries: dict[str, dict[str, list[str]]] = {}
        self._last_loaded_mtime: float | None = None
        if self.file_path:
            self.load_from_file(self.file_path)

    @classmethod
    def from_file(cls, path: str | Path | None) -> "SynonymDictionary":
        return cls(path)

    def load_from_file(self, path: str | Path) -> None:
        file_path = Path(path)
        self.file_path = str(file_path)
        if not file_path.exists():
            self.global_entries = {}
            self.connection_entries = {}
            self.domain_entries = {}
            self._last_loaded_mtime = None
            return
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Synonym dictionary must be a JSON object")
        if "global" not in payload and "connections" not in payload and "domains" not in payload:
            payload = {"global": payload}
        self.global_entries = _normalize_mapping(payload.get("global") or {})
        self.connection_entries = {
            str(connection_id): _normalize_mapping(entries)
            for connection_id, entries in (payload.get("connections") or {}).items()
            if isinstance(entries, dict)
        }
        self.domain_entries = {
            str(domain): _normalize_mapping(entries)
            for domain, entries in (payload.get("domains") or {}).items()
            if isinstance(entries, dict)
        }
        self._last_loaded_mtime = file_path.stat().st_mtime

    def reload(self) -> bool:
        if not self.file_path:
            return False
        self.load_from_file(self.file_path)
        return True

    def reload_if_needed(self) -> bool:
        if not self.file_path:
            return False
        file_path = Path(self.file_path)
        if not file_path.exists():
            return False
        mtime = file_path.stat().st_mtime
        if self._last_loaded_mtime is None or mtime > self._last_loaded_mtime:
            self.load_from_file(file_path)
            return True
        return False

    def add_entry(self, entry: SynonymEntry) -> None:
        canonical = entry.canonical.strip()
        if not canonical:
            return
        target = self.global_entries
        if entry.scope and entry.scope != "global":
            target = self.connection_entries.setdefault(entry.scope, {})
        if entry.domain:
            target = self.domain_entries.setdefault(entry.domain, {})
        target[canonical] = _coerce_str_list(entry.synonyms)

    def get_canonical(self, term: str, *, connection_id: str | None = None, domain: str | None = None) -> str:
        matched = self._find_canonical(term, connection_id=connection_id, domain=domain)
        return matched or term

    def get_synonyms(self, term: str, *, connection_id: str | None = None, domain: str | None = None) -> list[str]:
        canonical = self.get_canonical(term, connection_id=connection_id, domain=domain)
        for mapping in self._iter_mappings(connection_id=connection_id, domain=domain):
            if canonical in mapping:
                return list(mapping[canonical])
        return []

    def rewrite(
        self,
        text: str,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
    ) -> str:
        rewritten, _ = self.rewrite_with_trace(text, connection_id=connection_id, domain=domain)
        return rewritten

    def rewrite_with_trace(
        self,
        text: str,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
    ) -> tuple[str, list[tuple[str, str]]]:
        self.reload_if_needed()
        rewritten = text
        applied: list[tuple[str, str]] = []
        for mapping in self._iter_mappings(connection_id=connection_id, domain=domain):
            for canonical, synonyms in mapping.items():
                variants = sorted({canonical, *synonyms}, key=len, reverse=True)
                for variant in variants:
                    updated = _replace_variant(rewritten, variant, canonical)
                    if updated != rewritten and variant != canonical:
                        applied.append((variant, canonical))
                    rewritten = updated
        return rewritten, applied

    def _find_canonical(self, term: str, *, connection_id: str | None = None, domain: str | None = None) -> str | None:
        lowered = term.strip().lower()
        for mapping in self._iter_mappings(connection_id=connection_id, domain=domain):
            for canonical, synonyms in mapping.items():
                if canonical.lower() == lowered:
                    return canonical
                if lowered in {item.lower() for item in synonyms}:
                    return canonical
        return None

    def _iter_mappings(
        self,
        *,
        connection_id: str | None = None,
        domain: str | None = None,
    ) -> list[dict[str, list[str]]]:
        return [
            self.global_entries,
            self.connection_entries.get(connection_id or "", {}),
            self.domain_entries.get(domain or "", {}),
        ]


def _normalize_mapping(raw_mapping: dict[str, Any]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for canonical, synonyms in raw_mapping.items():
        canonical_key = str(canonical).strip()
        if not canonical_key:
            continue
        normalized[canonical_key] = _coerce_str_list(synonyms)
    return normalized
