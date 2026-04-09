from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable


def _normalize_text(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_list(values: Iterable[str] | None) -> list[str]:
    seen: list[str] = []
    for value in values or []:
        normalized = _normalize_text(value)
        if normalized and normalized not in seen:
            seen.append(normalized)
    return seen


@dataclass(slots=True)
class TenantScope:
    tenant_id: str | None = None
    project_id: str | None = None
    connection_id: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    db_type: str | None = None
    business_domains: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "TenantScope":
        return TenantScope(
            tenant_id=_normalize_text(self.tenant_id) or None,
            project_id=_normalize_text(self.project_id) or None,
            connection_id=_normalize_text(self.connection_id) or None,
            database_name=_normalize_text(self.database_name) or None,
            schema_name=_normalize_text(self.schema_name) or None,
            db_type=_normalize_text(self.db_type) or None,
            business_domains=_normalize_list(self.business_domains),
            extra=dict(self.extra),
        )

    def scope_parts(self) -> list[str]:
        normalized = self.normalized()
        parts = [
            f"tenant:{normalized.tenant_id}" if normalized.tenant_id else None,
            f"project:{normalized.project_id}" if normalized.project_id else None,
            f"connection:{normalized.connection_id}" if normalized.connection_id else None,
            f"database:{normalized.database_name}" if normalized.database_name else None,
            f"schema:{normalized.schema_name}" if normalized.schema_name else None,
            f"db_type:{normalized.db_type}" if normalized.db_type else None,
        ]
        if normalized.business_domains:
            parts.append("domains:" + ",".join(normalized.business_domains))
        return [part for part in parts if part]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["business_domains"] = list(self.business_domains)
        payload["extra"] = dict(self.extra)
        return payload


@dataclass(slots=True)
class MultiTenantFilter:
    tenant_id: str | None = None
    project_id: str | None = None
    connection_id: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    db_type: str | None = None
    business_domains: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["business_domains"] = list(self.business_domains)
        return payload


class MultiTenantIsolationManager:
    """Build retrieval isolation keys and filters from tenant scope."""

    def __init__(self, *, default_scope: TenantScope | None = None):
        self.default_scope = (default_scope or TenantScope()).normalized()

    def normalize_scope(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any) -> TenantScope:
        if scope is None:
            scope = TenantScope(**kwargs)
        elif isinstance(scope, dict):
            scope = TenantScope(**scope)
        if kwargs:
            scope_dict = scope.to_dict()
            scope_dict.update(kwargs)
            scope = TenantScope(**scope_dict)
        return scope.normalized()

    def isolation_key(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any) -> str:
        normalized = self.normalize_scope(scope, **kwargs)
        parts = normalized.scope_parts()
        if not parts:
            return "tenant:default"
        return "|".join(parts)

    def metadata_filter(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any) -> MultiTenantFilter:
        normalized = self.normalize_scope(scope, **kwargs)
        return MultiTenantFilter(
            tenant_id=normalized.tenant_id,
            project_id=normalized.project_id,
            connection_id=normalized.connection_id,
            database_name=normalized.database_name,
            schema_name=normalized.schema_name,
            db_type=normalized.db_type,
            business_domains=list(normalized.business_domains),
        )

    def merge_metadata_filter(
        self,
        base_filter: dict[str, Any] | None = None,
        scope: TenantScope | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        merged = dict(base_filter or {})
        normalized = self.normalize_scope(scope, **kwargs)
        if normalized.tenant_id:
            merged["tenant_id"] = normalized.tenant_id
        if normalized.project_id:
            merged["project_id"] = normalized.project_id
        if normalized.connection_id:
            merged["connection_id"] = normalized.connection_id
        if normalized.database_name:
            merged["database_name"] = normalized.database_name
        if normalized.schema_name:
            merged["schema_name"] = normalized.schema_name
        if normalized.db_type:
            merged["db_type"] = normalized.db_type
        if normalized.business_domains:
            merged["business_domains"] = list(normalized.business_domains)
        return merged

    def matches_metadata(
        self,
        metadata: dict[str, Any],
        scope: TenantScope | dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> bool:
        normalized = self.normalize_scope(scope, **kwargs)
        for field_name in ("tenant_id", "project_id", "connection_id", "database_name", "schema_name", "db_type"):
            expected = getattr(normalized, field_name)
            if expected is None:
                continue
            actual = _normalize_text(metadata.get(field_name))
            if actual != expected:
                return False
        if normalized.business_domains:
            metadata_domains = _normalize_list(metadata.get("business_domains") or metadata.get("table_tags"))
            if not set(normalized.business_domains).intersection(metadata_domains):
                return False
        return True

    def scope_payload(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        normalized = self.normalize_scope(scope, **kwargs)
        return {
            "scope": normalized.to_dict(),
            "isolation_key": self.isolation_key(normalized),
            "metadata_filter": self.metadata_filter(normalized).to_dict(),
        }


__all__ = [
    "MultiTenantFilter",
    "MultiTenantIsolationManager",
    "TenantScope",
]
