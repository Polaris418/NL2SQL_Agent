from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _normalize_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {item.strip().lower() for item in value.split(",") if item.strip()}
    if isinstance(value, Iterable):
        return {
            _normalize_text(item)
            for item in value
            if _normalize_text(item)
        }
    normalized = _normalize_text(value)
    return {normalized} if normalized else set()


def _coerce_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], (), {}, set()):
            return value
    return None


class AccessEffect(str, Enum):
    ALLOW = "allow"
    DENY = "deny"

    @property
    def allowed(self) -> bool:
        return self is AccessEffect.ALLOW


@dataclass(slots=True)
class AccessScope:
    tenant_id: str | None = None
    project_id: str | None = None
    connection_id: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    db_type: str | None = None
    table_name: str | None = None
    table_tags: set[str] = field(default_factory=set)
    business_domains: set[str] = field(default_factory=set)
    metadata: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "AccessScope":
        return AccessScope(
            tenant_id=_normalize_text(self.tenant_id) or None,
            project_id=_normalize_text(self.project_id) or None,
            connection_id=_normalize_text(self.connection_id) or None,
            database_name=_normalize_text(self.database_name) or None,
            schema_name=_normalize_text(self.schema_name) or None,
            db_type=_normalize_text(self.db_type) or None,
            table_name=_normalize_text(self.table_name) or None,
            table_tags=_normalize_set(self.table_tags),
            business_domains=_normalize_set(self.business_domains),
            metadata=dict(self.metadata or {}),
        )

    @classmethod
    def from_any(cls, value: Any = None, **kwargs: Any) -> "AccessScope":
        if value is None and not kwargs:
            return cls()
        if isinstance(value, cls):
            payload = value.to_dict()
        elif isinstance(value, Mapping):
            payload = dict(value)
            if not payload.get("table_name"):
                payload["table_name"] = _first_non_empty(payload.get("table_name"), payload.get("name"))
            if not payload.get("table_tags"):
                payload["table_tags"] = _first_non_empty(payload.get("table_tags"), payload.get("tags"))
            if not payload.get("business_domains"):
                payload["business_domains"] = _first_non_empty(
                    payload.get("business_domains"),
                    payload.get("domain_tags"),
                    payload.get("domains"),
                    payload.get("domain"),
                )
        else:
            payload = {
                "tenant_id": getattr(value, "tenant_id", None),
                "project_id": getattr(value, "project_id", None),
                "connection_id": getattr(value, "connection_id", None),
                "database_name": getattr(value, "database_name", None),
                "schema_name": getattr(value, "schema_name", None),
                "db_type": getattr(value, "db_type", None),
                "table_name": _first_non_empty(
                    getattr(value, "table_name", None),
                    getattr(value, "name", None),
                ),
                "table_tags": _first_non_empty(
                    getattr(value, "table_tags", None),
                    getattr(value, "tags", None),
                ),
                "business_domains": _first_non_empty(
                    getattr(value, "business_domains", None),
                    getattr(value, "domain_tags", None),
                    getattr(value, "domains", None),
                    getattr(value, "domain", None),
                ),
                "metadata": getattr(value, "metadata", None),
            }
        if kwargs:
            payload.update(kwargs)
        return cls(
            tenant_id=payload.get("tenant_id"),
            project_id=payload.get("project_id"),
            connection_id=payload.get("connection_id"),
            database_name=payload.get("database_name"),
            schema_name=payload.get("schema_name"),
            db_type=payload.get("db_type"),
            table_name=payload.get("table_name"),
            table_tags=_normalize_set(payload.get("table_tags")),
            business_domains=_normalize_set(payload.get("business_domains")),
            metadata=dict(_coerce_mapping(payload.get("metadata"))),
        ).normalized()

    def is_empty(self) -> bool:
        return self.specificity() == 0

    def specificity(self) -> int:
        return sum(
            1
            for value in (
                self.tenant_id,
                self.project_id,
                self.connection_id,
                self.database_name,
                self.schema_name,
                self.db_type,
                self.table_name,
                self.table_tags,
                self.business_domains,
            )
            if value
        )

    def matches(self, other: Any) -> bool:
        candidate = AccessScope.from_any(other).normalized()
        if self.tenant_id and self.tenant_id != candidate.tenant_id:
            return False
        if self.project_id and self.project_id != candidate.project_id:
            return False
        if self.connection_id and self.connection_id != candidate.connection_id:
            return False
        if self.database_name and self.database_name != candidate.database_name:
            return False
        if self.schema_name and self.schema_name != candidate.schema_name:
            return False
        if self.db_type and self.db_type != candidate.db_type:
            return False
        if self.table_name and self.table_name != candidate.table_name:
            return False
        if self.table_tags and not (self.table_tags & candidate.table_tags):
            return False
        if self.business_domains and not (self.business_domains & candidate.business_domains):
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["table_tags"] = sorted(self.table_tags)
        payload["business_domains"] = sorted(self.business_domains)
        payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(slots=True)
class AccessRule:
    rule_id: str
    effect: AccessEffect
    priority: int = 0
    principal_scope: AccessScope | None = None
    resource_scope: AccessScope | None = None
    reason: str = ""
    enabled: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_any(cls, value: Any) -> "AccessRule":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("AccessRule.from_any expects a mapping or AccessRule instance")
        payload = dict(value)
        effect = payload.get("effect", AccessEffect.DENY)
        if not isinstance(effect, AccessEffect):
            effect = AccessEffect(str(effect).lower())
        return cls(
            rule_id=str(payload.get("rule_id") or payload.get("id") or ""),
            effect=effect,
            priority=int(payload.get("priority", 0) or 0),
            principal_scope=AccessScope.from_any(payload.get("principal_scope") or payload.get("subject_scope")),
            resource_scope=AccessScope.from_any(payload.get("resource_scope")),
            reason=str(payload.get("reason") or ""),
            enabled=bool(payload.get("enabled", True)),
            metadata=dict(_coerce_mapping(payload.get("metadata"))),
        )

    def matches(self, principal: Any = None, resource: Any = None) -> bool:
        if not self.enabled:
            return False
        if self.principal_scope and not self.principal_scope.matches(principal):
            return False
        if self.resource_scope and not self.resource_scope.matches(resource):
            return False
        return True

    def specificity(self) -> int:
        score = 0
        if self.principal_scope:
            score += self.principal_scope.specificity()
        if self.resource_scope:
            score += self.resource_scope.specificity()
        return score

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "effect": self.effect.value,
            "priority": self.priority,
            "principal_scope": self.principal_scope.to_dict() if self.principal_scope else None,
            "resource_scope": self.resource_scope.to_dict() if self.resource_scope else None,
            "reason": self.reason,
            "enabled": self.enabled,
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class AccessDecision:
    allowed: bool
    effect: AccessEffect
    reason: str = ""
    rule_id: str | None = None
    matched_rules: list[AccessRule] = field(default_factory=list)
    principal_scope: AccessScope | None = None
    resource_scope: AccessScope | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def denied(self) -> bool:
        return not self.allowed

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "effect": self.effect.value,
            "reason": self.reason,
            "rule_id": self.rule_id,
            "matched_rules": [rule.to_dict() for rule in self.matched_rules],
            "principal_scope": self.principal_scope.to_dict() if self.principal_scope else None,
            "resource_scope": self.resource_scope.to_dict() if self.resource_scope else None,
            "metadata": dict(self.metadata),
        }


class AccessRuleRepository:
    """In-memory repository for access rules.

    The repository keeps rules in insertion order and exposes helpers that make
    it easy for orchestrator/schema documentation code to query by principal
    scope and resource scope without needing to know storage details.
    """

    def __init__(self, rules: Iterable[AccessRule | Mapping[str, Any]] | None = None):
        self._rules: dict[str, AccessRule] = {}
        if rules:
            for rule in rules:
                self.upsert(rule)

    def upsert(self, rule: AccessRule | Mapping[str, Any]) -> AccessRule:
        access_rule = AccessRule.from_any(rule)
        if not access_rule.rule_id:
            raise ValueError("AccessRule.rule_id is required")
        self._rules[access_rule.rule_id] = access_rule
        return access_rule

    def get(self, rule_id: str) -> AccessRule | None:
        return self._rules.get(rule_id)

    def delete(self, rule_id: str) -> bool:
        return self._rules.pop(rule_id, None) is not None

    def clear(self) -> None:
        self._rules.clear()

    def list(self, *, enabled_only: bool = True) -> list[AccessRule]:
        rules = list(self._rules.values())
        if enabled_only:
            rules = [rule for rule in rules if rule.enabled]
        return rules

    def matching(
        self,
        principal: Any = None,
        resource: Any = None,
        *,
        enabled_only: bool = True,
    ) -> list[AccessRule]:
        rules = self.list(enabled_only=enabled_only)
        return [rule for rule in rules if rule.matches(principal, resource)]

    def snapshot(self) -> dict[str, Any]:
        return {
            "rules": [rule.to_dict() for rule in self.list(enabled_only=False)],
            "count": len(self._rules),
        }


class AccessControlPolicy:
    """Policy engine that evaluates access rules against principals/resources."""

    def __init__(
        self,
        repository: AccessRuleRepository | Iterable[AccessRule | Mapping[str, Any]] | None = None,
        *,
        default_effect: AccessEffect = AccessEffect.DENY,
    ):
        if repository is None:
            repository = AccessRuleRepository()
        elif not isinstance(repository, AccessRuleRepository):
            repository = AccessRuleRepository(repository)
        self.repository = repository
        self.default_effect = default_effect

    def evaluate(self, principal: Any = None, resource: Any = None) -> AccessDecision:
        principal_scope = AccessScope.from_any(principal)
        resource_scope = AccessScope.from_any(resource)
        matched_rules = self.repository.matching(principal_scope, resource_scope, enabled_only=True)

        if not matched_rules:
            reason = "default deny: no matching access rule"
            return AccessDecision(
                allowed=self.default_effect.allowed if self.default_effect else False,
                effect=self.default_effect,
                reason=reason,
                principal_scope=principal_scope,
                resource_scope=resource_scope,
            )

        ordered = sorted(
            matched_rules,
            key=lambda rule: (
                rule.priority,
                1 if rule.effect is AccessEffect.DENY else 0,
                rule.specificity(),
            ),
            reverse=True,
        )
        selected = ordered[0]
        allowed = selected.effect.allowed
        reason = selected.reason or f"{selected.effect.value} rule matched"
        return AccessDecision(
            allowed=allowed,
            effect=selected.effect,
            reason=reason,
            rule_id=selected.rule_id,
            matched_rules=ordered,
            principal_scope=principal_scope,
            resource_scope=resource_scope,
            metadata={"selected_rule": selected.to_dict()},
        )

    def is_allowed(self, principal: Any = None, resource: Any = None) -> bool:
        return self.evaluate(principal, resource).allowed

    def filter_resources(self, resources: Iterable[Any], principal: Any = None) -> list[Any]:
        return [resource for resource in resources if self.is_allowed(principal, resource)]

    def decide_resources(self, resources: Iterable[Any], principal: Any = None) -> list[tuple[Any, AccessDecision]]:
        return [(resource, self.evaluate(principal, resource)) for resource in resources]


__all__ = [
    "AccessControlPolicy",
    "AccessDecision",
    "AccessEffect",
    "AccessResource",
    "AccessRule",
    "AccessRuleRepository",
    "AccessScope",
]


# Backwards-friendly alias for orchestrator/schema_doc call sites that want to
# read the target object as a resource rather than a generic scope.
AccessResource = AccessScope
