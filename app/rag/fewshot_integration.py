from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

from app.agent.contracts import tokenize_text
from app.rag.multi_tenant import MultiTenantIsolationManager, TenantScope


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
class FewShotExample:
    id: str
    question: str
    sql: str
    explanation: str = ""
    tenant_ids: list[str] = field(default_factory=list)
    project_ids: list[str] = field(default_factory=list)
    connection_ids: list[str] = field(default_factory=list)
    db_types: list[str] = field(default_factory=list)
    business_domains: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "FewShotExample":
        return FewShotExample(
            id=_normalize_text(self.id),
            question=self.question.strip(),
            sql=self.sql.strip(),
            explanation=self.explanation.strip(),
            tenant_ids=_normalize_list(self.tenant_ids),
            project_ids=_normalize_list(self.project_ids),
            connection_ids=_normalize_list(self.connection_ids),
            db_types=_normalize_list(self.db_types),
            business_domains=_normalize_list(self.business_domains),
            tags=_normalize_list(self.tags),
            priority=int(self.priority),
            metadata=dict(self.metadata),
        )

    def matches_scope(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any) -> bool:
        selector = FewShotScopeMatcher(scope, **kwargs)
        return selector.matches(self)

    def specificity(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any) -> int:
        selector = FewShotScopeMatcher(scope, **kwargs)
        return selector.specificity(self)

    def to_prompt_block(self, index: int | None = None) -> str:
        prefix = f"Example {index} ({self.id}):\n" if index is not None else f"Example ({self.id}):\n"
        explanation = f"\nReason: {self.explanation}" if self.explanation else ""
        return (
            f"{prefix}Question: {self.question}\n"
            f"SQL: {self.sql}"
            f"{explanation}"
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tenant_ids"] = list(self.tenant_ids)
        payload["project_ids"] = list(self.project_ids)
        payload["connection_ids"] = list(self.connection_ids)
        payload["db_types"] = list(self.db_types)
        payload["business_domains"] = list(self.business_domains)
        payload["tags"] = list(self.tags)
        payload["metadata"] = dict(self.metadata)
        return payload


class FewShotScopeMatcher:
    def __init__(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any):
        if scope is None:
            scope = TenantScope(**kwargs)
        elif isinstance(scope, dict):
            scope = TenantScope(**scope)
        elif kwargs:
            merged = scope.to_dict()
            merged.update({key: value for key, value in kwargs.items() if value is not None})
            scope = TenantScope(**merged)
        self.scope = scope.normalized()

    def matches(self, example: FewShotExample) -> bool:
        example = example.normalized()
        if self.scope.tenant_id and example.tenant_ids and self.scope.tenant_id not in example.tenant_ids:
            return False
        if self.scope.project_id and example.project_ids and self.scope.project_id not in example.project_ids:
            return False
        if self.scope.connection_id and example.connection_ids and self.scope.connection_id not in example.connection_ids:
            return False
        if self.scope.db_type and example.db_types and self.scope.db_type not in example.db_types:
            return False
        if self.scope.business_domains and example.business_domains:
            if not set(self.scope.business_domains).intersection(example.business_domains):
                return False
        return True

    def specificity(self, example: FewShotExample) -> int:
        example = example.normalized()
        score = 0
        if self.scope.tenant_id and (not example.tenant_ids or self.scope.tenant_id in example.tenant_ids):
            score += 3
        if self.scope.project_id and (not example.project_ids or self.scope.project_id in example.project_ids):
            score += 3
        if self.scope.connection_id and (not example.connection_ids or self.scope.connection_id in example.connection_ids):
            score += 4
        if self.scope.db_type and (not example.db_types or self.scope.db_type in example.db_types):
            score += 2
        if self.scope.business_domains and example.business_domains and set(self.scope.business_domains).intersection(example.business_domains):
            score += 2
        return score


class FewShotRegistry:
    def __init__(self):
        self._examples: dict[str, FewShotExample] = {}

    def register(self, example: FewShotExample | dict[str, Any]) -> FewShotExample:
        if isinstance(example, dict):
            example = FewShotExample(**example)
        normalized = example.normalized()
        self._examples[normalized.id] = normalized
        return normalized

    def register_many(self, examples: Iterable[FewShotExample | dict[str, Any]]) -> list[FewShotExample]:
        return [self.register(example) for example in examples]

    def list(self) -> list[FewShotExample]:
        return list(self._examples.values())

    def select(
        self,
        *,
        scope: TenantScope | dict[str, Any] | None = None,
        query: str | None = None,
        limit: int = 4,
        db_type: str | None = None,
        business_domain: str | None = None,
    ) -> list[FewShotExample]:
        matcher = FewShotScopeMatcher(scope, db_type=db_type, business_domains=[business_domain] if business_domain else None)
        query_tokens = set(tokenize_text(query or ""))
        candidates: list[tuple[int, int, FewShotExample]] = []
        for example in self._examples.values():
            if not matcher.matches(example):
                continue
            example_tokens = set(tokenize_text(f"{example.question} {example.sql} {example.explanation}"))
            overlap = len(query_tokens & example_tokens)
            score = matcher.specificity(example) * 10 + overlap + int(example.priority)
            candidates.append((score, -len(example_tokens), example))
        candidates.sort(key=lambda item: (item[0], item[1], item[2].id), reverse=True)
        if limit <= 0:
            return []
        return [item[2] for item in candidates[:limit]]

    def build_prompt_block(
        self,
        *,
        scope: TenantScope | dict[str, Any] | None = None,
        query: str | None = None,
        limit: int = 4,
        db_type: str | None = None,
        business_domain: str | None = None,
    ) -> str:
        examples = self.select(
            scope=scope,
            query=query,
            limit=limit,
            db_type=db_type,
            business_domain=business_domain,
        )
        return "\n\n".join(example.to_prompt_block(index=index + 1) for index, example in enumerate(examples))

    def select_payload(
        self,
        *,
        scope: TenantScope | dict[str, Any] | None = None,
        query: str | None = None,
        limit: int = 4,
        db_type: str | None = None,
        business_domain: str | None = None,
    ) -> dict[str, Any]:
        examples = self.select(
            scope=scope,
            query=query,
            limit=limit,
            db_type=db_type,
            business_domain=business_domain,
        )
        return {
            "examples": [example.to_dict() for example in examples],
            "prompt_block": "\n\n".join(example.to_prompt_block(index=index + 1) for index, example in enumerate(examples)),
        }


class FewShotIntegration:
    """Connects tenant-aware scope resolution with few-shot example selection."""

    def __init__(
        self,
        registry: FewShotRegistry | None = None,
        *,
        tenant_manager: MultiTenantIsolationManager | None = None,
    ):
        self.registry = registry or FewShotRegistry()
        self.tenant_manager = tenant_manager or MultiTenantIsolationManager()

    def register(self, example: FewShotExample | dict[str, Any]) -> FewShotExample:
        return self.registry.register(example)

    def register_many(self, examples: Iterable[FewShotExample | dict[str, Any]]) -> list[FewShotExample]:
        return self.registry.register_many(examples)

    def select(
        self,
        *,
        scope: TenantScope | dict[str, Any] | None = None,
        query: str | None = None,
        limit: int = 4,
        business_domain: str | None = None,
    ) -> list[FewShotExample]:
        scope_obj = self.tenant_manager.normalize_scope(scope)
        return self.registry.select(
            scope=scope_obj,
            query=query,
            limit=limit,
            db_type=scope_obj.db_type,
            business_domain=business_domain,
        )

    def build_prompt_block(
        self,
        *,
        scope: TenantScope | dict[str, Any] | None = None,
        query: str | None = None,
        limit: int = 4,
        business_domain: str | None = None,
    ) -> str:
        scope_obj = self.tenant_manager.normalize_scope(scope)
        return self.registry.build_prompt_block(
            scope=scope_obj,
            query=query,
            limit=limit,
            db_type=scope_obj.db_type,
            business_domain=business_domain,
        )

    def scope_payload(self, scope: TenantScope | dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        scope_obj = self.tenant_manager.normalize_scope(scope, **kwargs)
        return {
            "scope": scope_obj.to_dict(),
            "isolation_key": self.tenant_manager.isolation_key(scope_obj),
            "metadata_filter": self.tenant_manager.metadata_filter(scope_obj).to_dict(),
            "few_shot_prompt": self.build_prompt_block(scope=scope_obj),
        }


__all__ = [
    "FewShotExample",
    "FewShotIntegration",
    "FewShotRegistry",
    "FewShotScopeMatcher",
]
