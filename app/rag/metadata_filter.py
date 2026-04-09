from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from app.agent.contracts import tokenize_text


DOMAIN_HINTS = {
    "user": {"user", "member", "customer", "client", "会员", "用户", "客户"},
    "order": {"order", "orders", "purchase", "sale", "订单", "交易"},
    "sales": {"sales", "revenue", "gmv", "金额", "销售", "营收"},
    "organization": {"department", "dept", "team", "org", "organization", "部门", "组织"},
    "product": {"product", "item", "goods", "sku", "商品", "产品"},
}


@dataclass(slots=True)
class MetadataFilter:
    tenant_id: str | None = None
    project_id: str | None = None
    connection_id: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    db_type: str | None = None
    table_tags: set[str] = field(default_factory=set)
    business_domains: set[str] = field(default_factory=set)
    table_names: set[str] = field(default_factory=set)

    def matches(self, metadata: Mapping[str, Any]) -> bool:
        if self.tenant_id and metadata.get("tenant_id") != self.tenant_id:
            return False
        if self.project_id and metadata.get("project_id") != self.project_id:
            return False
        if self.connection_id and metadata.get("connection_id") != self.connection_id:
            return False
        if self.database_name and metadata.get("database_name") != self.database_name:
            return False
        if self.schema_name and metadata.get("schema_name") != self.schema_name:
            return False
        if self.db_type and str(metadata.get("db_type") or "").lower() != self.db_type:
            return False
        if self.table_names and metadata.get("table_name") not in self.table_names:
            return False

        if self.table_tags:
            meta_tags = self._normalize_set(metadata.get("table_tags") or metadata.get("tags"))
            if not meta_tags or not (meta_tags & self.table_tags):
                return False

        if self.business_domains:
            meta_domains = self._normalize_set(metadata.get("business_domains") or metadata.get("domain_tags"))
            if not meta_domains or not (meta_domains & self.business_domains):
                return False
        return True

    def to_where_clause(self) -> dict[str, Any] | None:
        clauses: list[dict[str, Any]] = []
        if self.tenant_id:
            clauses.append({"tenant_id": self.tenant_id})
        if self.project_id:
            clauses.append({"project_id": self.project_id})
        if self.connection_id:
            clauses.append({"connection_id": self.connection_id})
        if self.database_name:
            clauses.append({"database_name": self.database_name})
        if self.schema_name:
            clauses.append({"schema_name": self.schema_name})
        if self.db_type:
            clauses.append({"db_type": self.db_type})
        if self.table_names:
            clauses.append({"table_name": {"$in": sorted(self.table_names)}})
        if self.table_tags:
            clauses.append({"table_tags": {"$in": sorted(self.table_tags)}})
        if self.business_domains:
            clauses.append({"business_domains": {"$in": sorted(self.business_domains)}})
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return {"$and": clauses}

    def merge(self, other: "MetadataFilter") -> "MetadataFilter":
        return MetadataFilter(
            tenant_id=other.tenant_id or self.tenant_id,
            project_id=other.project_id or self.project_id,
            connection_id=other.connection_id or self.connection_id,
            database_name=other.database_name or self.database_name,
            schema_name=other.schema_name or self.schema_name,
            db_type=other.db_type or self.db_type,
            table_tags=set(self.table_tags) | set(other.table_tags),
            business_domains=set(self.business_domains) | set(other.business_domains),
            table_names=set(self.table_names) | set(other.table_names),
        )

    @classmethod
    def infer_from_query(
        cls,
        query: str,
        *,
        connection_id: str | None = None,
        database_name: str | None = None,
        schema_name: str | None = None,
    ) -> "MetadataFilter":
        tokens = set(tokenize_text(query))
        business_domains = {
            domain
            for domain, hints in DOMAIN_HINTS.items()
            if any(hint in tokens or hint in query.lower() for hint in hints)
        }
        return cls(
            connection_id=connection_id,
            database_name=database_name,
            schema_name=schema_name,
            business_domains=business_domains,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "project_id": self.project_id,
            "connection_id": self.connection_id,
            "database_name": self.database_name,
            "schema_name": self.schema_name,
            "db_type": self.db_type,
            "table_tags": sorted(self.table_tags),
            "business_domains": sorted(self.business_domains),
            "table_names": sorted(self.table_names),
        }

    @staticmethod
    def _normalize_set(value: Any) -> set[str]:
        if value is None:
            return set()
        if isinstance(value, str):
            return {item.strip().lower() for item in value.split(",") if item.strip()}
        if isinstance(value, Iterable):
            return {str(item).strip().lower() for item in value if str(item).strip()}
        return {str(value).strip().lower()}
