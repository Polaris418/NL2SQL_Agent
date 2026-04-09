from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any
import time

from app.agent.utils import stable_hash


@dataclass(slots=True, frozen=True)
class RetrievalCacheKey:
    query: str
    connection_id: str
    schema_version: str | None
    top_k: int

    def fingerprint(self) -> str:
        return stable_hash(
            [
                self.connection_id,
                self.schema_version or "",
                self.query,
                str(self.top_k),
            ]
        )


@dataclass(slots=True)
class RetrievalCacheEntry:
    key: RetrievalCacheKey
    value: Any
    created_at: float = field(default_factory=time.time)


class RetrievalCache:
    def __init__(
        self,
        *,
        max_entries: int | None = None,
        ttl_seconds: float | None = None,
        max_size: int | None = None,
        default_ttl: float | None = None,
        enabled: bool = True,
    ):
        resolved_max_entries = max_entries if max_entries is not None else max_size
        resolved_ttl = ttl_seconds if ttl_seconds is not None else default_ttl
        self.max_entries = max(1, int(resolved_max_entries if resolved_max_entries is not None else 256))
        self.ttl_seconds = max(0.0, float(resolved_ttl if resolved_ttl is not None else 300.0))
        self.enabled = bool(enabled)
        self._cache: OrderedDict[RetrievalCacheKey, RetrievalCacheEntry] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._invalidations = 0

    def snapshot(self) -> dict[str, Any]:
        total = self._hits + self._misses
        return {
            "entries": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
            "invalidations": self._invalidations,
            "hit_rate": (self._hits / total) if total else 0.0,
            "ttl_seconds": self.ttl_seconds,
            "max_entries": self.max_entries,
            "enabled": self.enabled,
        }

    def get(self, query: str, connection_id: str, schema_version: str | None, top_k: int = 8) -> Any | None:
        if not self.enabled:
            self._misses += 1
            return None
        key = RetrievalCacheKey(query=query, connection_id=connection_id, schema_version=schema_version, top_k=top_k)
        entry = self._cache.get(key)
        if entry is None:
            self._misses += 1
            return None
        if self.ttl_seconds and time.time() - entry.created_at > self.ttl_seconds:
            self._cache.pop(key, None)
            self._misses += 1
            self._invalidations += 1
            return None
        self._cache.move_to_end(key)
        self._hits += 1
        return deepcopy(entry.value)

    def put(self, query: str, connection_id: str, schema_version: str | None, value: Any, top_k: int = 8) -> None:
        if not self.enabled:
            return
        key = RetrievalCacheKey(query=query, connection_id=connection_id, schema_version=schema_version, top_k=top_k)
        self._cache[key] = RetrievalCacheEntry(key=key, value=deepcopy(value))
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)
            self._evictions += 1

    def invalidate_connection(self, connection_id: str) -> int:
        keys = [key for key in self._cache if key.connection_id == connection_id]
        for key in keys:
            self._cache.pop(key, None)
        self._invalidations += len(keys)
        return len(keys)

    def invalidate_schema_version(self, connection_id: str, schema_version: str | None) -> int:
        keys = [key for key in self._cache if key.connection_id == connection_id and key.schema_version != schema_version]
        for key in keys:
            self._cache.pop(key, None)
        self._invalidations += len(keys)
        return len(keys)
