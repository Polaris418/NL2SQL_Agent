from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Coroutine, TypeVar


T = TypeVar("T")


class OperationKind(str, Enum):
    RETRIEVAL = "retrieval"
    EMBEDDING = "embedding"
    RERANKER = "reranker"


class ConcurrencyPolicy(str, Enum):
    QUEUE = "queue"
    FAIL_FAST = "fail_fast"


class ConcurrencyControlError(RuntimeError):
    """Base error for concurrency control failures."""


class ConcurrencyLimitExceeded(ConcurrencyControlError):
    def __init__(self, operation_kind: OperationKind | str, limit: int):
        self.operation_kind = OperationKind(operation_kind)
        self.limit = max(1, int(limit))
        super().__init__(f"{self.operation_kind.value} concurrency limit exceeded (limit={self.limit})")


class OperationTimeoutError(TimeoutError):
    def __init__(self, operation_kind: OperationKind | str, timeout_seconds: float):
        self.operation_kind = OperationKind(operation_kind)
        self.timeout_seconds = float(timeout_seconds)
        super().__init__(
            f"{self.operation_kind.value} operation timed out after {self.timeout_seconds:.3f} seconds"
        )


class RetrievalConcurrencyError(ConcurrencyControlError):
    """Compatibility alias for the orchestrator's existing error handling."""


class RetrievalTimeoutError(OperationTimeoutError):
    """Compatibility alias for the orchestrator's existing timeout handling."""

    def __init__(self, operation_kind: OperationKind | str = OperationKind.RETRIEVAL, timeout_seconds: float | None = None):
        if timeout_seconds is None and isinstance(operation_kind, str) and operation_kind not in {item.value for item in OperationKind}:
            message = operation_kind
            self.operation_kind = OperationKind.RETRIEVAL
            self.timeout_seconds = 0.0
            TimeoutError.__init__(self, message)
            return
        super().__init__(operation_kind, float(timeout_seconds or 0.0))


@dataclass(slots=True)
class ConcurrencyConfig:
    retrieval_limit: int = 4
    embedding_limit: int = 2
    reranker_limit: int = 2
    retrieval_timeout_seconds: float = 5.0
    embedding_timeout_seconds: float = 2.0
    reranker_timeout_seconds: float = 1.0
    default_policy: ConcurrencyPolicy | str = ConcurrencyPolicy.QUEUE

    def normalized_policy(self) -> ConcurrencyPolicy:
        return ConcurrencyPolicy(self.default_policy)


@dataclass(slots=True)
class OperationMetrics:
    limit: int
    active: int = 0
    peak_active: int = 0
    queued: int = 0
    completed: int = 0
    failed: int = 0
    timed_out: int = 0
    rejected: int = 0
    total_started: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(slots=True)
class ConcurrencySnapshot:
    default_policy: str
    config: dict[str, Any]
    operations: dict[str, dict[str, int]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ConcurrencyController:
    """Lightweight async concurrency and timeout controller.

    The controller keeps independent queues for retrieval, embedding, and
    reranker operations. It can either queue requests until a slot becomes
    available or fail fast when the configured limit is exhausted.
    """

    def __init__(self, config: ConcurrencyConfig | None = None):
        self.config = config or ConcurrencyConfig()
        self._limits: dict[OperationKind, int] = {
            OperationKind.RETRIEVAL: max(1, int(self.config.retrieval_limit)),
            OperationKind.EMBEDDING: max(1, int(self.config.embedding_limit)),
            OperationKind.RERANKER: max(1, int(self.config.reranker_limit)),
        }
        self._timeouts: dict[OperationKind, float] = {
            OperationKind.RETRIEVAL: float(self.config.retrieval_timeout_seconds),
            OperationKind.EMBEDDING: float(self.config.embedding_timeout_seconds),
            OperationKind.RERANKER: float(self.config.reranker_timeout_seconds),
        }
        self._semaphores: dict[OperationKind, asyncio.Semaphore] = {
            kind: asyncio.Semaphore(limit) for kind, limit in self._limits.items()
        }
        self._metrics: dict[OperationKind, OperationMetrics] = {
            kind: OperationMetrics(limit=limit) for kind, limit in self._limits.items()
        }

    def timeout_for(self, operation_kind: OperationKind | str, override: float | None = None) -> float:
        if override is not None:
            return max(0.0, float(override))
        return self._timeouts[self._normalize_kind(operation_kind)]

    def limit_for(self, operation_kind: OperationKind | str) -> int:
        return self._limits[self._normalize_kind(operation_kind)]

    def metrics_for(self, operation_kind: OperationKind | str) -> dict[str, int]:
        return self._metrics[self._normalize_kind(operation_kind)].to_dict()

    def snapshot(self) -> dict[str, Any]:
        operations = {kind.value: metrics.to_dict() for kind, metrics in self._metrics.items()}
        snapshot = ConcurrencySnapshot(
            default_policy=self.config.normalized_policy().value,
            config={
                "retrieval_limit": self._limits[OperationKind.RETRIEVAL],
                "embedding_limit": self._limits[OperationKind.EMBEDDING],
                "reranker_limit": self._limits[OperationKind.RERANKER],
                "retrieval_timeout_seconds": self._timeouts[OperationKind.RETRIEVAL],
                "embedding_timeout_seconds": self._timeouts[OperationKind.EMBEDDING],
                "reranker_timeout_seconds": self._timeouts[OperationKind.RERANKER],
            },
            operations=operations,
        ).to_dict()
        snapshot.update({
            "active_requests": sum(metrics["active"] for metrics in operations.values()),
            "peak_active_requests": sum(metrics["peak_active"] for metrics in operations.values()),
            "queued_requests": sum(metrics["queued"] for metrics in operations.values()),
        })
        return snapshot

    @asynccontextmanager
    async def acquire(
        self,
        operation_kind: OperationKind | str,
        *,
        policy: ConcurrencyPolicy | str | None = None,
    ):
        kind = self._normalize_kind(operation_kind)
        limit = self._limits[kind]
        semaphore = self._semaphores[kind]
        resolved_policy = ConcurrencyPolicy(policy) if policy is not None else self.config.normalized_policy()
        metrics = self._metrics[kind]

        if resolved_policy == ConcurrencyPolicy.FAIL_FAST and semaphore.locked():
            metrics.rejected += 1
            raise ConcurrencyLimitExceeded(kind, limit)

        if resolved_policy == ConcurrencyPolicy.QUEUE:
            metrics.queued += 1

        await semaphore.acquire()
        if resolved_policy == ConcurrencyPolicy.QUEUE:
            metrics.queued = max(0, metrics.queued - 1)

        metrics.active += 1
        metrics.peak_active = max(metrics.peak_active, metrics.active)
        metrics.total_started += 1
        try:
            yield
        finally:
            metrics.active = max(0, metrics.active - 1)
            semaphore.release()

    async def run(
        self,
        operation_kind: OperationKind | str,
        operation: Awaitable[T] | Callable[[], Awaitable[T]],
        *,
        timeout_seconds: float | None = None,
        policy: ConcurrencyPolicy | str | None = None,
    ) -> T:
        kind = self._normalize_kind(operation_kind)
        metrics = self._metrics[kind]
        timeout = self.timeout_for(kind, timeout_seconds)
        try:
            async with self.acquire(kind, policy=policy):
                awaitable = operation() if callable(operation) else operation
                result = await asyncio.wait_for(awaitable, timeout=timeout)
                metrics.completed += 1
                return result
        except asyncio.TimeoutError as exc:
            metrics.timed_out += 1
            raise OperationTimeoutError(kind, timeout) from exc
        except ConcurrencyControlError:
            raise
        except Exception:
            metrics.failed += 1
            raise

    async def run_with_policy(
        self,
        operation_kind: OperationKind | str,
        operation: Awaitable[T] | Callable[[], Awaitable[T]],
        *,
        timeout_seconds: float | None = None,
        policy: ConcurrencyPolicy | str = ConcurrencyPolicy.QUEUE,
    ) -> T:
        return await self.run(
            operation_kind,
            operation,
            timeout_seconds=timeout_seconds,
            policy=policy,
        )

    @staticmethod
    def _normalize_kind(operation_kind: OperationKind | str) -> OperationKind:
        if isinstance(operation_kind, OperationKind):
            return operation_kind
        return OperationKind(str(operation_kind))


class RetrievalConcurrencyController(ConcurrencyController):
    def __init__(
        self,
        *,
        max_concurrent_requests: int = 8,
        queue_timeout_seconds: float = 1.0,
        retrieval_timeout_seconds: float = 8.0,
    ):
        super().__init__(
            ConcurrencyConfig(
                retrieval_limit=max(1, int(max_concurrent_requests)),
                embedding_limit=max(1, int(max_concurrent_requests)),
                reranker_limit=max(1, int(max_concurrent_requests)),
                retrieval_timeout_seconds=retrieval_timeout_seconds,
                embedding_timeout_seconds=retrieval_timeout_seconds,
                reranker_timeout_seconds=retrieval_timeout_seconds,
                default_policy=ConcurrencyPolicy.QUEUE,
            )
        )
        self.queue_timeout_seconds = max(0.0, float(queue_timeout_seconds))
        self._queue_timeout_count = 0

    async def execute(self, connection_id: str, operation: Callable[[], Awaitable[T]] | Awaitable[T]) -> T:
        metrics = self._metrics[OperationKind.RETRIEVAL]
        semaphore = self._semaphores[OperationKind.RETRIEVAL]
        try:
            metrics.queued += 1
            await asyncio.wait_for(semaphore.acquire(), timeout=self.queue_timeout_seconds)
        except asyncio.TimeoutError as exc:
            metrics.queued = max(0, metrics.queued - 1)
            metrics.rejected += 1
            self._queue_timeout_count += 1
            raise RetrievalConcurrencyError(
                f"retrieval queue timeout after {self.queue_timeout_seconds:.3f} seconds"
            ) from exc

        metrics.queued = max(0, metrics.queued - 1)
        metrics.active += 1
        metrics.peak_active = max(metrics.peak_active, metrics.active)
        metrics.total_started += 1
        try:
            awaitable = operation() if callable(operation) else operation
            result = await asyncio.wait_for(awaitable, timeout=self.timeout_for(OperationKind.RETRIEVAL))
            metrics.completed += 1
            return result
        except asyncio.TimeoutError as exc:
            metrics.timed_out += 1
            raise RetrievalTimeoutError(OperationKind.RETRIEVAL, self.timeout_for(OperationKind.RETRIEVAL)) from exc
        except Exception:
            metrics.failed += 1
            raise
        finally:
            metrics.active = max(0, metrics.active - 1)
            semaphore.release()

    def snapshot(self) -> dict[str, Any]:
        snapshot = super().snapshot()
        retrieval_metrics = self._metrics[OperationKind.RETRIEVAL].to_dict()
        snapshot.update(
            {
                "queue_timeout_count": self._queue_timeout_count,
                "retrieval_timeout_count": retrieval_metrics["timed_out"],
                "rejected_requests": retrieval_metrics["rejected"],
                "completed_requests": retrieval_metrics["completed"],
            }
        )
        return snapshot
