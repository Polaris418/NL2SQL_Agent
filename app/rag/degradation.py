from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any

from app.schemas.rag import RAGIndexMode


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class DegradationEvent:
    connection_id: str
    event_type: str
    previous_mode: RAGIndexMode
    current_mode: RAGIndexMode
    reason: str | None = None
    observed_healthy: bool | None = None
    created_at: str = field(default_factory=_utcnow)


@dataclass(slots=True)
class DegradationState:
    connection_id: str
    mode: RAGIndexMode = RAGIndexMode.HYBRID
    degradation_count: int = 0
    recovery_count: int = 0
    observation_count: int = 0
    consecutive_healthy: int = 0
    consecutive_unhealthy: int = 0
    last_event_type: str | None = None
    last_reason: str | None = None
    last_transition_at: str | None = None
    last_observed_at: str | None = None

    @property
    def degraded(self) -> bool:
        return self.mode == RAGIndexMode.BM25_ONLY


class DegradationManager:
    """Lightweight BM25 degradation coordinator.

    The manager keeps per-connection state and records transitions between
    hybrid retrieval and BM25-only fallback when the vector store is
    unavailable or times out. Healthy probes can restore the connection back
    to hybrid mode after a configurable number of consecutive successes.
    """

    def __init__(
        self,
        *,
        recovery_threshold: int = 1,
        max_events: int = 1000,
    ):
        self.recovery_threshold = max(1, int(recovery_threshold))
        self.max_events = max(10, int(max_events))
        self._lock = RLock()
        self._events: deque[DegradationEvent] = deque(maxlen=self.max_events)
        self._states: dict[str, DegradationState] = {}

    def get_state(self, connection_id: str) -> DegradationState:
        with self._lock:
            return self._states.setdefault(connection_id, DegradationState(connection_id=connection_id))

    def current_mode(self, connection_id: str | None = None) -> RAGIndexMode:
        with self._lock:
            if connection_id is not None:
                return self._states.get(connection_id, DegradationState(connection_id=connection_id)).mode
            return self._global_mode_locked()

    def should_use_bm25_only(self, connection_id: str | None = None) -> bool:
        return self.current_mode(connection_id) == RAGIndexMode.BM25_ONLY

    def record_vector_store_unavailable(
        self,
        connection_id: str,
        *,
        reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RAGIndexMode:
        return self._record_transition(
            connection_id,
            event_type="vector_store_unavailable",
            reason=self._compose_reason(reason, details),
            force_bm25=True,
            observed_healthy=False,
        )

    def record_timeout(
        self,
        connection_id: str,
        *,
        reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RAGIndexMode:
        return self._record_transition(
            connection_id,
            event_type="timeout",
            reason=self._compose_reason(reason, details),
            force_bm25=True,
            observed_healthy=False,
        )

    def observe_vector_store_health(
        self,
        connection_id: str,
        *,
        available: bool,
        reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RAGIndexMode:
        return self._record_transition(
            connection_id,
            event_type="health_probe",
            reason=self._compose_reason(reason, details),
            force_bm25=not available,
            observed_healthy=available,
        )

    def mark_recovered(
        self,
        connection_id: str,
        *,
        reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RAGIndexMode:
        return self._record_transition(
            connection_id,
            event_type="recovered",
            reason=self._compose_reason(reason, details),
            force_bm25=False,
            observed_healthy=True,
            explicit_recovery=True,
        )

    def record_recovery_probe(
        self,
        connection_id: str,
        *,
        reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RAGIndexMode:
        return self.observe_vector_store_health(connection_id, available=True, reason=reason, details=details)

    def record_manual_mode(
        self,
        connection_id: str,
        mode: RAGIndexMode,
        *,
        reason: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> RAGIndexMode:
        if mode not in {RAGIndexMode.HYBRID, RAGIndexMode.BM25_ONLY, RAGIndexMode.VECTOR}:
            raise ValueError(f"Unsupported degradation mode: {mode}")
        return self._record_transition(
            connection_id,
            event_type="manual_override",
            reason=self._compose_reason(reason, details),
            explicit_mode=mode,
        )

    def recent_events(self, *, connection_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        limit = max(1, int(limit))
        with self._lock:
            items = list(self._events)
        if connection_id is not None:
            items = [event for event in items if event.connection_id == connection_id]
        return [self._event_to_dict(event) for event in items[-limit:]]

    def export_stats(self, connection_id: str | None = None) -> dict[str, Any]:
        with self._lock:
            if connection_id is not None:
                state = self._states.get(connection_id, DegradationState(connection_id=connection_id))
                event_count = sum(1 for event in self._events if event.connection_id == connection_id)
                degraded_connections = sum(1 for item in self._states.values() if item.degraded)
                return self._stats_payload(
                    state=state,
                    event_count=event_count,
                    degraded_connections=degraded_connections,
                    total_connections=len(self._states) or (1 if connection_id else 0),
                )

            states = list(self._states.values())
            total_connections = len(states)
            event_count = len(self._events)
            degraded_connections = sum(1 for item in states if item.degraded)
            if states:
                aggregate = DegradationState(connection_id="__aggregate__")
                aggregate.degradation_count = sum(item.degradation_count for item in states)
                aggregate.recovery_count = sum(item.recovery_count for item in states)
                aggregate.observation_count = sum(item.observation_count for item in states)
                aggregate.mode = self._global_mode_locked()
                aggregate.last_transition_at = max((item.last_transition_at for item in states if item.last_transition_at), default=None)
                aggregate.last_observed_at = max((item.last_observed_at for item in states if item.last_observed_at), default=None)
                return self._stats_payload(
                    state=aggregate,
                    event_count=event_count,
                    degraded_connections=degraded_connections,
                    total_connections=total_connections,
                )

            return self._stats_payload(
                state=DegradationState(connection_id="__aggregate__"),
                event_count=0,
                degraded_connections=0,
                total_connections=0,
            )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "total_connections": len(self._states),
                "current_mode": self._global_mode_locked().value,
                "degraded_connections": sum(1 for item in self._states.values() if item.degraded),
                "events": [self._event_to_dict(event) for event in self._events],
                "states": {connection_id: self._state_to_dict(state) for connection_id, state in self._states.items()},
            }

    def _record_transition(
        self,
        connection_id: str,
        *,
        event_type: str,
        reason: str | None = None,
        force_bm25: bool = False,
        observed_healthy: bool | None = None,
        explicit_recovery: bool = False,
        explicit_mode: RAGIndexMode | None = None,
    ) -> RAGIndexMode:
        with self._lock:
            state = self._states.setdefault(connection_id, DegradationState(connection_id=connection_id))
            previous_mode = state.mode
            state.observation_count += 1
            state.last_observed_at = _utcnow()
            state.last_event_type = event_type
            state.last_reason = reason

            next_mode = previous_mode
            if explicit_mode is not None:
                next_mode = explicit_mode
            elif force_bm25:
                next_mode = RAGIndexMode.BM25_ONLY
            elif observed_healthy is True:
                state.consecutive_healthy += 1
                state.consecutive_unhealthy = 0
                if previous_mode == RAGIndexMode.BM25_ONLY and state.consecutive_healthy >= self.recovery_threshold:
                    next_mode = RAGIndexMode.HYBRID
            elif observed_healthy is False:
                state.consecutive_unhealthy += 1
                state.consecutive_healthy = 0
                next_mode = RAGIndexMode.BM25_ONLY

            if next_mode == RAGIndexMode.BM25_ONLY:
                state.consecutive_healthy = 0 if observed_healthy is False or force_bm25 else state.consecutive_healthy
                if previous_mode != RAGIndexMode.BM25_ONLY:
                    state.degradation_count += 1
                    state.last_transition_at = _utcnow()
            elif next_mode == RAGIndexMode.HYBRID:
                if previous_mode == RAGIndexMode.BM25_ONLY:
                    state.recovery_count += 1
                    state.last_transition_at = _utcnow()
                    state.consecutive_unhealthy = 0
                    state.consecutive_healthy = 0
            else:
                state.consecutive_healthy = 0
                state.consecutive_unhealthy = 0
                if previous_mode != next_mode:
                    state.last_transition_at = _utcnow()

            state.mode = next_mode

            event = DegradationEvent(
                connection_id=connection_id,
                event_type=event_type if not explicit_recovery else "recovered",
                previous_mode=previous_mode,
                current_mode=state.mode,
                reason=reason,
                observed_healthy=observed_healthy,
            )
            self._events.append(event)
            return state.mode

    def _global_mode_locked(self) -> RAGIndexMode:
        if not self._states:
            return RAGIndexMode.HYBRID
        if any(state.degraded for state in self._states.values()):
            return RAGIndexMode.BM25_ONLY
        return RAGIndexMode.HYBRID

    @staticmethod
    def _compose_reason(reason: str | None, details: dict[str, Any] | None) -> str | None:
        if not details:
            return reason
        detail_text = ", ".join(f"{key}={value}" for key, value in sorted(details.items()))
        if reason:
            return f"{reason}; {detail_text}"
        return detail_text

    @staticmethod
    def _state_to_dict(state: DegradationState) -> dict[str, Any]:
        return {
            "connection_id": state.connection_id,
            "mode": state.mode.value,
            "degradation_count": state.degradation_count,
            "recovery_count": state.recovery_count,
            "observation_count": state.observation_count,
            "consecutive_healthy": state.consecutive_healthy,
            "consecutive_unhealthy": state.consecutive_unhealthy,
            "last_event_type": state.last_event_type,
            "last_reason": state.last_reason,
            "last_transition_at": state.last_transition_at,
            "last_observed_at": state.last_observed_at,
            "degraded": state.degraded,
        }

    @staticmethod
    def _event_to_dict(event: DegradationEvent) -> dict[str, Any]:
        return {
            "connection_id": event.connection_id,
            "event_type": event.event_type,
            "previous_mode": event.previous_mode.value,
            "current_mode": event.current_mode.value,
            "reason": event.reason,
            "observed_healthy": event.observed_healthy,
            "created_at": event.created_at,
        }

    @staticmethod
    def _stats_payload(
        *,
        state: DegradationState,
        event_count: int,
        degraded_connections: int,
        total_connections: int,
    ) -> dict[str, Any]:
        degradation_rate = round(state.degradation_count / state.observation_count, 4) if state.observation_count else 0.0
        return {
            "connection_id": state.connection_id if state.connection_id != "__aggregate__" else None,
            "current_mode": state.mode.value,
            "degradation_count": state.degradation_count,
            "recovery_count": state.recovery_count,
            "observation_count": state.observation_count,
            "degradation_rate": degradation_rate,
            "event_count": event_count,
            "degraded_connections": degraded_connections,
            "total_connections": total_connections,
            "last_transition_at": state.last_transition_at,
            "last_observed_at": state.last_observed_at,
        }


__all__ = [
    "DegradationEvent",
    "DegradationManager",
    "DegradationState",
]
