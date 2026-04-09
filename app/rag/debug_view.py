from __future__ import annotations

from typing import Any, Mapping

from app.schemas.rag import (
    RAGDebugArtifacts,
    RAGDebugCandidateScore,
    RAGDebugQueryContext,
    RAGDebugTiming,
    RAGQueryDebugView,
)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, set):
        return [str(item) for item in sorted(value) if str(item).strip()]
    return [str(value)] if str(value).strip() else []


def _summary_line(label: str, value: Any) -> str:
    if value in (None, "", [], {}, ()):  # pragma: no branch - tiny helper
        return f"{label}: -"
    if isinstance(value, list):
        return f"{label}: {', '.join(str(item) for item in value)}"
    return f"{label}: {value}"


def _build_summary(payload: dict[str, Any], *, index_state: dict[str, Any] | None = None) -> str:
    lines = [
        _summary_line("Query", payload.get("original_query") or payload.get("query") or payload.get("rewritten_query")),
        _summary_line("Connection", payload.get("connection_id")),
        _summary_line("Selected tables", payload.get("selected_tables")),
        _summary_line("Candidate tables", [item.get("table_name") for item in payload.get("candidate_scores", []) if isinstance(item, Mapping)]),
        _summary_line("Failure", payload.get("failure_category") or payload.get("failure_stage")),
        _summary_line("Degradation", payload.get("degradation_mode")),
    ]
    if index_state:
        lines.append(_summary_line("Index status", index_state.get("index_status")))
        lines.append(_summary_line("Health status", index_state.get("health_status")))
    return "\n".join(lines).strip()


def _normalize_candidates(payload: dict[str, Any]) -> list[RAGDebugCandidateScore]:
    raw_candidates = payload.get("candidate_scores") or payload.get("candidates") or []
    normalized: list[RAGDebugCandidateScore] = []
    for index, item in enumerate(raw_candidates):
        candidate = _mapping(item)
        table_name = str(
            candidate.get("table_name")
            or candidate.get("table")
            or candidate.get("key")
            or candidate.get("name")
            or ""
        ).strip()
        if not table_name:
            continue
        source_scores = candidate.get("source_scores") or {}
        normalized.append(
            RAGDebugCandidateScore(
                table_name=table_name,
                score=float(candidate.get("score") or 0.0),
                source=str(candidate.get("source") or candidate.get("origin") or "") or None,
                rank=int(candidate.get("rank") or index + 1),
                source_scores={str(key): float(value) for key, value in _mapping(source_scores).items()},
                metadata={key: value for key, value in candidate.items() if key not in {"table_name", "table", "key", "name", "score", "source", "origin", "rank", "source_scores"}},
            )
        )
    return normalized


def _normalize_stage_latencies(payload: dict[str, Any]) -> dict[str, float]:
    raw = _mapping(payload.get("stage_latencies"))
    normalized: dict[str, float] = {}
    for key, value in raw.items():
        coerced = _coerce_float(value)
        if coerced is not None:
            normalized[str(key)] = coerced
    return normalized


def build_query_debug_view(
    query_id: str,
    *,
    query_log: dict[str, Any] | None = None,
    query_details: dict[str, Any] | None = None,
    runtime_metrics: dict[str, Any] | None = None,
    index_state: dict[str, Any] | None = None,
) -> RAGQueryDebugView:
    payload: dict[str, Any] = {}
    payload.update(_mapping(query_log))
    payload.update(_mapping(query_details))

    connection_id = str(payload.get("connection_id") or "").strip() or None
    selected_tables = _string_list(payload.get("selected_tables"))
    reranked_tables = _string_list(payload.get("reranked_tables"))
    candidates = _normalize_candidates(payload)
    query_context = RAGDebugQueryContext(
        query_id=query_id,
        connection_id=connection_id,
        original_query=payload.get("original_query") or payload.get("query"),
        rewritten_query=payload.get("rewritten_query"),
        expanded_query=payload.get("expanded_query"),
        applied_synonyms=[(str(item[0]), str(item[1])) for item in payload.get("applied_synonyms") or [] if isinstance(item, (list, tuple)) and len(item) == 2],
        selected_tables=selected_tables,
        reranked_tables=reranked_tables,
        candidate_count=_coerce_int(payload.get("candidate_count"), len(candidates)),
        selected_count=_coerce_int(payload.get("selected_count"), len(selected_tables)),
    )
    timings = RAGDebugTiming(
        retrieval_latency_ms=_coerce_float(payload.get("retrieval_latency_ms")),
        embedding_latency_ms=_coerce_float(payload.get("embedding_latency_ms")),
        stage_latencies=_normalize_stage_latencies(payload),
    )
    artifacts = RAGDebugArtifacts(
        prompt_schema=payload.get("prompt_schema"),
        final_sql=payload.get("final_sql"),
    )
    index_state = dict(index_state or {})
    runtime_metrics = dict(runtime_metrics or {})
    access_metadata = _mapping(payload.get("access") or payload.get("access_metadata"))
    input_validation = _mapping(payload.get("input_validation") or payload.get("validation"))

    return RAGQueryDebugView(
        query_id=query_id,
        connection_id=connection_id,
        summary=_build_summary(payload, index_state=index_state),
        query=query_context,
        candidates=candidates,
        timings=timings,
        artifacts=artifacts,
        failure_category=payload.get("failure_category"),
        failure_stage=payload.get("failure_stage"),
        degradation_mode=payload.get("degradation_mode"),
        cache_hit=_coerce_bool(payload.get("cache_hit")),
        used_fallback=_coerce_bool(payload.get("used_fallback")),
        index_state=index_state,
        runtime_metrics=runtime_metrics,
        access_metadata=access_metadata,
        input_validation=input_validation,
        raw_log=payload,
    )


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, "", 0, "0", "false", "False", "no", "No", "off", "Off"):
        return False
    return bool(value)


def build_debug_view_from_manager(manager: Any, query_id: str) -> RAGQueryDebugView | None:
    query_log = None
    getter = getattr(manager, "get_query_details", None)
    if callable(getter):
        query_log = getter(query_id)
    if query_log is None:
        return None
    runtime_metrics = {}
    runtime_getter = getattr(manager, "get_runtime_metrics", None)
    if callable(runtime_getter):
        runtime_metrics = runtime_getter() or {}
    index_state = {}
    connection_id = query_log.get("connection_id") if isinstance(query_log, Mapping) else None
    if connection_id:
        status_getter = getattr(manager, "get_status", None)
        if callable(status_getter):
            status = status_getter(connection_id)
            if hasattr(status, "model_dump"):
                index_state = status.model_dump(mode="json")
            elif hasattr(status, "to_dict"):
                index_state = status.to_dict()
            elif isinstance(status, Mapping):
                index_state = dict(status)
    return build_query_debug_view(
        query_id,
        query_log=query_log if isinstance(query_log, dict) else dict(query_log or {}),
        runtime_metrics=runtime_metrics if isinstance(runtime_metrics, dict) else dict(runtime_metrics or {}),
        index_state=index_state,
    )


__all__ = [
    "build_debug_view_from_manager",
    "build_query_debug_view",
]
