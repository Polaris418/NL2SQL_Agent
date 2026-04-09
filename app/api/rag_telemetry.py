from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.dependencies import get_rag_index_manager
from app.core.rag_index_manager import RAGIndexManager
from app.rag.debug_view import build_debug_view_from_manager
from app.schemas.rag import (
    RAGAcceptanceResult,
    RAGDegradationEventRecord,
    RAGDegradationSnapshot,
    RAGQueryDebugView,
    RAGTelemetryDashboard,
    RAGTelemetryEventRecord,
    RAGTelemetrySnapshot,
    RAGTelemetrySummary,
    RAGHealthReport,
    RAGIndexBuildRequest,
    RAGIndexHealthDetail,
    RAGIndexJob,
    RAGIndexMetrics,
    RAGIndexState,
    RAGStabilityResult,
    SchemaVersionDiffResponse,
    SchemaVersionRecordResponse,
    SchemaVersionResponse,
)

router = APIRouter(tags=["rag"])


@router.get("/rag/index/status", response_model=list[RAGIndexState])
async def list_index_statuses(rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> list[RAGIndexState]:
    return rag_index_manager.list_statuses()


@router.get("/rag/index/status/{connection_id}", response_model=RAGIndexState)
async def get_index_status(connection_id: str, rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> RAGIndexState:
    state = rag_index_manager.get_status(connection_id)
    if state is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Index state not found"})
    return state


@router.post("/rag/index/{connection_id}/rebuild", response_model=RAGIndexState)
async def rebuild_index(
    connection_id: str,
    payload: RAGIndexBuildRequest | None = None,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> RAGIndexState:
    return rag_index_manager.schedule_rebuild(connection_id, payload)


@router.get("/rag/index/jobs", response_model=list[RAGIndexJob])
async def list_index_jobs(
    connection_id: str | None = None,
    limit: int = 20,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> list[RAGIndexJob]:
    return rag_index_manager.list_index_jobs(connection_id=connection_id, limit=limit)


@router.get("/rag/index/health/{connection_id}", response_model=RAGIndexHealthDetail)
async def get_index_health_detail(
    connection_id: str,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> RAGIndexHealthDetail:
    return rag_index_manager.get_index_health_detail(connection_id)


@router.get("/rag/schema-version/{connection_id}", response_model=SchemaVersionResponse)
async def get_schema_version(
    connection_id: str,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> SchemaVersionResponse:
    return rag_index_manager.get_schema_version(connection_id)


@router.get("/rag/schema-version/{connection_id}/history", response_model=list[SchemaVersionRecordResponse])
async def list_schema_versions(
    connection_id: str,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> list[SchemaVersionRecordResponse]:
    return rag_index_manager.list_schema_versions(connection_id)


@router.get("/rag/schema-version/{connection_id}/{version}", response_model=SchemaVersionRecordResponse)
async def get_schema_version_detail(
    connection_id: str,
    version: str,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> SchemaVersionRecordResponse:
    payload = rag_index_manager.get_schema_version_detail(connection_id, version)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Schema version not found"})
    return payload


@router.get("/rag/schema-version/{connection_id}/{left_version}/diff", response_model=SchemaVersionDiffResponse)
async def diff_schema_versions(
    connection_id: str,
    left_version: str,
    right_version: str | None = None,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> SchemaVersionDiffResponse:
    return rag_index_manager.diff_schema_versions(connection_id, left_version=left_version, right_version=right_version)


@router.get("/rag/metrics", response_model=RAGIndexMetrics)
async def get_metrics(rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> RAGIndexMetrics:
    return rag_index_manager.get_metrics()


@router.get("/rag/runtime", response_model=dict[str, Any])
async def get_runtime_metrics(rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> dict[str, Any]:
    return rag_index_manager.get_runtime_metrics()


@router.get("/rag/telemetry/dashboard", response_model=RAGTelemetryDashboard)
async def get_rag_telemetry_dashboard(
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> RAGTelemetryDashboard:
    return RAGTelemetryDashboard(**rag_index_manager.get_telemetry_dashboard())


@router.get("/rag/telemetry/history", response_model=list[RAGTelemetrySnapshot])
async def list_rag_telemetry_history(
    limit: int = 50,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> list[RAGTelemetrySnapshot]:
    return [RAGTelemetrySnapshot(**item) for item in rag_index_manager.list_telemetry_history(limit=limit)]


@router.get("/rag/telemetry/events", response_model=list[RAGTelemetryEventRecord])
async def list_rag_telemetry_events(
    connection_id: str | None = None,
    query_id: str | None = None,
    limit: int = 50,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> list[RAGTelemetryEventRecord]:
    events = rag_index_manager.get_telemetry_events(connection_id=connection_id, query_id=query_id, limit=limit)
    return [RAGTelemetryEventRecord(**event) if not isinstance(event, RAGTelemetryEventRecord) else event for event in events]


@router.get("/rag/telemetry/events/{query_id}", response_model=RAGTelemetryEventRecord)
async def get_rag_telemetry_event(
    query_id: str,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> RAGTelemetryEventRecord:
    event = rag_index_manager.get_telemetry_event(query_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Telemetry event not found"})
    return RAGTelemetryEventRecord(**event) if not isinstance(event, RAGTelemetryEventRecord) else event


@router.get("/rag/telemetry/summary", response_model=RAGTelemetrySummary)
async def get_rag_telemetry_summary(
    connection_id: str | None = None,
    limit: int = 20,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> RAGTelemetrySummary:
    runtime_metrics = rag_index_manager.get_runtime_metrics()
    telemetry_metrics = dict(runtime_metrics.get("telemetry") or {})
    recent_events = rag_index_manager.get_telemetry_events(connection_id=connection_id, limit=limit)
    return RAGTelemetrySummary(
        metrics=telemetry_metrics,
        recent_events=[RAGTelemetryEventRecord(**event) if not isinstance(event, RAGTelemetryEventRecord) else event for event in recent_events],
    )


@router.get("/rag/degradation", response_model=RAGDegradationSnapshot)
async def get_rag_degradation_snapshot(
    connection_id: str | None = None,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> RAGDegradationSnapshot:
    return rag_index_manager.get_degradation_snapshot(connection_id)


@router.get("/rag/degradation/events", response_model=list[RAGDegradationEventRecord])
async def list_rag_degradation_events(
    connection_id: str | None = None,
    limit: int = 50,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> list[RAGDegradationEventRecord]:
    return rag_index_manager.list_degradation_events(connection_id=connection_id, limit=limit)


@router.get("/rag/logs", response_model=list[dict[str, Any]])
async def list_rag_logs(
    connection_id: str | None = None,
    limit: int = 50,
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> list[dict[str, Any]]:
    return rag_index_manager.list_query_logs(connection_id=connection_id, limit=limit)


@router.get("/rag/query/{query_id}", response_model=dict[str, Any])
async def get_rag_query_details(query_id: str, rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> dict[str, Any]:
    payload = rag_index_manager.get_query_details(query_id)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Query log not found"})
    return payload


@router.get("/rag/debug/{query_id}", response_model=RAGQueryDebugView)
async def get_rag_debug_view(query_id: str, rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> RAGQueryDebugView:
    view = build_debug_view_from_manager(rag_index_manager, query_id)
    if view is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"message": "Query log not found"})
    return view


@router.get("/rag/acceptance", response_model=RAGAcceptanceResult)
async def get_rag_acceptance(rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> RAGAcceptanceResult:
    runtime_metrics = rag_index_manager.get_runtime_metrics()
    telemetry = dict(runtime_metrics.get("telemetry") or {})
    return rag_index_manager.evaluate_acceptance(
        {
            "recall_at_5": telemetry.get("recall_at_5", 0.0),
            "mrr": telemetry.get("mrr", 0.0),
            "table_not_found_rate": telemetry.get("table_not_found_rate", 0.0),
        }
    )


@router.get("/rag/stability", response_model=RAGStabilityResult)
async def get_rag_stability(rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> RAGStabilityResult:
    return rag_index_manager.evaluate_stability()


@router.get("/rag/health", response_model=RAGHealthReport)
async def get_rag_health(rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager)) -> RAGHealthReport:
    return RAGHealthReport(metrics=rag_index_manager.get_metrics(), connections=rag_index_manager.list_statuses())
