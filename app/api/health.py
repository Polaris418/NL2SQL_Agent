from fastapi import APIRouter

from app.core.dependencies import get_service_container

router = APIRouter(tags=["health"])


@router.get("/health")
async def healthcheck() -> dict[str, object]:
    container = get_service_container()
    rag_metrics = container.rag_index_manager.get_metrics()
    return {
        "status": "ok",
        "app": container.settings.app_name,
        "version": container.settings.app_version,
        "cache": container.agent.cache_stats(),
        "rag_index": rag_metrics.model_dump(mode="json") if hasattr(rag_metrics, "model_dump") else rag_metrics.__dict__,
    }
