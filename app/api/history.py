from fastapi import APIRouter

from app.agent.utils import build_user_friendly_error
from app.core.dependencies import get_metadata_db
from app.db.repositories.history_repo import QueryHistoryRepository
from app.schemas.query import FollowUpContext, QueryHistoryDetail, QueryHistoryItem

router = APIRouter(tags=["history"])


@router.get("/history", response_model=list[QueryHistoryItem])
async def list_history(limit: int = 50, offset: int = 0) -> list[QueryHistoryItem]:
    items = QueryHistoryRepository(get_metadata_db()).list_history(limit=limit, offset=offset)
    return [
        item.model_copy(
            update={
                "error_type": build_user_friendly_error(item.error_message)[0] if item.error_message else None,
                "error_suggestion": build_user_friendly_error(item.error_message)[2] if item.error_message else None,
            }
        )
        for item in items
    ]


@router.get("/history/{query_id}", response_model=QueryHistoryDetail)
async def get_history_detail(query_id: str) -> QueryHistoryDetail:
    detail = QueryHistoryRepository(get_metadata_db()).get_history(query_id)
    if detail is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="History item not found")
    error_type, _, suggestion = build_user_friendly_error(detail.error_message) if detail.error_message else (None, None, None)
    return detail.model_copy(update={"error_type": error_type, "error_suggestion": suggestion})


@router.get("/history/{query_id}/context", response_model=FollowUpContext)
async def get_follow_up_context(query_id: str) -> FollowUpContext:
    context = QueryHistoryRepository(get_metadata_db()).build_follow_up_context(query_id)
    if context is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="History item not found")
    return context


@router.delete("/history/{query_id}")
async def delete_history(query_id: str) -> dict[str, str]:
    """删除单条查询历史记录"""
    success = QueryHistoryRepository(get_metadata_db()).delete_history(query_id)
    if not success:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="History item not found")
    return {"message": "History deleted successfully", "query_id": query_id}


@router.delete("/history")
async def delete_all_history() -> dict[str, str | int]:
    """删除所有查询历史记录"""
    count = QueryHistoryRepository(get_metadata_db()).delete_all_history()
    return {"message": "All history deleted successfully", "deleted_count": count}
