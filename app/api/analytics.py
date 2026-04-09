from fastapi import APIRouter

from app.core.dependencies import get_metadata_db
from app.db.repositories.history_repo import QueryHistoryRepository
from app.schemas.query import AnalyticsReport, AnalyticsSummary, ErrorDistributionItem, TopTableItem

router = APIRouter(tags=["analytics"])


@router.get("/analytics/summary", response_model=AnalyticsSummary)
async def analytics_summary() -> AnalyticsSummary:
    return QueryHistoryRepository(get_metadata_db()).analytics()


@router.get("/analytics/errors", response_model=list[ErrorDistributionItem])
async def analytics_errors() -> list[ErrorDistributionItem]:
    return QueryHistoryRepository(get_metadata_db()).error_distribution()


@router.get("/analytics/top-tables", response_model=list[TopTableItem])
async def analytics_top_tables() -> list[TopTableItem]:
    return QueryHistoryRepository(get_metadata_db()).top_tables()


@router.get("/analytics/report", response_model=AnalyticsReport)
async def analytics_report() -> AnalyticsReport:
    return QueryHistoryRepository(get_metadata_db()).analytics_report()
