from fastapi import APIRouter, Depends, HTTPException, status

from app.agent.utils import build_user_friendly_error, log_exception_details
from app.core.dependencies import get_db_manager, get_rag_index_manager
from app.db.connectors.base import DBConnectorError
from app.db.manager import DBManager
from app.schemas.connection import ConnectionCreate, ConnectionStatus, ConnectionTestResult, SchemaCacheEntry
from app.core.rag_index_manager import RAGIndexManager

router = APIRouter(tags=["connections"])


def _friendly_http_exception(
    exc: Exception,
    *,
    action: str,
    connection_id: str | None = None,
    status_code: int = status.HTTP_400_BAD_REQUEST,
    extra: dict[str, str] | None = None,
) -> HTTPException:
    log_extra: dict[str, str] = {"action": action}
    if connection_id is not None:
        log_extra["connection_id"] = connection_id
    if extra:
        log_extra.update(extra)

    if isinstance(exc, KeyError):
        log_exception_details(action, exc=exc, extra=log_extra)
        return HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "message": "Connection not found.",
                "error_type": "not_found",
                "suggestion": "Check the connection ID and try again.",
            },
        )

    error_type, friendly_message, suggestion = build_user_friendly_error(str(exc))
    log_exception_details(action, exc=exc, extra=log_extra)
    return HTTPException(
        status_code=status_code,
        detail={
            "message": friendly_message,
            "error_type": error_type,
            "suggestion": suggestion,
        },
    )


@router.post("/connections", response_model=ConnectionStatus, status_code=status.HTTP_201_CREATED)
async def create_connection(
    payload: ConnectionCreate,
    db_manager: DBManager = Depends(get_db_manager),
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> ConnectionStatus:
    try:
        connection_status = db_manager.create_connection(payload)
        if hasattr(rag_index_manager, "schedule_rebuild"):
            rag_index_manager.schedule_rebuild(connection_status.id)
        return connection_status
    except (DBConnectorError, ValueError) as exc:
        raise _friendly_http_exception(
            exc,
            action="Connection creation failed",
            status_code=status.HTTP_400_BAD_REQUEST,
            extra={"name": payload.name, "db_type": payload.db_type.value},
        ) from exc


@router.get("/connections", response_model=list[ConnectionStatus])
async def list_connections(db_manager: DBManager = Depends(get_db_manager)) -> list[ConnectionStatus]:
    return db_manager.list_connections()


@router.delete("/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: str,
    db_manager: DBManager = Depends(get_db_manager),
) -> None:
    if not db_manager.delete_connection(connection_id):
        raise HTTPException(status_code=404, detail="Connection not found")


@router.post("/connections/{connection_id}/sync", response_model=SchemaCacheEntry)
async def refresh_schema(
    connection_id: str,
    db_manager: DBManager = Depends(get_db_manager),
    rag_index_manager: RAGIndexManager = Depends(get_rag_index_manager),
) -> SchemaCacheEntry:
    try:
        schema_cache = db_manager.refresh_schema_cache(connection_id)
        if hasattr(rag_index_manager, "schedule_rebuild"):
            rag_index_manager.schedule_rebuild(connection_id)
        return schema_cache
    except (KeyError, DBConnectorError) as exc:
        raise _friendly_http_exception(
            exc,
            action="Schema refresh failed",
            connection_id=connection_id,
        ) from exc


@router.post("/connections/{connection_id}/test", response_model=ConnectionTestResult)
async def test_connection(connection_id: str, db_manager: DBManager = Depends(get_db_manager)) -> ConnectionTestResult:
    try:
        return db_manager.test_connection(connection_id)
    except (KeyError, DBConnectorError, ValueError) as exc:
        raise _friendly_http_exception(
            exc,
            action="Connection test failed",
            connection_id=connection_id,
        ) from exc


@router.get("/connections/{connection_id}/schema", response_model=SchemaCacheEntry)
async def get_schema(connection_id: str, db_manager: DBManager = Depends(get_db_manager)) -> SchemaCacheEntry:
    if hasattr(db_manager, "get_connection_status"):
        try:
            db_manager.get_connection_status(connection_id)
        except KeyError as exc:
            raise _friendly_http_exception(
                exc,
                action="Schema lookup failed",
                connection_id=connection_id,
            ) from exc
    schema_cache = db_manager.get_schema_cache(connection_id)
    if schema_cache is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "message": "Schema cache not found.",
                "error_type": "not_found",
                "suggestion": "Refresh the schema cache after verifying the connection.",
            },
        )
    return schema_cache
