from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, StreamingResponse

from app.agent.nl2sql_agent import NL2SQLAgent, TooManyRequestsError
from app.agent.utils import build_user_friendly_error, execution_rows_to_csv, log_exception_details
from app.core.dependencies import get_agent
from app.schemas.query import AgentStep, QueryRequest, QueryResult, SQLExecutionRequest

router = APIRouter(tags=["query"])


def _json_default(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)


@router.post("/query", response_model=QueryResult)
async def run_query(payload: QueryRequest, agent: NL2SQLAgent = Depends(get_agent)) -> QueryResult:
    try:
        return await agent.process_query(
            payload.question,
            payload.connection_id,
            page_number=payload.page_number,
            page_size=payload.page_size,
            include_total_count=payload.include_total_count,
            previous_query_id=payload.previous_query_id,
            follow_up_instruction=payload.follow_up_instruction,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except TooManyRequestsError as exc:
        raise HTTPException(status_code=429, detail={"message": str(exc), "error_type": "rate_limit"}) from exc
    except Exception as exc:  # noqa: BLE001
        log_exception_details("Query request failed", exc=exc, extra={"connection_id": payload.connection_id})
        error_type, friendly_message, suggestion = build_user_friendly_error(str(exc))
        return QueryResult(
            question=payload.question,
            rewritten_query=payload.question,
            retrieved_tables=[],
            sql="",
            result=None,
            chart=None,
            steps=[],
            status="failed",
            retry_count=0,
            error_message=friendly_message,
            error_type=error_type,
            error_suggestion=suggestion,
        )


@router.post("/query/stream")
async def stream_query(payload: QueryRequest, agent: NL2SQLAgent = Depends(get_agent)) -> StreamingResponse:
    async def event_stream():
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def on_step(step: AgentStep) -> None:
            await queue.put(f"event: step\ndata: {json.dumps(step.model_dump(mode='json'), ensure_ascii=False, default=_json_default)}\n\n")

        async def runner():
            try:
                result = await agent.process_query(
                    payload.question,
                    payload.connection_id,
                    page_number=payload.page_number,
                    page_size=payload.page_size,
                    include_total_count=payload.include_total_count,
                    previous_query_id=payload.previous_query_id,
                    follow_up_instruction=payload.follow_up_instruction,
                    step_handler=on_step,
                )
                await queue.put(
                    f"event: result\ndata: {json.dumps(result.model_dump(mode='json'), ensure_ascii=False, default=_json_default)}\n\n"
                )
            except TooManyRequestsError as exc:
                await queue.put(
                    f"event: error\ndata: {json.dumps({'message': str(exc), 'error_type': 'rate_limit'}, ensure_ascii=False)}\n\n"
                )
            except Exception as exc:  # noqa: BLE001
                log_exception_details("Streaming query failed", exc=exc, extra={"connection_id": payload.connection_id})
                await queue.put(f"event: error\ndata: {json.dumps({'message': str(exc)}, ensure_ascii=False)}\n\n")
            finally:
                await queue.put(None)

        task = asyncio.create_task(runner())
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            task.cancel()

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/query/sql", response_model=QueryResult)
async def execute_sql(payload: SQLExecutionRequest, agent: NL2SQLAgent = Depends(get_agent)) -> QueryResult:
    try:
        connector = agent.db_manager.get_connector(payload.connection_id)
        execution = agent.sql_executor.execute(
            connector,
            payload.sql,
            page_number=payload.page_number,
            page_size=payload.page_size,
            include_total_count=payload.include_total_count,
        )
        return QueryResult(
            question="Manual SQL execution",
            rewritten_query=payload.sql,
            sql=payload.sql,
            result=execution,
            chart=agent.chart_suggester.suggest(execution),
            status="success",
            db_latency_ms=execution.db_latency_ms,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Connection not found") from exc
    except TooManyRequestsError as exc:
        raise HTTPException(status_code=429, detail={"message": str(exc), "error_type": "rate_limit"}) from exc
    except Exception as exc:  # noqa: BLE001
        log_exception_details("Manual SQL execution failed", exc=exc, extra={"connection_id": payload.connection_id})
        error_type, friendly_message, suggestion = build_user_friendly_error(str(exc))
        return QueryResult(
            question="Manual SQL execution",
            rewritten_query=payload.sql,
            sql=payload.sql,
            result=None,
            chart=None,
            status="failed",
            error_message=friendly_message,
            error_type=error_type,
            error_suggestion=suggestion,
        )


@router.post("/query/export")
async def export_query(payload: QueryRequest, agent: NL2SQLAgent = Depends(get_agent)) -> Response:
    result = await run_query(payload, agent)
    if result.result is None:
        raise HTTPException(status_code=400, detail="Query did not produce exportable rows")
    csv_text = execution_rows_to_csv(result.result.columns, result.result.rows)
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="query-results.csv"'},
    )


@router.post("/query/sql/export")
async def export_sql(payload: SQLExecutionRequest, agent: NL2SQLAgent = Depends(get_agent)) -> Response:
    try:
        connector = agent.db_manager.get_connector(payload.connection_id)
        execution = agent.sql_executor.execute(
            connector,
            payload.sql,
            page_number=payload.page_number,
            page_size=payload.page_size,
            include_total_count=payload.include_total_count,
        )
        csv_text = execution_rows_to_csv(execution.columns, execution.rows)
        return Response(
            content=csv_text,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="query-results.csv"'},
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Connection not found") from exc
    except TooManyRequestsError as exc:
        raise HTTPException(status_code=429, detail={"message": str(exc), "error_type": "rate_limit"}) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
