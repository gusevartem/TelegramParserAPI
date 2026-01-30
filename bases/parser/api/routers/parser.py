import json
from typing import Annotated, Any, Final
from uuid import UUID

import structlog
from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Body, File, Form, Security, UploadFile, status
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from parser.api.utils import (
    CustomHTTPException,
    ErrorResponse,
    MessageResponse,
    secret_key_check,
)
from parser.dto import ParsingTask, ProxySettings, TelegramCredentials
from parser.persistence import ParsingTaskDAO
from parser.scheduler import AddTask, calculate_next_run
from parser.telegram import ClientBanned, InvalidClient, ITelegram
from pydantic import BaseModel, BeforeValidator

router = APIRouter(
    prefix="/parser",
    tags=["parser"],
    route_class=DishkaRoute,
)

logger: Final[structlog.BoundLogger] = structlog.get_logger("api.parser")
tracer: Final[trace.Tracer] = trace.get_tracer("api.parser")


@router.get("/task", status_code=status.HTTP_200_OK)
async def get_task(
    parsing_task_dao: FromDishka[ParsingTaskDAO],
    task_id: UUID = Body(..., description="Идентификатор задачи", embed=True),
    _: None = Security(secret_key_check),
) -> ParsingTask:
    with tracer.start_as_current_span("api.get_task") as span:
        request_logger = logger.bind(task_id=task_id)
        span.set_attribute("task.id", str(task_id))
        request_logger.info("received_get_task_request", stage="start")

        result = await parsing_task_dao.find_by_id(task_id)

        if result is None:
            request_logger.info("task_not_found", stage="error")
            span.set_status(status=Status(StatusCode.ERROR, "Task not found"))
            raise CustomHTTPException(
                error="TaskNotFound",
                status_code=status.HTTP_404_NOT_FOUND,
                message=f"Task with id {task_id} not found",
            )
        request_logger.info("task_found", stage="success", id=result.id, url=result.url)
        span.set_attribute("task.id", str(result.id))
        span.set_attribute("task.url", result.url)
        span.add_event("task_found")
        next_run = calculate_next_run(
            bucket=result.bucket,
            last_parsed_at=result.last_parsed_at,
            created_at=result.created_at,
            status=result.status,
        )
        if next_run:
            span.set_attribute("task.next_run", str(next_run))
        return ParsingTask.from_persistence(
            result, int(next_run.timestamp()) if next_run is not None else None
        )


@router.post("/schedule", status_code=status.HTTP_201_CREATED)
async def add_channel(
    add_task: FromDishka[AddTask],
    channel_url: str = Body(..., description="Ссылка на канал", embed=True),
    _: None = Security(secret_key_check),
) -> ParsingTask:
    with tracer.start_as_current_span("api.add_channel") as span:
        request_logger = logger.bind(channel_url=channel_url)
        span.set_attribute("channel.url", channel_url)
        request_logger.info("received_add_channel_request", stage="start")
        result = await add_task(channel_url)

        request_logger.info(
            "channel_added", stage="success", id=result.id, url=result.url
        )
        span.set_attribute("task.id", str(result.id))
        span.set_attribute("task.url", result.url)
        span.add_event("channel_added")

        return result


class AddClientRequest(BaseModel):
    credentials: TelegramCredentials | None
    proxy: ProxySettings | None


def validate_json(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")
    return v


@router.post(
    "/client",
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Клиент не валиден",
        },
    },
)
async def add_client(
    client_settings: Annotated[
        Annotated[AddClientRequest, BeforeValidator(validate_json)], Form(...)
    ],
    telegram: FromDishka[ITelegram],
    session: UploadFile = File(..., description="Файл сессии"),
    _: None = Security(secret_key_check),
) -> MessageResponse:
    with tracer.start_as_current_span("api.add_client") as span:
        request_logger = logger.bind(session_filename=session.filename or "none")
        span.set_attribute("session.filename", session.filename or "none")
        span.set_attribute(
            "credentials.provided",
            client_settings.credentials is not None,
        )
        span.set_attribute("proxy.provided", client_settings.proxy is not None)
        request_logger.info("received_add_client_request", stage="start")
        try:
            await telegram.add_client(
                session.file.read(),
                credentials=client_settings.credentials,
                proxy=client_settings.proxy,
            )
            request_logger.info("client_added", stage="success")
        except (InvalidClient, ClientBanned) as e:
            request_logger.error("client_add_error", stage="error", exc_info=True)
            span.set_status(status=Status(StatusCode.ERROR, str(e)))
            span.record_exception(e)
            raise CustomHTTPException.from_exception(
                e, status.HTTP_400_BAD_REQUEST
            ) from e

        return MessageResponse(message="Client added successfully")
