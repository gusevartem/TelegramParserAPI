import json
import time
from logging import getLogger
from typing import Annotated, Any
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Body, File, Form, Security, UploadFile, status
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

logger = getLogger(__name__)


@router.get("/task", status_code=status.HTTP_200_OK)
async def get_task(
    parsing_task_dao: FromDishka[ParsingTaskDAO],
    task_id: UUID = Body(..., description="Идентификатор задачи", embed=True),
    _: None = Security(secret_key_check),
) -> ParsingTask:
    logger.info(f"⌛ Received get task request for task id: {task_id}")

    start_time = time.perf_counter()
    result = await parsing_task_dao.find_by_id(task_id)

    if result is None:
        raise CustomHTTPException(
            error="TaskNotFound",
            status_code=status.HTTP_404_NOT_FOUND,
            message=f"Task with id {task_id} not found",
        )
    duration = (time.perf_counter() - start_time) * 1000
    logger.info(
        f"✅ Get task request for task id: {task_id} completed. "
        + f"Duration: {duration:.0f}ms"
    )
    next_run = calculate_next_run(
        bucket=result.bucket,
        last_parsed_at=result.last_parsed_at,
        created_at=result.created_at,
        status=result.status,
    )
    return ParsingTask.from_persistence(
        result, int(next_run.timestamp()) if next_run is not None else None
    )


@router.post("/schedule", status_code=status.HTTP_201_CREATED)
async def add_channel(
    add_task: FromDishka[AddTask],
    channel_url: str = Body(..., description="Ссылка на канал", embed=True),
    _: None = Security(secret_key_check),
) -> ParsingTask:
    logger.info(f"⌛ Received add channel request for channel link: {channel_url}")

    start_time = time.perf_counter()
    result = await add_task(channel_url)
    duration = (time.perf_counter() - start_time) * 1000
    logger.info(
        f"✅ Add channel request for channel link: {channel_url} completed. "
        + f"Duration: {duration:.0f}ms"
    )

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
    logger.info(
        f"⌛ Received add client request with session: {session.filename}. "
        + "Proxy: "
        + f"{'Provided' if client_settings.proxy is not None else 'Not provided'}. "
        + "Credentials: "
        + f"{'Provided' if client_settings.credentials is not None else 'Use default'}"
    )
    try:
        start_time = time.perf_counter()
        await telegram.add_client(
            session.file.read(),
            credentials=client_settings.credentials,
            proxy=client_settings.proxy,
        )
        duration = (time.perf_counter() - start_time) * 1000
        logger.info(
            f"✅ Add client request with session: {session.filename} completed. "
            + f"Duration: {duration:.0f}ms"
        )
    except (InvalidClient, ClientBanned) as e:
        raise CustomHTTPException.from_exception(e, status.HTTP_400_BAD_REQUEST) from e

    return MessageResponse(message="Client added successfully")
