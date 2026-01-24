import json
from logging import getLogger
from typing import Annotated, Any

from dishka.integrations.fastapi import DishkaRoute
from fastapi import APIRouter, Body, File, Form, Security, UploadFile, status
from parser.api.utils import CustomHTTPException, MessageResponse, secret_key_check
from parser.dto import Channel, ProxySettings, TelegramCredentials
from pydantic import BaseModel, BeforeValidator

router = APIRouter(
    prefix="/parser",
    tags=["parser"],
    route_class=DishkaRoute,
)

logger = getLogger(__name__)


@router.post("", status_code=status.HTTP_200_OK)
async def parse_channel(
    channel_link: str = Body(..., description="Ссылка на канал", embed=True),
    _: None = Security(secret_key_check),
) -> Channel:
    logger.info(f"Received test request for channel link: {channel_link}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


@router.post("/schedule", status_code=status.HTTP_200_OK)
async def add_channel(
    channel_link: str = Body(..., description="Ссылка на канал", embed=True),
    _: None = Security(secret_key_check),
) -> Channel:
    logger.info(f"Received add channel request for channel link: {channel_link}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


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


@router.post("/client", status_code=status.HTTP_201_CREATED)
async def add_client(
    client_settings: Annotated[
        Annotated[AddClientRequest, BeforeValidator(validate_json)], Form(...)
    ],
    session: UploadFile = File(..., description="Файл сессии"),
    _: None = Security(secret_key_check),
) -> MessageResponse:
    logger.info(f"Received session: {session.filename}")
    logger.info(f"Received client settings: {client_settings}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )
