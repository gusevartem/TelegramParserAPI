from logging import getLogger

from dishka.integrations.fastapi import DishkaRoute
from fastapi import APIRouter, Body, File, Security, UploadFile, status
from parser.api.utils import CustomHTTPException, MessageResponse, secret_key_check
from parser.dto import Channel

router = APIRouter(
    prefix="/parser",
    tags=["parser"],
    route_class=DishkaRoute,
)

logger = getLogger(__name__)


@router.post("", status_code=status.HTTP_200_OK)
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


@router.post("/client", status_code=status.HTTP_201_CREATED)
async def add_client(
    archive: UploadFile = File(..., description="ZIP архив с файлом .session"),
    _: None = Security(secret_key_check),
) -> MessageResponse:
    logger.info(f"Received archive: {archive.filename}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


@router.post("/test", status_code=status.HTTP_201_CREATED)
async def test(
    channel_link: str = Body(..., description="Ссылка на канал", embed=True),
    _: None = Security(secret_key_check),
) -> Channel:
    logger.info(f"Received test request for channel link: {channel_link}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )
