from logging import getLogger
from typing import Literal
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute
from fastapi import APIRouter, status
from parser.api.utils import CustomHTTPException
from parser.dto import Channel, ChannelMessage, ChannelStatistic, MediaWithLink

router = APIRouter(
    prefix="/public",
    tags=["public"],
    route_class=DishkaRoute,
)

logger = getLogger(__name__)


@router.get("/media", status_code=status.HTTP_200_OK)
async def get_media(media_id: UUID) -> MediaWithLink:
    logger.info(f"Received get media request for media id: {media_id}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


@router.get("/channel", status_code=status.HTTP_200_OK)
async def get_channel(
    channel_id: int,
) -> Channel:
    logger.info(f"Received get channel request for channel with id: {channel_id}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


@router.get("/channel/ids", status_code=status.HTTP_200_OK)
async def get_channel_ids() -> list[int]:
    logger.info("Received get channel ids request")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


@router.get("/channel/statistics", status_code=status.HTTP_200_OK)
async def get_channel_statistics(
    channel_id: int,
    sorting: Literal["newest", "oldest"] = "newest",
    skip: int = 0,
    limit: int | None = None,
) -> list[ChannelStatistic]:
    logger.info(
        f"Received get channel statistics request. sorting={sorting}, "
        + f"channel_id={channel_id}, skip={skip}, limit={limit}"
    )
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


@router.get("/channel/messages", status_code=status.HTTP_200_OK)
async def get_channel_messages(
    channel_id: int,
    skip: int = 0,
    limit: int | None = None,
) -> list[ChannelMessage]:
    logger.info(
        "Received get channel messages request."
        + f"channel_id={channel_id}, skip={skip}, limit={limit}"
    )
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )


@router.get("/message", status_code=status.HTTP_200_OK)
async def get_message(
    message_id: int,
) -> ChannelMessage:
    logger.info(f"Received get message request for message with id: {message_id}")
    raise CustomHTTPException(
        error="NotImplemented",
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        message="Method not implemented",
    )
