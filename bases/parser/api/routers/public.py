from logging import getLogger
from typing import Literal
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, status
from parser.api.utils import CustomHTTPException, ErrorResponse
from parser.dto import (
    Channel,
    ChannelMessage,
    ChannelStatistic,
    Media,
    MediaWithLink,
)
from parser.persistence import (
    ChannelDAO,
    ChannelMessageDAO,
    ChannelStatisticDAO,
    MediaDAO,
)
from parser.storage import IStorage

router = APIRouter(
    prefix="/public",
    tags=["public"],
    route_class=DishkaRoute,
)

logger = getLogger(__name__)


@router.get(
    "/media",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse, "description": "Медиа не найдено в базе данных"},
    },
)
async def get_media(
    media_id: UUID, storage: FromDishka[IStorage], media_dao: FromDishka[MediaDAO]
) -> MediaWithLink:
    logger.info(f"Received get media request for media id: {media_id}")

    media = await media_dao.find_by_id(media_id)
    if media is None:
        raise CustomHTTPException(
            error="MediaNotFound",
            status_code=status.HTTP_404_NOT_FOUND,
            message=f"Media with id {media_id} not found",
        )

    url = await storage.generate_presigned_url(media.file_name)

    result = MediaWithLink.from_media(Media.from_persistence(media), url)

    logger.info(f"Get media request for media id: {media_id} completed")
    return result


@router.get(
    "/channel",
    status_code=status.HTTP_200_OK,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Канал/последняя статистика канала не найдены в базе данных",
        },
    },
)
async def get_channel(
    channel_id: int,
    channel_dao: FromDishka[ChannelDAO],
    channel_statistics_dao: FromDishka[ChannelStatisticDAO],
) -> Channel:
    logger.info(f"Received get channel request for channel with id: {channel_id}")

    channel = await channel_dao.find_by_id_with_loaded_logo(channel_id)
    if channel is None:
        raise CustomHTTPException(
            error="ChannelNotFound",
            status_code=status.HTTP_404_NOT_FOUND,
            message=f"Channel with id {channel_id} not found",
        )

    newest_statistic = await channel_statistics_dao.get_latest_by_channel_id(channel_id)

    if newest_statistic is None:
        raise CustomHTTPException(
            error="LatestChannelStatisticNotFound",
            status_code=status.HTTP_404_NOT_FOUND,
            message=f"Latest statistic for channel with id {channel_id} not found",
        )

    result = Channel.from_persistence(channel, newest_statistic)

    logger.info(f"Get channel request for channel with id: {channel_id} completed")
    return result


@router.get("/channel/ids", status_code=status.HTTP_200_OK)
async def get_channel_ids(channel_dao: FromDishka[ChannelDAO]) -> list[int]:
    logger.info("Received get channel ids request")

    result = await channel_dao.get_ids()

    logger.info("Get channel ids request completed")
    return result


@router.get("/channel/statistics", status_code=status.HTTP_200_OK)
async def get_channel_statistics(
    channel_id: int,
    channel_statistics_dao: FromDishka[ChannelStatisticDAO],
    sorting: Literal["newest", "oldest"] = "newest",
    skip: int = 0,
    limit: int | None = None,
) -> list[ChannelStatistic]:
    logger.info(
        f"Received get channel statistics request. sorting={sorting}, "
        + f"channel_id={channel_id}, skip={skip}, limit={limit}"
    )

    statistics = await channel_statistics_dao.get_channel_statistics(
        channel_id, sorting, skip, limit
    )

    result = [ChannelStatistic.from_persistence(statistic) for statistic in statistics]

    logger.info(
        "Get channel statistics request "
        + f"for channel with id: {channel_id} completed"
    )
    return result


@router.get("/channel/messages", status_code=status.HTTP_200_OK)
async def get_channel_messages(
    channel_id: int,
    channel_message_dao: FromDishka[ChannelMessageDAO],
    sorting: Literal["newest", "oldest"] = "newest",
    skip: int = 0,
    limit: int | None = None,
) -> list[ChannelMessage]:
    logger.info(
        f"Received get channel messages request. sorting={sorting}, "
        + f"channel_id={channel_id}, skip={skip}, limit={limit}"
    )

    messages = await channel_message_dao.get_channel_messages(
        channel_id, sorting, skip, limit
    )

    result = [ChannelMessage.from_persistence(message) for message in messages]

    logger.info(
        "Get channel messages request " + f"for channel with id: {channel_id} completed"
    )
    return result


@router.get(
    "/message",
    status_code=status.HTTP_200_OK,
    responses={
        404: {
            "model": ErrorResponse,
            "description": "Сообщение не найдено в базе данных",
        },
    },
)
async def get_message(
    message_id: int,
    channel_message_dao: FromDishka[ChannelMessageDAO],
) -> ChannelMessage:
    logger.info(f"Received get message request for message with id: {message_id}")

    message = await channel_message_dao.find_by_id(message_id)
    if message is None:
        raise CustomHTTPException(
            error="ChannelMessageNotFound",
            status_code=status.HTTP_404_NOT_FOUND,
            message=f"Message with id {message_id} not found",
        )

    result = ChannelMessage.from_persistence(message)

    logger.info(f"Get message request for message with id: {message_id} completed")
    return result
