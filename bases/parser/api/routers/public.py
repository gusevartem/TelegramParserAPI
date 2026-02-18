from typing import Final, Literal
from uuid import UUID

import structlog
from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, status
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from parser.api.settings import APISettings
from parser.api.utils import CustomHTTPException, ErrorResponse
from parser.dto import (
    Channel,
    ChannelMessage,
    ChannelStatistic,
    Media,
    MediaWithURL,
)
from parser.persistence import (
    ChannelDAO,
    ChannelMessageDAO,
    ChannelStatisticDAO,
    MediaDAO,
)
from parser.storage import IStorage
from yarl import URL

router = APIRouter(
    prefix="/public",
    tags=["public"],
    route_class=DishkaRoute,
)

logger: Final[structlog.BoundLogger] = structlog.get_logger("api.public")
tracer: Final[trace.Tracer] = trace.get_tracer("api.public")


@router.get(
    "/media",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"model": ErrorResponse, "description": "Медиа не найдено в базе данных"},
    },
)
async def get_media(
    media_id: UUID,
    storage: FromDishka[IStorage],
    media_dao: FromDishka[MediaDAO],
    api_settings: FromDishka[APISettings],
) -> MediaWithURL:
    with tracer.start_as_current_span("api.get_media") as span:
        request_logger = logger.bind(media_id=media_id)
        span.set_attribute("media.id", str(media_id))
        request_logger.info("received_get_media_request", stage="start")

        media = await media_dao.find_by_id(media_id)

        if media is None:
            request_logger.error("media_not_found", stage="error")
            span.set_status(status=Status(StatusCode.ERROR, "Media not found"))
            raise CustomHTTPException(
                error="MediaNotFound",
                status_code=status.HTTP_404_NOT_FOUND,
                message=f"Media with id {media_id} not found",
            )

        request_logger.info("media_found", id=media.id, file_name=media.file_name)
        request_logger.info("generating_presigned_url", stage="start")
        url = await storage.generate_presigned_url(media.file_name)

        if api_settings.s3_public_url is not None:
            internal_url = URL(url)
            public_base = URL(api_settings.s3_public_url)
            url = str(
                public_base.with_path(internal_url.path).with_query(internal_url.query)
            )

        result = MediaWithURL.from_media(Media.from_persistence(media), url)
        request_logger.info("get_media_request_completed", stage="complete")

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
    with tracer.start_as_current_span("api.get_channel") as span:
        request_logger = logger.bind(channel_id=channel_id)
        span.set_attribute("channel.id", str(channel_id))
        request_logger.info("received_get_channel_request", stage="start")

        channel = await channel_dao.find_by_id_with_loaded_logo(channel_id)
        if channel is None:
            request_logger.error("channel_not_found", stage="error")
            span.set_status(status=Status(StatusCode.ERROR, "Channel not found"))
            raise CustomHTTPException(
                error="ChannelNotFound",
                status_code=status.HTTP_404_NOT_FOUND,
                message=f"Channel with id {channel_id} not found",
            )

        request_logger.info("getting_latest_channel_statistic")
        newest_statistic = await channel_statistics_dao.get_latest_by_channel_id(
            channel_id
        )

        if newest_statistic is None:
            request_logger.error("latest_channel_statistic_not_found", stage="error")
            span.set_status(
                status=Status(StatusCode.ERROR, "Latest channel statistic not found")
            )
            raise CustomHTTPException(
                error="LatestChannelStatisticNotFound",
                status_code=status.HTTP_404_NOT_FOUND,
                message=f"Latest statistic for channel with id {channel_id} not found",
            )

        result = Channel.from_persistence(channel, newest_statistic)

        request_logger.info("get_channel_request_completed", stage="complete")
        return result


@router.get("/channel/ids", status_code=status.HTTP_200_OK)
async def get_channel_ids(channel_dao: FromDishka[ChannelDAO]) -> list[int]:
    with tracer.start_as_current_span("api.get_channel_ids"):
        logger.info("received_get_channel_ids_request", stage="start")

        result = await channel_dao.get_ids()

        logger.info("get_channel_ids_request_completed", stage="complete")
        return result


@router.get("/channel/statistics", status_code=status.HTTP_200_OK)
async def get_channel_statistics(
    channel_id: int,
    channel_statistics_dao: FromDishka[ChannelStatisticDAO],
    sorting: Literal["newest", "oldest"] = "newest",
    skip: int = 0,
    limit: int | None = None,
) -> list[ChannelStatistic]:
    with tracer.start_as_current_span("api.get_channel_statistics") as span:
        span.set_attribute("channel.id", str(channel_id))
        span.set_attribute("sorting", sorting)
        span.set_attribute("skip", skip)
        span.set_attribute("limit", limit or "none")
        span.add_event("getting_channel_statistics_from_database")

        request_logger = logger.bind(
            channel_id=channel_id, sorting=sorting, skip=skip, limit=limit or "none"
        )
        request_logger.info("received_get_channel_statistics_request", stage="start")

        statistics = await channel_statistics_dao.get_channel_statistics(
            channel_id, sorting, skip, limit
        )

        result = [
            ChannelStatistic.from_persistence(statistic) for statistic in statistics
        ]

        request_logger.info(
            "get_channel_statistics_request_completed",
            stage="complete",
            count=len(result),
        )
        span.set_attribute("count", len(result))
        return result


@router.get("/channel/messages", status_code=status.HTTP_200_OK)
async def get_channel_messages(
    channel_id: int,
    channel_message_dao: FromDishka[ChannelMessageDAO],
    sorting: Literal["newest", "oldest"] = "newest",
    skip: int = 0,
    limit: int | None = None,
) -> list[ChannelMessage]:
    with tracer.start_as_current_span("api.get_channel_messages") as span:
        request_logger = logger.bind(
            channel_id=channel_id, sorting=sorting, skip=skip, limit=limit or "none"
        )
        span.set_attribute("channel.id", str(channel_id))
        span.set_attribute("sorting", sorting)
        span.set_attribute("skip", skip)
        span.set_attribute("limit", limit or "none")

        request_logger.info("received_get_channel_messages_request", stage="start")
        messages = await channel_message_dao.get_channel_messages(
            channel_id, sorting, skip, limit
        )

        result = [ChannelMessage.from_persistence(message) for message in messages]

        request_logger.info(
            "get_channel_messages_request_completed",
            stage="complete",
            count=len(result),
        )
        span.set_attribute("count", len(result))
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
    message_id: UUID,
    channel_message_dao: FromDishka[ChannelMessageDAO],
) -> ChannelMessage:
    with tracer.start_as_current_span("api.get_message") as span:
        span.set_attribute("message.id", str(message_id))
        request_logger = logger.bind(message_id=message_id)
        request_logger.info("received_get_message_request", stage="start")

        message = await channel_message_dao.find_with_loaded_statistics_and_media(
            message_id
        )
        if message is None:
            request_logger.error("message_not_found", stage="error")
            span.set_status(status=Status(StatusCode.ERROR, "Message not found"))
            raise CustomHTTPException(
                error="ChannelMessageNotFound",
                status_code=status.HTTP_404_NOT_FOUND,
                message=f"Message with id {message_id} not found",
            )

        result = ChannelMessage.from_persistence(message)

        request_logger.info("get_message_request_completed", stage="complete")
        return result
