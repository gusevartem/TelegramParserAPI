from app.services.base import BaseService
from typing import Optional
from arq.jobs import Job
from shared_models.database.get_channel import GetChannelRequest, GetChannelResponse
from shared_models.database.get_channels_ids import GetChannelsIdsResponse
from shared_models.database.get_24h_statistics import (
    Get24hStatisticsRequest,
    Get24hStatisticsResponse,
)
from shared_models.database.get_messages import GetMessagesRequest, GetMessagesResponse
from shared_models.database.get_media import GetMediaRequest, GetMediaResponse
from app.config import RedisConfig


class Database(BaseService):
    def __init__(self):
        super().__init__(RedisConfig.DATABASE_QUEUE_NAME)

    async def get_channel(self, request: GetChannelRequest) -> GetChannelResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job(  # type: ignore
            "Database.get_channel", request
        )
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.DEFAULT_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )

    async def get_channels_ids(self) -> GetChannelsIdsResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job("Database.get_channels_ids")  # type: ignore
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.DEFAULT_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )

    async def get_24h_statistics(
        self, request: Get24hStatisticsRequest
    ) -> Get24hStatisticsResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job(  # type: ignore
            "Database.get_24h_statistics", request
        )
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.DEFAULT_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )

    async def get_channel_messages(
        self, request: GetMessagesRequest
    ) -> GetMessagesResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job(  # type: ignore
            "Database.get_messages", request
        )
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.DEFAULT_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )

    async def get_media(self, request: GetMediaRequest) -> GetMediaResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job(  # type: ignore
            "Database.get_media", request
        )
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.DEFAULT_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )
