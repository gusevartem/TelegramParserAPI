import asyncio
import logging
from types import CoroutineType
from .allocator.allocator import Allocator
from shared_models.database.get_channel import GetChannelRequest, GetChannelResponse
from arq.connections import RedisSettings
from shared_models.parser.get_channel_info import (
    GetChannelInfoRequest,
    GetChannelInfoResponse,
)
from shared_models.scheduler.add_channel import AddChannelRequest, AddChannelResponse
from shared_models.database.get_channels_ids import GetChannelsIdsResponse
from shared_models.storage.save_logo import SaveLogoRequest
from shared_models.database.get_channel_by_link import (
    GetChannelByLinkRequest,
    GetChannelByLinkResponse,
)
from shared_models.database.errors import ChannelDoesNotExistError
from typing import Any, Optional
from arq import create_pool
from arq.jobs import Job


class Scheduler:
    def __init__(
        self,
        slots_count: int,
        allocation_interval_minutes: int,
        parser_redis: RedisSettings,
        parser_queue_name: str,
        database_redis: RedisSettings,
        database_queue_name: str,
        storage_redis: RedisSettings,
        storage_queue_name: str,
    ) -> None:
        self.logger = logging.getLogger("scheduler")
        self.allocator: Optional[Allocator] = None
        self.slots_count = slots_count
        self.allocation_interval_minutes = allocation_interval_minutes

        self.parser_redis_settings = parser_redis
        self.parser_redis = None
        self.parser_queue_name = parser_queue_name

        self.database_redis_settings = database_redis
        self.database_redis = None
        self.database_queue_name = database_queue_name

        self.storage_redis_settings = storage_redis
        self.storage_redis = None
        self.storage_queue_name = storage_queue_name

    async def get_channel_from_db(self, channel_id: int) -> GetChannelResponse:
        task: Optional[Job] = await self.database_redis.enqueue_job(  # type: ignore
            "Database.get_channel", GetChannelRequest(channel_id=channel_id)
        )
        if task is None:
            raise ValueError("Task was not created")
        return await task.result()

    async def init_allocator(self):
        task: Optional[Job] = await self.database_redis.enqueue_job(  # type: ignore
            "Database.get_channels_ids"
        )
        if task is None:
            raise ValueError("Task was not created")
        channels_ids: GetChannelsIdsResponse = await task.result()

        jobs = [
            self.get_channel_from_db(channel_id)
            for channel_id in channels_ids.channel_ids
        ]
        results = await asyncio.gather(*jobs, return_exceptions=True)
        channels: list[GetChannelResponse] = []
        for result in results:
            if isinstance(result, Exception):
                raise ValueError(f"Failed to get channel info: {result}")
            else:
                channels.append(result)  # type: ignore

        channels.sort(key=lambda x: x.last_update)
        self.allocator = Allocator(
            self.slots_count,
            self.allocation_interval_minutes,
            [channel.channel.channel_id for channel in channels],
        )

    async def init(self):
        if not self.parser_redis:
            self.parser_redis = await create_pool(
                self.parser_redis_settings, default_queue_name=self.parser_queue_name
            )
        if not self.database_redis:
            self.database_redis = await create_pool(
                self.database_redis_settings,
                default_queue_name=self.database_queue_name,
            )
        if not self.storage_redis:
            self.storage_redis = await create_pool(
                self.storage_redis_settings, default_queue_name=self.storage_queue_name
            )
        if not self.allocator:
            await self.init_allocator()

    async def get_channel(self, channel_link: str) -> GetChannelInfoResponse:
        request = GetChannelInfoRequest(channel_link=channel_link, get_logo=True)
        response = await self.parser_redis.enqueue_job(  # type: ignore
            "Parser.get_channel_info", request
        )
        if not response:
            raise ValueError(f"Failed to get response for channel {channel_link}")
        return await response.result()

    async def update_logo(self, channel_id: int, logo: bytes):
        request = SaveLogoRequest(channel_id=channel_id, logo=logo)
        await self.storage_redis.enqueue_job("Storage.save", request)  # type: ignore

    # Cron
    @staticmethod
    async def run_iteration(ctx):
        self: Scheduler = ctx["Scheduler_instance"]
        await self.init()

        self.logger.info("Running iteration")
        try:
            update_channels = self.allocator.get_next_channels()  # type: ignore
        except ValueError as e:
            self.logger.error(str(e))
            self.logger.info("Initialization of allocator")
            await self.init_allocator()
            return
        if not update_channels:
            self.logger.info("No channels to update")
            return

        jobs: list[CoroutineType[Any, Any, GetChannelInfoResponse]] = []

        for channel_id in update_channels:
            channel = await self.get_channel_from_db(channel_id)
            jobs.append(self.get_channel(channel_link=channel.channel.link))

        results = await asyncio.gather(*jobs, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Failed to get channel info: {result}")
                continue
            else:
                await self.database_redis.enqueue_job(  # type: ignore
                    "Database.update_or_create_channel",
                    result.channel,  # type: ignore
                )
                await self.update_logo(result.channel.channel_id, result.logo)  # type: ignore

    # Methods
    @staticmethod
    async def add_channel(ctx, request: AddChannelRequest) -> AddChannelResponse:
        self: Scheduler = ctx["Scheduler_instance"]
        await self.init()

        if not isinstance(request, AddChannelRequest):
            raise ValueError(f"Invalid request: {request}")

        try:
            get_channel_request = GetChannelByLinkRequest(
                channel_link=request.channel_link
            )
            r = await self.database_redis.enqueue_job(  # type: ignore
                "Database.get_channel_by_link", get_channel_request
            )
            channel_by_link: GetChannelByLinkResponse = await r.result()  # type: ignore
        except ChannelDoesNotExistError:
            channel = await self.get_channel(channel_link=request.channel_link)
            await self.database_redis.enqueue_job(  # type: ignore
                "Database.update_or_create_channel", channel.channel
            )
            if channel.logo is not None:
                await self.update_logo(channel.channel.channel_id, channel.logo)
            return AddChannelResponse(channel=channel.channel, success=True)
        else:
            return AddChannelResponse(channel=channel_by_link.channel, success=False)
