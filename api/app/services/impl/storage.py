from app.services.base import BaseService
from typing import Optional
from arq.jobs import Job
from shared_models.storage.get_logo import GetLogoRequest, GetLogoResponse
from shared_models.storage.get_media import GetMediaRequest, GetMediaResponse
from app.config import RedisConfig


class Storage(BaseService):
    def __init__(self):
        super().__init__(RedisConfig.STORAGE_QUEUE_NAME)

    async def get_logo(self, request: GetLogoRequest) -> GetLogoResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job("Storage.get_logo", request)  # type: ignore
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.DEFAULT_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )

    async def get_media(self, request: GetMediaRequest) -> GetMediaResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job("Storage.get_media", request)  # type: ignore
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.DEFAULT_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )
