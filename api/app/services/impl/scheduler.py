from app.services.base import BaseService
from typing import Optional
from arq.jobs import Job
from shared_models.scheduler.add_channel import AddChannelRequest, AddChannelResponse
from app.config import RedisConfig


class Scheduler(BaseService):
    def __init__(self):
        super().__init__(RedisConfig.SCHEDULER_QUEUE_NAME)

    async def add_channel(self, request: AddChannelRequest) -> AddChannelResponse:
        await self.init()
        task: Optional[Job] = await self.redis.enqueue_job(  # type: ignore
            "Scheduler.add_channel", request
        )
        if task is None:
            raise ValueError("Task was not created")
        return await task.result(
            RedisConfig.PARSER_TIMEOUT, poll_delay=RedisConfig.DEFAULT_POLL_DELAY
        )
