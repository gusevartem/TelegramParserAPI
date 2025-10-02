from arq import ArqRedis, create_pool
from typing import Optional
from app.config import RedisConfig


class BaseService:
    def __init__(self, queue_name: str) -> None:
        self.redis: Optional[ArqRedis] = None
        self.queue_name = queue_name

    async def init(self):
        if not self.redis:
            self.redis = await create_pool(
                RedisConfig.REDIS_SETTINGS, default_queue_name=self.queue_name
            )
