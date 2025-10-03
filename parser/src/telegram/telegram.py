import logging
import os
from .custom_client import CustomClient, RedisConfig
from tortoise import Tortoise
from ..config import Config, TORTOISE_ORM, TelegramClientConfig
from .models import Client, TelegramCredentials
import zipfile
import io


class Telegram:
    def __init__(
        self, redis_host: str, redis_port: int, telegram_clients_redis_db: int
    ) -> None:
        self.logger = logging.getLogger("telegram")
        self.__redis_config = RedisConfig(
            host=redis_host, port=redis_port, db=telegram_clients_redis_db
        )

    async def init_database(self) -> None:
        self.logger.info("Initializing database")
        await Tortoise.init(config=TORTOISE_ORM)
        await Tortoise.generate_schemas()
        self.logger.info("Database initialized")

    async def close(self) -> None:
        await Tortoise.close_connections()

    async def get_client(self) -> CustomClient:
        client = await Client.filter(working=True).order_by("users_count", "id").first()
        if not client:
            raise ValueError("No working clients found")

        return CustomClient(client, self.__redis_config)

    # Methods
    @staticmethod
    async def add_client(ctx, tdata: bytes) -> None:
        self: Telegram = ctx["Telegram_instance"]
        self.logger.info("Adding client")

        telegram_credentials, _ = await TelegramCredentials.get_or_create(
            api_id=TelegramClientConfig.API_ID,
            api_hash=TelegramClientConfig.API_HASH,
            device_model=TelegramClientConfig.DEVICE_MODEL,
            system_version=TelegramClientConfig.SYSTEM_VERSION,
            app_version=TelegramClientConfig.APP_VERSION,
            lang_code=TelegramClientConfig.LANG_CODE,
            system_lang_code=TelegramClientConfig.SYSTEM_LANG_CODE,
            lang_pack=TelegramClientConfig.LANG_PACK,
        )

        new_client = await Client.create(
            telegram_credentials=telegram_credentials, working=False
        )
        await new_client.save()

        target_directory = os.path.join(Config.TDATA_PATH, str(new_client.id))
        os.makedirs(target_directory, exist_ok=True)
        with io.BytesIO(tdata) as zip_buffer:
            with zipfile.ZipFile(zip_buffer) as z:
                z.extractall(target_directory)
        if not os.path.exists(os.path.join(target_directory, "tdata")):
            raise zipfile.BadZipFile("tdata directory not found")

        client = CustomClient(new_client, self.__redis_config)
        async with client:
            self.logger.info("Client activated")
            new_client.working = True
            await new_client.save()
