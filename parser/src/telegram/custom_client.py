import os
from dataclasses import dataclass
from typing import Optional
from telethon import TelegramClient
from telethon.sessions import StringSession
from opentele.td.tdesktop import TDesktop
from opentele.api import API
from .models import Client, TelegramCredentials
from redis.asyncio import Redis
from telethon.errors import SessionPasswordNeededError
from shared_models.parser.errors import SessionPasswordNeeded
from ..config import Config
from tortoise.expressions import F


@dataclass
class RedisConfig:
    host: str
    port: int
    db: int


class CustomClient:
    def __init__(self, client: Client, redis_config: RedisConfig) -> None:
        self._redis = Redis(
            host=redis_config.host, port=redis_config.port, db=redis_config.db
        )
        self._client = client
        self._t_client: Optional[TelegramClient] = None
        self._session: Optional[StringSession] = None

    async def mark_as_ban(self) -> None:
        self._client.working = False
        await self._client.save()

    async def _get_client_from_tdata(self) -> TelegramClient:
        tdata_path = os.path.join(Config.TDATA_PATH, str(self._client.id), "tdata")
        credentials: TelegramCredentials = await self._client.telegram_credentials
        api = API.TelegramDesktop(
            api_id=credentials.api_id,
            api_hash=credentials.api_hash,
            device_model=credentials.device_model,
            system_version=credentials.system_version,
            app_version=credentials.app_version,
            lang_code=credentials.lang_code,
            system_lang_code=credentials.system_lang_code,
            lang_pack=credentials.lang_pack,
        )
        tdesk = TDesktop(tdata_path, api)

        pass_path = os.path.join(Config.TDATA_PATH, str(self._client.id), "2FA.txt")
        password = None
        if os.path.exists(pass_path):
            with open(pass_path, "r") as f:
                password = f.read().strip()

        return await tdesk.ToTelethon(
            session=self._session,  # type: ignore
            api=api,
            password=password,  # type: ignore
            auto_reconnect=False,
        )

    async def __aenter__(self) -> TelegramClient:
        session_str = await self._redis.get(str(self._client.id))

        if session_str:
            self._session = StringSession(session_str.decode("utf-8"))
            credentials: TelegramCredentials = await self._client.telegram_credentials
            t_client = TelegramClient(
                auto_reconnect=False,
                session=self._session,
                api_id=credentials.api_id,
                api_hash=credentials.api_hash,
                device_model=credentials.device_model,
                system_version=credentials.system_version,
                app_version=credentials.app_version,
                lang_code=credentials.lang_code,
                system_lang_code=credentials.system_lang_code,
            )
            try:
                await t_client.start()  # type: ignore
                await Client.filter(id=self._client.id).update(
                    users_count=F("users_count") + 1
                )
                self._t_client = t_client
                return t_client
            except Exception as e:
                await self.mark_as_ban()
                raise ValueError(f"Cannot start client: {str(e)}")
        else:
            self._session = StringSession()
            try:
                t_client = await self._get_client_from_tdata()
                await t_client.start()  # type: ignore
                await Client.filter(id=self._client.id).update(
                    users_count=F("users_count") + 1
                )
                self._t_client = t_client
                return t_client
            except SessionPasswordNeededError:
                await self.mark_as_ban()
                raise SessionPasswordNeeded()
            except BaseException as e:
                await self.mark_as_ban()
                raise ValueError(f"Account not working: {str(e)}")

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session:
            await self._redis.set(
                str(self._client.id), self._session.save().encode("utf-8")
            )
        if self._t_client:
            await Client.filter(id=self._client.id).update(
                users_count=F("users_count") - 1
            )
