import tempfile
import time
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from logging import Logger, getLogger
from typing import Protocol, override

from dishka import Provider, Scope, provide
from dishka.dependency_source import CompositeDependencySource
from parser.dto import ProxySettings, TelegramCredentials
from parser.persistence import TelegramClientDAO
from telethon.sessions.sqlite import SQLiteSession
from telethon.sessions.string import StringSession

from .client import ITelegramClient, TelegramClient
from .exceptions import (
    InvalidClient,
    NoWorkingClientsFoundError,
)
from .session_storage import ITelegramSessionStorage, RabbitMQSessionStorage
from .settings import TelegramSettings


class ITelegram(Protocol):
    async def add_client(
        self,
        session_data: bytes,
        credentials: TelegramCredentials | None,
        proxy: ProxySettings | None,
    ) -> None: ...

    def get_client(
        self, timeout: int = 5
    ) -> AbstractAsyncContextManager[ITelegramClient]: ...


class Telegram(ITelegram):
    def __init__(
        self,
        telegram_client_dao: TelegramClientDAO,
        session_storage: ITelegramSessionStorage,
        settings: TelegramSettings,
    ) -> None:
        self.telegram_client_dao: TelegramClientDAO = telegram_client_dao
        self.session_storage: ITelegramSessionStorage = session_storage
        self.settings: TelegramSettings = settings
        self.logger: Logger = getLogger(__name__)

    @override
    async def add_client(
        self,
        session_data: bytes,
        credentials: TelegramCredentials | None = None,
        proxy: ProxySettings | None = None,
    ) -> None:
        self.logger.info("⌛ Adding client")

        start_time = time.perf_counter()
        with tempfile.NamedTemporaryFile(suffix=".session") as session_temp_file:
            self.logger.info("⌛ Creating temporary session file")
            session_temp_file.write(session_data)

            credentials = credentials or self.settings.default_credentials

            session = SQLiteSession(session_temp_file.name)
            client = TelegramClient(
                session=session,
                credentials=credentials,
                proxy=proxy,
            )
            async with client:
                me = await client.get_me()
                if me.phone is None:
                    raise InvalidClient("Client does not have phone number")

                self.logger.info("✅ Client working")
                existing_client = await self.telegram_client_dao.find_by_id(me.id)
                if existing_client is not None:
                    raise InvalidClient("Client already exists")

                self.logger.info("⌛ Adding client to database")
                await self.telegram_client_dao.create(
                    telegram_id=me.id,
                    phone=me.phone,
                    api_id=credentials.api_id,
                    api_hash=credentials.api_hash,
                    device_model=credentials.device_model,
                    system_version=credentials.system_version,
                    app_version=credentials.app_version,
                    lang_code=credentials.lang_code,
                    system_lang_code=credentials.system_lang_code,
                    proxy=ProxySettings.to_persistence(proxy)
                    if proxy is not None
                    else None,
                )

                self.logger.info("⌛ Adding client to session storage")
                string_session = StringSession()
                string_session.set_dc(
                    session.dc_id, session.server_address, session.port
                )
                string_session.auth_key = session.auth_key

            await self.session_storage.add_session(me.id, string_session.save())

            await self.telegram_client_dao.commit()

        duration = (time.perf_counter() - start_time) * 1000
        self.logger.info(f"✅ Client added successfully. Duration: {duration:.0f}ms")

    @override
    @asynccontextmanager
    async def get_client(self, timeout: int = 5) -> AsyncIterator[ITelegramClient]:
        if not await self.telegram_client_dao.is_working_clients_exists():
            raise NoWorkingClientsFoundError("No working clients found in database")

        try:
            async with self.session_storage.get_session(timeout) as session_container:
                client_info = await self.telegram_client_dao.find_with_proxy(
                    session_container.user_id
                )
                if client_info is None:
                    self.logger.info(
                        f"❌ Client with id: {session_container.user_id} "
                        + "not found in database. Deleting from session storage"
                    )
                    raise InvalidClient(
                        "Client from session storage not found in database"
                    )
                if client_info.banned:
                    self.logger.info(
                        f"❌ Client with id: {session_container.user_id} "
                        + "is banned. Deleting from session storage"
                    )
                    raise InvalidClient("Client from session storage is banned")

                client = TelegramClient(
                    session=session_container.session,
                    credentials=TelegramCredentials(
                        api_id=client_info.api_id,
                        api_hash=client_info.api_hash,
                        device_model=client_info.device_model,
                        system_version=client_info.system_version,
                        app_version=client_info.app_version,
                        lang_code=client_info.lang_code,
                        system_lang_code=client_info.system_lang_code,
                    ),
                    proxy=ProxySettings.from_persistence(client_info.proxy)
                    if client_info.proxy is not None
                    else None,
                )

                async with client:
                    self.logger.info(
                        f"🚀 Starting usage of client: {session_container.user_id}"
                    )
                    start_time = time.perf_counter()

                    yield client

                    duration = (time.perf_counter() - start_time) * 1000
                    self.logger.info(
                        f"🏁 Finished usage of client: {session_container.user_id}. "
                        + f"Duration: {duration:.0f}ms"
                    )

                session_container.session.set_dc(
                    client.current_session.dc_id,
                    client.current_session.server_address,
                    client.current_session.port,
                )
                session_container.session.auth_key = client.current_session.auth_key

        except InvalidClient as e:
            if e.user_id is not None:
                telegram_client = await self.telegram_client_dao.find_by_id(e.user_id)
                if telegram_client is not None:
                    self.logger.info(f"⌛ Banning client: {e.user_id}")

                    telegram_client.banned = True
                    await self.telegram_client_dao.save(telegram_client)
                    await self.telegram_client_dao.commit()

                    self.logger.info(f"✅ Client banned: {e.user_id}")
            raise


class TelegramProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> TelegramSettings:
        return TelegramSettings()  # type: ignore # pyright: ignore

    session_storage: CompositeDependencySource = provide(
        RabbitMQSessionStorage,
        scope=Scope.REQUEST,
        provides=ITelegramSessionStorage,
    )

    telegram: CompositeDependencySource = provide(
        Telegram, scope=Scope.REQUEST, provides=ITelegram
    )
