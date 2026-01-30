import tempfile
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol, override

import aio_pika
import structlog
from dishka import Provider, Scope, provide
from dishka.dependency_source import CompositeDependencySource
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from parser.dto import ProxySettings, TelegramCredentials
from parser.persistence import TelegramClientDAOFactory
from telethon.sessions.sqlite import SQLiteSession
from telethon.sessions.string import StringSession

from .client import ITelegramClient, ITelegramClientFactory, TelegramClient
from .exceptions import (
    InvalidClient,
    NoWorkingClientsFoundError,
)
from .session_storage import (
    ITelegramSessionStorage,
    RabbitMQSessionStorage,
    SessionStorageChannel,
)
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
        telegram_client_dao_factory: TelegramClientDAOFactory,
        session_storage: ITelegramSessionStorage,
        telegram_client_factory: ITelegramClientFactory,
        settings: TelegramSettings,
    ) -> None:
        self.telegram_client_dao_factory: TelegramClientDAOFactory = (
            telegram_client_dao_factory
        )
        self.session_storage: ITelegramSessionStorage = session_storage
        self.telegram_client_factory: ITelegramClientFactory = telegram_client_factory
        self.settings: TelegramSettings = settings

        self.logger: structlog.BoundLogger = structlog.get_logger("telegram_pool")
        self.tracer: trace.Tracer = trace.get_tracer("telegram_pool")

    @override
    async def add_client(
        self,
        session_data: bytes,
        credentials: TelegramCredentials | None = None,
        proxy: ProxySettings | None = None,
    ) -> None:
        with self.tracer.start_as_current_span("telegram.add_client") as span:
            span.set_attribute("telegram.proxy_provided", proxy is not None)
            logger = self.logger.bind(proxy_provided=proxy is not None)

            logger.info("adding_client", stage="start")
            span.add_event("add_client_started")

            with tempfile.NamedTemporaryFile(suffix=".session") as session_temp_file:
                logger.info("creating_temporary_session_file")
                session_temp_file.write(session_data)
                session_temp_file.flush()

                credentials = credentials or self.settings.default_credentials

                session = SQLiteSession(session_temp_file.name)
                client = self.telegram_client_factory(
                    session=session,
                    credentials=credentials,
                    proxy=proxy,
                    settings=self.settings,
                )
                async with client:
                    me = await client.get_me()
                    if me.phone is None:
                        logger.error("client_no_phone_number")
                        span.set_status(
                            Status(StatusCode.ERROR, "Client has no phone number")
                        )
                        raise InvalidClient("Client does not have phone number")

                    logger.info(
                        "client_validation_success", user_id=me.id, phone=me.phone
                    )
                    span.set_attribute("telegram.user_id", me.id)
                    span.set_attribute("telegram.phone", me.phone)

                    async with (
                        self.telegram_client_dao_factory() as telegram_client_dao
                    ):
                        existing_client = await telegram_client_dao.find_by_id(me.id)
                        if existing_client is not None:
                            logger.error("client_already_exists", user_id=me.id)
                            span.set_status(
                                Status(StatusCode.ERROR, "Client already exists")
                            )
                            raise InvalidClient("Client already exists")

                        logger.info("saving_client_to_database", user_id=me.id)
                        await telegram_client_dao.create(
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
                        await telegram_client_dao.commit()

                        logger.info("saving_session_to_storage", user_id=me.id)
                        string_session = StringSession()
                        string_session.set_dc(
                            session.dc_id, session.server_address, session.port
                        )
                        string_session.auth_key = session.auth_key

                        await self.session_storage.add_session(
                            me.id, string_session.save()
                        )

            logger.info("client_added_successfully", user_id=me.id, phone=me.phone)
            span.add_event("add_client_completed")

    @override
    @asynccontextmanager
    async def get_client(self, timeout: int = 5) -> AsyncIterator[ITelegramClient]:
        with self.tracer.start_as_current_span("telegram.get_client") as span:
            span.set_attribute("telegram.session_timeout_seconds", timeout)
            logger = self.logger.bind(timeout=timeout)

            async with self.telegram_client_dao_factory() as telegram_client_dao:
                if not await telegram_client_dao.is_working_clients_exists():
                    logger.error("no_working_clients_found")
                    span.set_status(Status(StatusCode.ERROR, "No working clients"))
                    raise NoWorkingClientsFoundError(
                        "No working clients found in database"
                    )

            try:
                async with self.session_storage.get_session(
                    timeout
                ) as session_container:
                    async with (
                        self.telegram_client_dao_factory() as telegram_client_dao
                    ):
                        client_info = await telegram_client_dao.find_with_proxy(
                            session_container.user_id
                        )
                        if client_info is None:
                            logger.warning(
                                "client_not_found_in_db",
                                user_id=session_container.user_id,
                                next_action="delete_from_session_storage",
                            )
                            span.set_status(
                                Status(StatusCode.ERROR, "Client not in DB")
                            )
                            raise InvalidClient(
                                "Client from session storage not found in database"
                            )
                        if client_info.banned:
                            logger.warning(
                                "client_banned",
                                user_id=session_container.user_id,
                                next_action="delete_from_session_storage",
                            )
                            span.set_status(Status(StatusCode.ERROR, "Client banned"))
                            raise InvalidClient("Client from session storage is banned")

                        client = self.telegram_client_factory(
                            settings=self.settings,
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
                        logger.info(
                            "client_acquired",
                            user_id=session_container.user_id,
                            phone=client_info.phone,
                            stage="start",
                        )
                        span.set_attribute(
                            "telegram.user_id", session_container.user_id
                        )
                        span.set_attribute(
                            "telegram.phone", client_info.phone or "none"
                        )
                        span.add_event("client_acquired")

                        yield client

                        logger.info(
                            "client_usage_completed",
                            user_id=session_container.user_id,
                            phone=client_info.phone,
                        )
                        span.add_event("client_usage_completed")

                    session_container.session.set_dc(
                        client.current_session.dc_id,
                        client.current_session.server_address,
                        client.current_session.port,
                    )
                    session_container.session.auth_key = client.current_session.auth_key

            except InvalidClient as e:
                if e.user_id is not None:
                    async with (
                        self.telegram_client_dao_factory() as telegram_client_dao
                    ):
                        telegram_client = await telegram_client_dao.find_by_id(
                            e.user_id
                        )
                        if telegram_client is not None:
                            logger.info("banning_client", user_id=e.user_id)

                            telegram_client.banned = True
                            await telegram_client_dao.save(telegram_client)
                            await telegram_client_dao.commit()

                            logger.info("client_banned", user_id=e.user_id)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise


class TelegramProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> TelegramSettings:
        return TelegramSettings()  # type: ignore # pyright: ignore

    @provide(scope=Scope.APP)
    async def channel(
        self, connection: aio_pika.abc.AbstractConnection
    ) -> AsyncIterator[SessionStorageChannel]:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        await channel.set_qos(prefetch_count=1)

        yield SessionStorageChannel(channel)

        await channel.close()

    @provide(scope=Scope.APP)
    def telegram_client_factory(self) -> ITelegramClientFactory:
        return TelegramClient

    session_storage: CompositeDependencySource = provide(
        RabbitMQSessionStorage,
        scope=Scope.APP,
        provides=ITelegramSessionStorage,
    )

    telegram: CompositeDependencySource = provide(
        Telegram, scope=Scope.APP, provides=ITelegram
    )
