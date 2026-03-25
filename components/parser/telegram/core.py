import tempfile
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Protocol, override
from uuid import uuid4

import structlog
from dishka import Provider, Scope, provide
from dishka.dependency_source import CompositeDependencySource
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from parser.dto import ProxySettings, TelegramCredentials
from parser.persistence import TelegramClientDAOFactory
from telethon import TelegramClient as TelethonTelegramClient
from telethon import errors
from telethon.sessions.sqlite import SQLiteSession
from telethon.sessions.string import StringSession

from .client import ITelegramClient, ITelegramClientFactory, TelegramClient
from .exceptions import (
    InvalidClient,
    NoWorkingClientsFoundError,
)
from .session_storage import (
    ITelegramSessionStorage,
    PostgreSQLSessionStorage,
)
from .settings import TelegramSettings


@dataclass
class PendingAuth:
    phone: str
    phone_code_hash: str
    session_string: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def is_expired(self, ttl_minutes: int = 10) -> bool:
        return datetime.now(timezone.utc) > self.created_at + timedelta(minutes=ttl_minutes)


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

    async def request_code(self, phone: str) -> str: ...

    async def confirm_code(
        self,
        request_id: str,
        code: str,
        twofa_password: str | None = None,
    ) -> None: ...


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
        self._pending_auths: dict[str, PendingAuth] = {}

    def _build_bare_telethon_client(self, session: StringSession) -> TelethonTelegramClient:
        """Build a raw Telethon client (not yet connected) using default credentials."""
        creds = self.settings.default_credentials
        return TelethonTelegramClient(
            session=session,
            api_id=creds.api_id,
            api_hash=creds.api_hash,
            device_model=creds.device_model,
            system_version=creds.system_version,
            app_version=creds.app_version,
            lang_code=creds.lang_code,
            system_lang_code=creds.system_lang_code,
            timeout=30,
            flood_sleep_threshold=0,
        )

    @override
    async def request_code(self, phone: str) -> str:
        with self.tracer.start_as_current_span("telegram.request_code") as span:
            span.set_attribute("telegram.phone", phone)
            logger = self.logger.bind(phone=phone)
            logger.info("requesting_code", stage="start")

            session = StringSession()
            client = self._build_bare_telethon_client(session)
            await client.connect()
            try:
                sent = await client.send_code_request(phone)
            finally:
                await client.disconnect()

            request_id = str(uuid4())
            self._pending_auths[request_id] = PendingAuth(
                phone=phone,
                phone_code_hash=sent.phone_code_hash,
                session_string=session.save(),
            )
            logger.info("code_sent", request_id=request_id, stage="complete")
            span.set_attribute("telegram.request_id", request_id)
            return request_id

    @override
    async def confirm_code(
        self,
        request_id: str,
        code: str,
        twofa_password: str | None = None,
    ) -> None:
        with self.tracer.start_as_current_span("telegram.confirm_code") as span:
            span.set_attribute("telegram.request_id", request_id)
            logger = self.logger.bind(request_id=request_id)
            logger.info("confirming_code", stage="start")

            pending = self._pending_auths.get(request_id)
            if pending is None:
                raise InvalidClient("Auth request not found")
            if pending.is_expired():
                del self._pending_auths[request_id]
                raise InvalidClient("Auth request expired, please request a new code")

            session = StringSession(pending.session_string)
            client = self._build_bare_telethon_client(session)
            try:
                await client.connect()
                try:
                    await client.sign_in(
                        pending.phone,
                        code=code,
                        phone_code_hash=pending.phone_code_hash,
                    )
                except errors.SessionPasswordNeededError:
                    if not twofa_password:
                        raise InvalidClient("2FA password required")
                    await client.sign_in(password=twofa_password)

                me = await client.get_me()
                if me is None or me.phone is None:
                    raise InvalidClient("Client does not have phone number")

                span.set_attribute("telegram.user_id", me.id)
                span.set_attribute("telegram.phone", me.phone)

                async with self.telegram_client_dao_factory() as telegram_client_dao:
                    existing = await telegram_client_dao.find_by_id(me.id)
                    if existing is not None:
                        raise InvalidClient("Client already exists")

                    creds = self.settings.default_credentials
                    await telegram_client_dao.create(
                        telegram_id=me.id,
                        phone=me.phone,
                        api_id=creds.api_id,
                        api_hash=creds.api_hash,
                        device_model=creds.device_model,
                        system_version=creds.system_version,
                        app_version=creds.app_version,
                        lang_code=creds.lang_code,
                        system_lang_code=creds.system_lang_code,
                        proxy=None,
                    )
                    await telegram_client_dao.commit()

                    await self.session_storage.add_session(me.id, session.save())

                del self._pending_auths[request_id]
                logger.info("code_confirmed", user_id=me.id, phone=me.phone, stage="complete")

            except Exception:
                await client.disconnect()
                raise
            await client.disconnect()

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

            logger.info(
                "client_added_successfully",
                user_id=me.id,
                phone=me.phone,
                stage="complete",
            )

    @override
    @asynccontextmanager
    async def get_client(self, timeout: int = 5) -> AsyncIterator[ITelegramClient]:
        logger = self.logger.bind(timeout=timeout)

        try:
            working_clients_exists = True
            with self.tracer.start_as_current_span("telegram.check_clients") as span:
                span.set_attribute("worker.id", self.settings.worker_id)
                async with self.telegram_client_dao_factory() as telegram_client_dao:
                    if not await telegram_client_dao.is_working_clients_exists():
                        working_clients_exists = False
                        span.set_attribute("working_clients_exists", False)
                    else:
                        span.set_attribute("working_clients_exists", True)

            if not working_clients_exists:
                raise NoWorkingClientsFoundError("No working clients found in database")

            async with self.session_storage.get_session(timeout) as session_container:
                with self.tracer.start_as_current_span("telegram.check_client") as span:
                    span.set_attribute("telegram.session_timeout_seconds", timeout)
                    span.set_attribute("worker.id", self.settings.worker_id)
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

                async with client:
                    yield client

                # Успех
                with self.tracer.start_as_current_span(
                    "telegram.client_usage_completed"
                ) as span:
                    span.set_attribute("telegram.user_id", session_container.user_id)
                    span.set_attribute("telegram.phone", client_info.phone)
                    span.set_attribute("worker.id", self.settings.worker_id)
                    self.logger.info(
                        "client_usage_completed",
                        user_id=session_container.user_id,
                        phone=client_info.phone,
                        stage="complete",
                    )
                    session_container.session.set_dc(
                        client.current_session.dc_id,
                        client.current_session.server_address,
                        client.current_session.port,
                    )
                    session_container.session.auth_key = client.current_session.auth_key
        except NoWorkingClientsFoundError:
            raise
        except InvalidClient as e:
            with self.tracer.start_as_current_span(
                "telegram.handle_invalid_client_error"
            ) as err_span:
                if e.user_id is None:
                    err_span.set_attribute("telegram.user_id", "unknown")
                    self.logger.warning("invalid_client_error", exc_info=True)
                    raise
                err_span.set_attribute("telegram.user_id", e.user_id)
                err_span.set_attribute("worker.id", self.settings.worker_id)

                async with self.telegram_client_dao_factory() as telegram_client_dao:
                    telegram_client = await telegram_client_dao.find_by_id(e.user_id)
                    if telegram_client is not None:
                        self.logger.info("banning_client", user_id=e.user_id)
                        telegram_client.banned = True
                        await telegram_client_dao.save(telegram_client)
                        await telegram_client_dao.commit()
                        self.logger.info("client_banned", user_id=e.user_id)
                err_span.set_status(Status(StatusCode.ERROR, str(e)))
                err_span.record_exception(e)
                raise
        except Exception as e:
            with self.tracer.start_as_current_span(
                "telegram.handle_invalid_client_error"
            ) as err_span:
                err_span.set_status(Status(StatusCode.ERROR, str(e)))
                self.logger.error("invalid_client_error", exc_info=True)
                err_span.record_exception(e)
            raise


class TelegramProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> TelegramSettings:
        return TelegramSettings()  # type: ignore # pyright: ignore

    # @provide(scope=Scope.APP)
    # async def channel(
    #     self, connection: aio_pika.abc.AbstractConnection
    # ) -> AsyncIterator[SessionStorageChannel]:
    #     channel = await connection.channel(
    #         publisher_confirms=True, on_return_raises=True
    #     )
    #     await channel.set_qos(prefetch_count=1)

    #     yield SessionStorageChannel(channel)

    #     await channel.close()

    @provide(scope=Scope.APP)
    def telegram_client_factory(self) -> ITelegramClientFactory:
        return TelegramClient

    session_storage: CompositeDependencySource = provide(
        PostgreSQLSessionStorage,
        scope=Scope.APP,
        provides=ITelegramSessionStorage,
    )

    telegram: CompositeDependencySource = provide(
        Telegram, scope=Scope.APP, provides=ITelegram
    )
