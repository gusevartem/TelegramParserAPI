import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, ClassVar, Protocol, override

import structlog
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from parser.persistence import (
    MultipleDAOFactory,
    TelegramClientDAO,
    WorkerAccountUsageDAO,
)
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    PlainSerializer,
)
from telethon.sessions.string import StringSession

from .exceptions import ClientBanned, FloodWait, InvalidClient
from .settings import TelegramSettings


def serialize_string_session(session: StringSession | str) -> str:
    if isinstance(session, StringSession):
        return session.save()
    return session


def validate_string_session(value: Any) -> StringSession:
    if isinstance(value, StringSession):
        return value
    if isinstance(value, str):
        return StringSession(value)
    raise ValueError(f"Invalid session type: {type(value).__name__}")


class TelegramSession(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    user_id: int
    session: Annotated[
        StringSession,
        PlainSerializer(serialize_string_session, return_type=str, when_used="json"),
        BeforeValidator(validate_string_session),
    ]


class ITelegramSessionStorage(Protocol):
    async def add_session(self, user_id: int, session: str) -> None: ...
    def get_session(
        self, timeout: int = 5
    ) -> AbstractAsyncContextManager[TelegramSession]: ...


class PostgreSQLSessionStorage(ITelegramSessionStorage):
    def __init__(
        self,
        multiple_dao_factory: MultipleDAOFactory,
        settings: TelegramSettings,
    ):
        self.multiple_dao_factory: MultipleDAOFactory = multiple_dao_factory
        self.settings: TelegramSettings = settings
        self.logger: structlog.BoundLogger = structlog.get_logger(
            "session_storage_postgres"
        )
        self.tracer: trace.Tracer = trace.get_tracer("session_storage_postgres")

    @override
    async def add_session(self, user_id: int, session: str) -> None:
        with self.tracer.start_as_current_span(
            "postgres_session_storage.add_session"
        ) as span:
            span.set_attribute("session.user_id", user_id)
            span.set_attribute("worker.id", self.settings.worker_id)

            async with self.multiple_dao_factory() as dao_factory:
                client_dao = dao_factory(TelegramClientDAO)

                client = await client_dao.find_by_id(user_id)
                if client is None:
                    self.logger.warning(
                        "client_not_found_on_add_session", user_id=user_id
                    )
                    return

                client.session_string = session
                await client_dao.save(client)
                await dao_factory.commit()
                self.logger.info("postgres_session_updated", user_id=user_id)

    @override
    @asynccontextmanager
    async def get_session(self, timeout: int = 5) -> AsyncIterator[TelegramSession]:
        with self.tracer.start_as_current_span(
            "postgres_session_storage.acquire_session"
        ) as acquire_span:
            acquire_span.set_attribute("messaging.system", "postgresql")
            acquire_span.set_attribute("worker.id", self.settings.worker_id)
            logger = self.logger.bind(timeout=timeout)
            logger.info("waiting_for_postgres_session", stage="start")

            start_time = time.monotonic()
            client_id: int | None = None
            session_str: str | None = None

            async with self.multiple_dao_factory() as dao_factory:
                client_dao = dao_factory(TelegramClientDAO)
                usage_dao = dao_factory(WorkerAccountUsageDAO)

                while time.monotonic() - start_time < timeout:
                    available_client = await client_dao.find_available_account()
                    if available_client is not None:
                        client_id = available_client.telegram_id
                        session_str = available_client.session_string

                        locked_until = datetime.now(timezone.utc) + timedelta(
                            hours=self.settings.account_lock_hours
                        )
                        await usage_dao.create(
                            telegram_id=client_id,
                            worker_id=self.settings.worker_id,
                            locked_until=locked_until,
                            reason="usage",
                        )
                        await dao_factory.commit()
                        break

                    await asyncio.sleep(min(1.0, timeout / 5))

            if client_id is None or session_str is None:
                raise TimeoutError("Cannot get session from mysql database")

            telegram_session = TelegramSession(
                user_id=client_id,
                session=StringSession(session_str),
            )

            logger.info("session_yielded", user_id=client_id)
            acquire_span.set_attribute("session.user_id", client_id)

        try:
            yield telegram_session

            with self.tracer.start_as_current_span(
                "postgres_session_storage.handle_session_usage_completed"
            ) as usage_span:
                usage_span.set_attribute("session.user_id", telegram_session.user_id)
                usage_span.set_attribute("worker.id", self.settings.worker_id)
                # Успех
                self.logger.info(
                    "postgres_session_usage_completed", user_id=telegram_session.user_id
                )
                async with self.multiple_dao_factory() as dao_factory:
                    usage_dao = dao_factory(WorkerAccountUsageDAO)
                    await usage_dao.create(
                        telegram_id=telegram_session.user_id,
                        worker_id=self.settings.worker_id,
                        locked_until=datetime.now(timezone.utc),
                        reason="usage_completed",
                    )
                    await dao_factory.commit()

        except ClientBanned as e:
            raise InvalidClient("Client is banned", telegram_session.user_id) from e

        except InvalidClient as e:
            raise InvalidClient(e.message, telegram_session.user_id) from e

        except FloodWait as e:
            with self.tracer.start_as_current_span(
                "postgres_session_storage.handle_flood_wait"
            ) as err_span:
                err_span.set_attribute("session.user_id", telegram_session.user_id)
                err_span.set_attribute("worker.id", self.settings.worker_id)
                self.logger.warning(
                    "flood_wait_delaying_session",
                    user_id=telegram_session.user_id,
                    delay_seconds=e.seconds + 10,
                )
                err_span.set_status(Status(StatusCode.ERROR, "Flood wait"))
                err_span.record_exception(e)

                async with self.multiple_dao_factory() as dao_factory:
                    usage_dao = dao_factory(WorkerAccountUsageDAO)
                    await usage_dao.create(
                        telegram_id=telegram_session.user_id,
                        worker_id=self.settings.worker_id,
                        locked_until=datetime.now(timezone.utc)
                        + timedelta(seconds=e.seconds + 10),
                        reason="flood_wait",
                    )
                    await dao_factory.commit()

            raise

        except Exception as e:
            with self.tracer.start_as_current_span(
                "postgres_session_storage.handle_unknown_error"
            ) as err_span:
                err_span.set_attribute("session.user_id", telegram_session.user_id)
                err_span.set_attribute("worker.id", self.settings.worker_id)
                self.logger.error("unhandled_session_usage_error", exc_info=True)
                err_span.set_status(
                    Status(StatusCode.ERROR, "Unhandled error during session usage")
                )
                err_span.record_exception(e)
            raise
