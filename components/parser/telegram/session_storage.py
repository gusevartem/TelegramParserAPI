import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Annotated, Any, ClassVar, NewType, Protocol, override

import aio_pika
import structlog
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    PlainSerializer,
    ValidationError,
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


SessionStorageChannel = NewType("SessionStorageChannel", aio_pika.abc.AbstractChannel)


class ITelegramSessionStorage(Protocol):
    async def add_session(self, user_id: int, session: str) -> None: ...
    def get_session(
        self, timeout: int = 5
    ) -> AbstractAsyncContextManager[TelegramSession]: ...


class RabbitMQSessionStorage(ITelegramSessionStorage):
    _configured: ClassVar[bool] = False

    def __init__(self, channel: SessionStorageChannel, settings: TelegramSettings):
        self._channel: aio_pika.abc.AbstractChannel = channel
        self.settings: TelegramSettings = settings

        self.logger: structlog.BoundLogger = structlog.get_logger("session_storage")
        self.tracer: trace.Tracer = trace.get_tracer("session_storage")

    @classmethod
    async def setup(
        cls,
        channel: aio_pika.abc.AbstractChannel,
        settings: TelegramSettings,
        logger: structlog.BoundLogger,
    ):
        if cls._configured:
            return

        logger.info("configuring_session_storage", stage="start")
        logger.info(
            "declaring_session_queue",
            queue_name=settings.session_storage_queue_name,
            queue_type="quorum",
        )
        queue = await channel.declare_queue(
            settings.session_storage_queue_name,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )

        logger.info(
            "declaring_delayed_exchange",
            exchange_name=settings.session_storage_delayed_exchange_name,
        )
        delayed_exchange = await channel.declare_exchange(
            settings.session_storage_delayed_exchange_name,
            type=aio_pika.abc.ExchangeType.X_DELAYED_MESSAGE,
            arguments={"x-delayed-type": "direct"},
            durable=True,
        )
        await queue.bind(
            delayed_exchange, routing_key=settings.session_storage_queue_name
        )

        logger.info("session_storage_configured", stage="complete")
        cls._configured = True

    @override
    @asynccontextmanager
    async def get_session(self, timeout: int = 5) -> AsyncIterator[TelegramSession]:
        """Получение сессии из очереди

        Args:
            timeout (int, optional): Таймаут. Defaults to 5.

        Raises:
            TimeoutError: Случился таймаут при получении сессии
            InvalidClient: Если сообщение из очереди не прошло валидацию
            InvalidClient: Обернутое ClientBanned c указанием user_id

        Returns:
            AsyncIterator[TelegramSession]: Сессия

        Yields:
            Iterator[AsyncIterator[TelegramSession]]: Сессия
        """
        with self.tracer.start_as_current_span("session_storage.get_session") as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute(
                "messaging.destination", self.settings.session_storage_queue_name
            )
            span.set_attribute("messaging.operation", "consume")
            span.set_attribute("consume.timeout_seconds", timeout)

            logger = self.logger.bind(
                queue=self.settings.session_storage_queue_name, timeout=timeout
            )

            await self.setup(self._channel, self.settings, logger)

            queue = await self._channel.get_queue(
                self.settings.session_storage_queue_name
            )

            logger.info("waiting_for_session", stage="start")
            span.add_event("consume_started")

            async with queue.iterator() as queue_iter:
                try:
                    message = await asyncio.wait_for(
                        queue_iter.__anext__(), timeout=timeout
                    )
                except (asyncio.TimeoutError, StopAsyncIteration) as e:
                    logger.warning("session_consume_timeout", timeout=timeout)
                    span.add_event("consume_timeout")
                    raise TimeoutError("Cannot get session from queue") from e

            try:
                telegram_session = TelegramSession.model_validate_json(message.body)
            except ValidationError as e:
                logger.error("invalid_session_message", error=str(e), exc_info=True)
                span.set_status(Status(StatusCode.ERROR, "Invalid session message"))
                span.record_exception(e)
                await message.reject(requeue=False)
                raise InvalidClient("Unexpected message in session queue") from e

            logger.info(
                "session_received", user_id=telegram_session.user_id, stage="success"
            )
            span.set_attribute("session.user_id", telegram_session.user_id)
            span.add_event("session_received")

            try:
                yield telegram_session

                # Успех
                logger.info(
                    "returning_session_to_queue", user_id=telegram_session.user_id
                )
                await self._channel.default_exchange.publish(
                    aio_pika.Message(
                        body=TelegramSession.model_dump_json(telegram_session).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        content_type="application/json",
                    ),
                    routing_key=self.settings.session_storage_queue_name,
                )

                await message.ack()
                span.add_event("session_returned")

            except ClientBanned as e:
                # Бан
                logger.warning(
                    "client_banned_removing_session", user_id=telegram_session.user_id
                )
                span.set_status(Status(StatusCode.ERROR, "Client banned"))
                span.record_exception(e)
                await message.ack()
                raise InvalidClient("Client is banned", telegram_session.user_id) from e

            except InvalidClient as e:
                # Не валидная сессия
                logger.warning(
                    "client_invalid_removing_session",
                    user_id=telegram_session.user_id,
                    reason=e.message,
                )
                span.set_status(Status(StatusCode.ERROR, "Client invalid"))
                span.record_exception(e)
                await message.ack()
                raise InvalidClient(e.message, telegram_session.user_id) from e

            except FloodWait as e:
                # Флуд
                delay_ms = (e.seconds + 10) * 1000
                logger.warning(
                    "flood_wait_delaying_session",
                    user_id=telegram_session.user_id,
                    delay_seconds=e.seconds + 10,
                    delay_ms=delay_ms,
                )
                span.add_event("flood_wait_delay", {"delay_ms": delay_ms})

                delayed_exchange = await self._channel.get_exchange(
                    self.settings.session_storage_delayed_exchange_name
                )
                await delayed_exchange.publish(
                    aio_pika.Message(
                        body=TelegramSession.model_dump_json(telegram_session).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        headers={"x-delay": delay_ms},
                    ),
                    routing_key=self.settings.session_storage_queue_name,
                )

                await message.ack()
                raise

            except Exception as e:
                # Если упало что-то внутри бизнес-логики (не связанное с сессией),
                # возвращаем сообщение в очередь.
                logger.error("unhandled_session_usage_error_requeueing", exc_info=True)
                span.set_status(
                    Status(StatusCode.ERROR, "Unhandled error during session usage")
                )
                span.record_exception(e)
                await message.reject(requeue=True)
                raise

    @override
    async def add_session(self, user_id: int, session: str) -> None:
        with self.tracer.start_as_current_span("session_storage.add_session") as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute(
                "messaging.destination", self.settings.session_storage_queue_name
            )
            span.set_attribute("messaging.operation", "publish")
            span.set_attribute("session.user_id", user_id)

            logger = self.logger.bind(
                user_id=user_id, queue=self.settings.session_storage_queue_name
            )

            await self.setup(self._channel, self.settings, logger)

            logger.info("adding_session", stage="start")
            span.add_event("add_started")

            telegram_session = TelegramSession(
                user_id=user_id,
                session=StringSession(session),
            )
            await self._channel.default_exchange.publish(
                aio_pika.Message(
                    body=TelegramSession.model_dump_json(telegram_session).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                ),
                routing_key=self.settings.session_storage_queue_name,
            )
            logger.info("session_added", stage="success")
            span.add_event("add_completed")
