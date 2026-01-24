import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from logging import Logger, getLogger
from typing import Annotated, Any, ClassVar, Protocol, override

import aio_pika
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


class ITelegramSessionStorage(Protocol):
    async def add_session(self, user_id: int, session: str) -> None: ...
    def get_session(
        self, timeout: int = 5
    ) -> AbstractAsyncContextManager[TelegramSession]: ...


class RabbitMQSessionStorage(ITelegramSessionStorage):
    _configured: ClassVar[bool] = False

    def __init__(
        self, channel: aio_pika.abc.AbstractChannel, settings: TelegramSettings
    ):
        self._channel: aio_pika.abc.AbstractChannel = channel
        self.settings: TelegramSettings = settings
        self.logger: Logger = getLogger(__name__)

    @classmethod
    async def setup(
        cls,
        channel: aio_pika.abc.AbstractChannel,
        settings: TelegramSettings,
        logger: Logger,
    ):
        if cls._configured:
            return
        logger.info("⌛ Configuring session storage")
        logger.info(
            f"⌛ Declaring session storage queue: {settings.session_storage_queue_name}"
        )
        queue = await channel.declare_queue(
            settings.session_storage_queue_name,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        await queue.bind(
            channel.default_exchange,
            routing_key=settings.session_storage_queue_name,
        )

        logger.info(
            "⌛ Declaring session storage delayed exchange: "
            + f"{settings.session_storage_delayed_exchange_name}"
        )
        delayed_exchange = await channel.declare_exchange(
            settings.session_storage_delayed_exchange_name,
            type="x-delayed-message",
            arguments={"x-delayed-type": "direct"},
            durable=True,
        )
        await queue.bind(
            delayed_exchange, routing_key=settings.session_storage_queue_name
        )

        logger.info("✅ Session storage configured")
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
        await self.setup(self._channel, self.settings, self.logger)

        queue = await self._channel.get_queue(self.settings.session_storage_queue_name)
        await self._channel.set_qos(prefetch_count=1)

        async with queue.iterator() as queue_iter:
            try:
                self.logger.info(
                    "⌛ Waiting for session in queue: "
                    + f"{self.settings.session_storage_queue_name}"
                )
                message = await asyncio.wait_for(
                    queue_iter.__anext__(), timeout=timeout
                )
            except (asyncio.TimeoutError, StopAsyncIteration) as e:
                self.logger.warning(
                    "⚠️ Cannot get session from queue: "
                    + f"{self.settings.session_storage_queue_name}, timeout: {timeout}s"
                )
                raise TimeoutError("Cannot get session from queue") from e

            try:
                telegram_session = TelegramSession.model_validate_json(message.body)
            except ValidationError as e:
                self.logger.error(f"❌ Unexpected message in session queue: {str(e)}")
                await message.reject(requeue=False)
                raise InvalidClient("Unexpected message in session queue") from e

            self.logger.info(f"✅ Got session for user_id: {telegram_session.user_id}")
            try:
                yield telegram_session

                # Успех
                self.logger.info(
                    f"✅ Returning session for user_id: {telegram_session.user_id} "
                    + "back to queue"
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

            except ClientBanned as e:
                # Бан
                self.logger.warning(
                    f"⚠️ Client is banned, user_id: {telegram_session.user_id}. "
                    + "Removing session from queue."
                )

                await message.ack()
                raise InvalidClient("Client is banned", telegram_session.user_id) from e

            except InvalidClient as e:
                # Не валидная сессия
                self.logger.warning(
                    f"⚠️ Client is invalid, user_id: {telegram_session.user_id}. "
                    + "Removing session from queue."
                )
                await message.ack()
                raise InvalidClient(e.message, telegram_session.user_id) from e

            except FloodWait as e:
                # Флуд
                delay_ms = (e.seconds + 10) * 1000
                self.logger.warning(
                    f"⏳ FloodWait {e.seconds}s, user_id: {telegram_session.user_id}. "
                    + f"Delaying for {delay_ms}ms"
                )

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

            except Exception:
                # Если упало что-то внутри бизнес-логики (не связанное с сессией),
                # возвращаем сообщение в очередь.
                self.logger.error("❌ Unhandled exception during session usage")
                await message.reject(requeue=True)
                raise

    @override
    async def add_session(self, user_id: int, session: str) -> None:
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
