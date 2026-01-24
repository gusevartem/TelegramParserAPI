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

from .exceptions import AllClientsAreBusyError, ClientBanned, FloodWait
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
        cls, channel: aio_pika.abc.AbstractChannel, settings: TelegramSettings
    ):
        if cls._configured:
            return
        queue = await channel.declare_queue(
            settings.session_storage_queue_name,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        await queue.bind(
            channel.default_exchange,
            routing_key=settings.session_storage_queue_name,
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

    @override
    @asynccontextmanager
    async def get_session(self, timeout: int = 5) -> AsyncIterator[TelegramSession]:
        await self.setup(self._channel, self.settings)

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
            except (asyncio.TimeoutError, StopAsyncIteration):
                self.logger.warning(
                    "⚠️ Cannot get session from queue: "
                    + f"{self.settings.session_storage_queue_name}, timeout: {timeout}s"
                )
                raise AllClientsAreBusyError("Cannot get session from queue")

            async with message.process(ignore_processed=True, requeue=True):
                try:
                    telegram_session = TelegramSession.model_validate_json(message.body)
                except ValidationError as e:
                    self.logger.error(
                        f"❌ Unexpected message in session queue: {str(e)}"
                    )
                    await message.reject(requeue=False)
                    raise

                self.logger.info(
                    f"✅ Got session for user_id: {telegram_session.user_id}"
                )
                recycle = True
                try:
                    yield telegram_session

                except ClientBanned:
                    self.logger.warning(
                        f"⚠️ Client is banned, user_id: {telegram_session.user_id}. "
                        + "Removing session from queue: "
                        + f"{self.settings.session_storage_queue_name}"
                    )
                    recycle = False
                    await message.ack()
                    raise

                except FloodWait as e:
                    delay_ms = (e.seconds + 10) * 1000
                    self.logger.warning(
                        f"⏳ FloodWait {e.seconds}s, "
                        + f"user_id: {telegram_session.user_id}. "
                        + "Adding session to delayed exchange: "
                        + f"{self.settings.session_storage_delayed_exchange_name}"
                        + f"for {delay_ms}ms"
                    )
                    delayed_exchange = await self._channel.get_exchange(
                        self.settings.session_storage_delayed_exchange_name
                    )
                    await delayed_exchange.publish(
                        aio_pika.Message(
                            body=TelegramSession.model_dump_json(
                                telegram_session
                            ).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            headers={"x-delay": delay_ms},
                        ),
                        routing_key=self.settings.session_storage_queue_name,
                    )
                    recycle = False
                    await message.ack()
                    raise

                finally:
                    if recycle:
                        self.logger.info(
                            "✅ Returning session for user_id: "
                            + f"{telegram_session.user_id} back to queue"
                        )
                        await self._channel.default_exchange.publish(
                            aio_pika.Message(
                                body=TelegramSession.model_dump_json(
                                    telegram_session
                                ).encode(),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                                content_type="application/json",
                            ),
                            routing_key=self.settings.session_storage_queue_name,
                        )

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
