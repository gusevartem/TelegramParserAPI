import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from logging import Logger, getLogger
from typing import ClassVar, NewType, Protocol, override

import aio_pika
from dishka import Provider, Scope, provide
from dishka.dependency_source import CompositeDependencySource
from parser.dto import ParsingTask
from pydantic import ValidationError

from .exceptions import InvalidMessageError, InvalidTask
from .settings import MessageBrokerSettings


class IMessageBroker(Protocol):
    async def publish_task(self, task: ParsingTask) -> None: ...
    def get_task(
        self, timeout: int = 5
    ) -> AbstractAsyncContextManager[ParsingTask]: ...


BrokerChannel = NewType("BrokerChannel", aio_pika.abc.AbstractChannel)


class MessageBroker(IMessageBroker):
    _configured: ClassVar[bool] = False

    def __init__(self, channel: BrokerChannel, settings: MessageBrokerSettings) -> None:
        self._channel: aio_pika.abc.AbstractChannel = channel
        self.settings: MessageBrokerSettings = settings
        self.logger: Logger = getLogger(__name__)

    @classmethod
    async def setup(
        cls,
        channel: aio_pika.abc.AbstractChannel,
        settings: MessageBrokerSettings,
        logger: Logger,
    ):
        if cls._configured:
            return
        logger.info("⌛ Configuring message broker")
        logger.info(f"⌛ Declaring tasks queue: {settings.parsing_tasks_queue_name}")
        queue = await channel.declare_queue(
            settings.parsing_tasks_queue_name,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        await queue.bind(
            channel.default_exchange,
            routing_key=settings.parsing_tasks_queue_name,
        )

        logger.info("✅ Message broker configured")
        cls._configured = True

    @override
    async def publish_task(self, task: ParsingTask) -> None:
        await self.setup(self._channel, self.settings, self.logger)

        self.logger.info(f"⌛ Publishing task with id: {task.id}")
        await self._channel.default_exchange.publish(
            aio_pika.Message(
                body=ParsingTask.model_dump_json(task).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
            ),
            routing_key=self.settings.parsing_tasks_queue_name,
        )
        self.logger.info(f"✅ Task with id: {task.id} published")

    @override
    @asynccontextmanager
    async def get_task(self, timeout: int = 5) -> AsyncIterator[ParsingTask]:
        await self.setup(self._channel, self.settings, self.logger)

        queue = await self._channel.get_queue(self.settings.parsing_tasks_queue_name)
        await self._channel.set_qos(prefetch_count=1)
        self.logger.info(f"⌛ Getting task with timeout: {timeout}")

        async with queue.iterator() as queue_iter:
            try:
                self.logger.info(
                    "⌛ Waiting for task in queue: "
                    + f"{self.settings.parsing_tasks_queue_name}"
                )
                message = await asyncio.wait_for(
                    queue_iter.__anext__(), timeout=timeout
                )
            except (asyncio.TimeoutError, StopAsyncIteration) as e:
                self.logger.info(
                    "⚠️ Cannot get task from queue "
                    + f"{self.settings.parsing_tasks_queue_name}, timeout: {timeout}"
                )
                raise TimeoutError("Cannot get task from queue") from e

        async with message.process(ignore_processed=True, requeue=True):
            try:
                task = ParsingTask.model_validate_json(message.body.decode())
            except ValidationError as e:
                self.logger.error(
                    f"❌ Unexpected message in task queue: {str(e)}",
                    exc_info=True,
                )
                await message.reject(requeue=False)
                raise InvalidMessageError(
                    f"Unexpected message in task queue: {str(e)}"
                ) from e
            self.logger.info(f"✅ Got task with id: {task.id}. URL: {task.url}")
            try:
                yield task
                self.logger.info(
                    f"✅ Task with id: {task.id}, "
                    + f"URL: {task.url} processed successfully"
                )
            except InvalidTask as e:
                self.logger.error(f"❌ Invalid task: {str(e)}", exc_info=True)
                await message.reject(requeue=False)
                raise InvalidTask(f"Invalid task: {str(e)}") from e
            except Exception:
                self.logger.error("❌ Unexpected error, requeueing...", exc_info=True)
                raise


class MessageBrokerProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> MessageBrokerSettings:
        return MessageBrokerSettings()  # type: ignore # pyright: ignore

    @provide(scope=Scope.APP)
    async def connection(
        self, settings: MessageBrokerSettings
    ) -> AsyncIterator[aio_pika.abc.AbstractConnection]:
        connection = await aio_pika.connect_robust(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_login,
            password=settings.rabbitmq_password,
        )
        yield connection
        await connection.close()

    @provide(scope=Scope.REQUEST)
    async def channel(
        self, connection: aio_pika.abc.AbstractConnection
    ) -> AsyncIterator[BrokerChannel]:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        yield BrokerChannel(channel)
        await channel.close()

    message_broker: CompositeDependencySource = provide(
        MessageBroker,
        scope=Scope.REQUEST,
        provides=IMessageBroker,
    )
