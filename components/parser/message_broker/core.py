import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import ClassVar, NewType, Protocol, override

import aio_pika
import structlog
from dishka import Provider, Scope, provide
from dishka.dependency_source import CompositeDependencySource
from opentelemetry import trace
from opentelemetry.instrumentation.aio_pika import AioPikaInstrumentor
from opentelemetry.trace import Status, StatusCode
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
        self.logger: structlog.BoundLogger = structlog.get_logger("message_broker")
        self.tracer: trace.Tracer = trace.get_tracer("message_broker")

    @classmethod
    async def setup(
        cls,
        channel: aio_pika.abc.AbstractChannel,
        settings: MessageBrokerSettings,
        logger: structlog.BoundLogger,
    ):
        if cls._configured:
            return

        logger.info("configuring_message_broker", stage="start")
        logger.info(
            "declaring_tasks_queue",
            queue_name=settings.parsing_tasks_queue_name,
            queue_type="quorum",
        )
        await channel.declare_queue(
            settings.parsing_tasks_queue_name,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )

        logger.info("message_broker_configured", stage="complete")
        cls._configured = True

    @override
    async def publish_task(self, task: ParsingTask) -> None:
        with self.tracer.start_as_current_span("message_broker.publish_task") as span:
            span.set_attribute("messaging.system", "rabbitmq")
            span.set_attribute(
                "messaging.destination", self.settings.parsing_tasks_queue_name
            )
            span.set_attribute("messaging.operation", "publish")
            span.set_attribute("task.id", str(task.id))
            span.set_attribute("task.url", task.url)

            task_logger = self.logger.bind(
                task_id=str(task.id),
                task_url=task.url,
                queue=self.settings.parsing_tasks_queue_name,
            )
            task_logger.info("configure_message_broker", stage="start")
            await self.setup(self._channel, self.settings, task_logger)
            task_logger.info("configure_message_broker", stage="complete")

            task_logger.info("publishing_task", stage="start")
            try:
                await self._channel.default_exchange.publish(
                    aio_pika.Message(
                        body=ParsingTask.model_dump_json(task).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        content_type="application/json",
                    ),
                    routing_key=self.settings.parsing_tasks_queue_name,
                )
                task_logger.info("task_published", stage="complete")
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                task_logger.error("task_publish_failed", exc_info=True)
                raise

    @override
    @asynccontextmanager
    async def get_task(self, timeout: int = 5) -> AsyncIterator[ParsingTask]:
        logger = self.logger.bind(
            queue=self.settings.parsing_tasks_queue_name, timeout=timeout
        )

        await self.setup(self._channel, self.settings, logger)

        logger.info("waiting_for_task", stage="start")

        queue = await self._channel.get_queue(self.settings.parsing_tasks_queue_name)

        async with queue.iterator() as queue_iter:
            try:
                message = await asyncio.wait_for(
                    queue_iter.__anext__(), timeout=timeout
                )
            except (asyncio.TimeoutError, StopAsyncIteration) as e:
                logger.info("task_consume_timeout")
                raise TimeoutError("Cannot get task from queue") from e

        async with message.process(ignore_processed=True, requeue=True):
            try:
                task = ParsingTask.model_validate_json(message.body.decode())
            except ValidationError as e:
                logger.error("invalid_message_received", exc_info=True)
                await message.reject(requeue=False)
                raise InvalidMessageError(
                    f"Unexpected message in task queue: {str(e)}"
                ) from e

            task_logger = logger.bind(task_id=str(task.id), task_url=task.url)
            task_logger.info("task_received", stage="complete")

            try:
                yield task
                task_logger.info("task_processed_successfully")

            except InvalidTask:
                task_logger.error("invalid_task", exc_info=True)
                await message.reject(requeue=False)
                raise
            except Exception:
                task_logger.error("task_processing_failed_requeueing", exc_info=True)
                raise


class MessageBrokerProvider(Provider):
    @provide(scope=Scope.APP)
    def settings(self) -> MessageBrokerSettings:
        return MessageBrokerSettings()  # type: ignore # pyright: ignore

    @provide(scope=Scope.APP)
    async def connection(
        self, settings: MessageBrokerSettings
    ) -> AsyncIterator[aio_pika.abc.AbstractConnection]:
        AioPikaInstrumentor().instrument()
        connection = await aio_pika.connect_robust(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_login,
            password=settings.rabbitmq_password,
        )
        yield connection
        await connection.close()

    @provide(scope=Scope.APP)
    async def channel(
        self, connection: aio_pika.abc.AbstractConnection
    ) -> AsyncIterator[BrokerChannel]:
        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        await channel.set_qos(prefetch_count=1)

        yield BrokerChannel(channel)

        await channel.close()

    message_broker: CompositeDependencySource = provide(
        MessageBroker,
        scope=Scope.APP,
        provides=IMessageBroker,
    )
