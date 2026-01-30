import asyncio
from importlib.metadata import version

import structlog
from dishka import make_async_container
from opentelemetry import trace
from parser.logging import setup_logging
from parser.message_broker import IMessageBroker, MessageBrokerProvider
from parser.persistence import PersistenceProvider
from parser.scheduler import GetTasks, SchedulerProvider
from parser.settings import ProjectSettings, ProjectSettingsProvider


async def run_scheduler() -> None:
    container = make_async_container(
        SchedulerProvider(),
        MessageBrokerProvider(),
        PersistenceProvider(),
        ProjectSettingsProvider(),
    )
    settings = await container.get(ProjectSettings)
    setup_logging(settings, "scheduler", version("scheduler"))

    message_broker = await container.get(IMessageBroker)

    logger: structlog.BoundLogger = structlog.get_logger("scheduler")
    tracer = trace.get_tracer("scheduler")

    try:
        while True:
            with tracer.start_as_current_span("scheduler_iteration") as span:
                try:
                    task_limit = 1000
                    span.set_attribute("iteration.tasks_limit", task_limit)
                    logger.info("running_iteration")

                    async with container() as request_container:
                        get_tasks = await request_container.get(GetTasks)
                        published_count = 0
                        async for task in get_tasks(
                            set_processing=True, tasks_limit=task_limit
                        ):
                            with tracer.start_as_current_span(
                                "publish_task"
                            ) as task_span:
                                task_span.set_attribute("task.id", str(task.id))
                                task_span.set_attribute("task.url", task.url)
                                task_logger = logger.bind(
                                    task_id=str(task.id),
                                    task_url=task.url,
                                )
                                if task.channel_id is not None:
                                    task_logger = task_logger.bind(
                                        channel_id=task.channel_id
                                    )
                                    task_span.set_attribute(
                                        "task.channel_id", task.channel_id
                                    )
                                try:
                                    task_logger.info("publishing_task")
                                    task_span.add_event("task_publish_started")
                                    await message_broker.publish_task(task)
                                    task_span.add_event("task_publish_completed")
                                    task_logger.info("task_published")
                                    published_count += 1
                                except Exception as e:
                                    task_span.set_status(
                                        trace.Status(trace.StatusCode.ERROR, str(e))
                                    )
                                    task_span.record_exception(e)
                                    task_logger.error(
                                        "task_publish_failed", exc_info=True
                                    )

                    logger.info("iteration_completed")
                    span.set_attribute("iteration.published_tasks", published_count)
                except asyncio.CancelledError:
                    logger.info("iteration_cancelled")
                    break
                except Exception as e:
                    logger.error("iteration_failed", exc_info=True)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    span.record_exception(e)

            logger.info("sleeping", duration_seconds=60)
            await asyncio.sleep(60)
    finally:
        await container.close()


def run():
    try:
        asyncio.run(run_scheduler())
    except KeyboardInterrupt:
        pass
