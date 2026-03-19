import asyncio

import structlog
from dishka import make_async_container
from opentelemetry import trace
from parser.logging import LoggingSettings, LoggingSettingsProvider, setup_logging
from parser.persistence import PersistenceProvider
from parser.scheduler import GetTasks, SchedulerProvider


async def run_scheduler() -> None:
    container = make_async_container(
        SchedulerProvider(),
        PersistenceProvider(),
        LoggingSettingsProvider(),
    )
    settings = await container.get(LoggingSettings)
    setup_logging(settings, "scheduler", "scheduler")

    logger: structlog.BoundLogger = structlog.get_logger("scheduler")
    tracer = trace.get_tracer("scheduler")

    try:
        while True:
            with tracer.start_as_current_span("scheduler_iteration") as span:
                try:
                    task_limit = 1000
                    span.set_attribute("iteration.tasks_limit", task_limit)
                    logger.info("running_iteration", stage="start")

                    async with container() as request_container:
                        get_tasks = await request_container.get(GetTasks)
                        scheduled_count = 0
                        async for task in get_tasks(
                            set_processing=True, tasks_limit=task_limit
                        ):
                            with tracer.start_as_current_span(
                                "schedule_task"
                            ) as task_span:
                                task_span.set_attribute("task.id", str(task.id))
                                task_span.set_attribute("task.url", task.url)
                                if task.channel_id is not None:
                                    task_span.set_attribute(
                                        "task.channel_id", task.channel_id
                                    )
                                scheduled_count += 1

                    logger.info("iteration_completed", stage="complete")
                    span.set_attribute("iteration.scheduled_tasks", scheduled_count)
                except asyncio.CancelledError:
                    logger.info("iteration_cancelled")
                    break
                except Exception as e:
                    logger.error("iteration_failed", exc_info=True)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    span.record_exception(e)

            logger.info("sleeping", duration_seconds=30)
            await asyncio.sleep(30)
    finally:
        await container.close()


def run():
    try:
        asyncio.run(run_scheduler())
    except KeyboardInterrupt:
        pass
