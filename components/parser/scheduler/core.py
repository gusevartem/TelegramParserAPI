from collections.abc import AsyncIterable
from datetime import datetime, timedelta, timezone

from dishka import Provider, Scope, provide
from dishka.dependency_source import CompositeDependencySource
from parser.dto import ParsingTask as ParsingTaskDTO
from parser.persistence import (
    ParsingTaskDAO,
    ParsingTaskStatus,
)
from pydantic import HttpUrl, TypeAdapter


def calculate_next_run(
    bucket: int,
    last_parsed_at: datetime | None,
    created_at: datetime,
    status: ParsingTaskStatus,
) -> datetime | None:
    if status not in (
        ParsingTaskStatus.IDLE,
        ParsingTaskStatus.PROCESSING,
    ):
        return None

    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    bucket_time_today = today_start + timedelta(minutes=bucket)

    if last_parsed_at is not None:
        return (
            bucket_time_today
            if last_parsed_at.astimezone(timezone.utc) < today_start
            else bucket_time_today + timedelta(days=1)
        )

    return (
        bucket_time_today
        if created_at.astimezone(timezone.utc) < today_start
        else bucket_time_today + timedelta(days=1)
    )


class AddTask:
    def __init__(self, parsing_task_dao: ParsingTaskDAO):
        self.parsing_task_dao: ParsingTaskDAO = parsing_task_dao

    async def __call__(self, url: str) -> ParsingTaskDTO:
        http_url = TypeAdapter(HttpUrl).validate_python(url)
        clean_url = f"{http_url.scheme}://{http_url.host}{http_url.path}".rstrip("/")

        async def create_task_and_generate_dto(bucket: int) -> ParsingTaskDTO:
            new_task = await self.parsing_task_dao.create(clean_url, bucket)
            await self.parsing_task_dao.refresh(new_task)

            next_run = calculate_next_run(
                new_task.bucket,
                new_task.last_parsed_at,
                new_task.created_at,
                new_task.status,
            )
            result = ParsingTaskDTO.from_persistence(
                new_task, int(next_run.timestamp()) if next_run else None
            )

            await self.parsing_task_dao.commit()
            return result

        existing_task = await self.parsing_task_dao.find_by_url(clean_url)
        if existing_task:
            next_run = calculate_next_run(
                existing_task.bucket,
                existing_task.last_parsed_at,
                existing_task.created_at,
                existing_task.status,
            )
            return ParsingTaskDTO.from_persistence(
                existing_task, int(next_run.timestamp()) if next_run else None
            )

        buckets_loadings = await self.parsing_task_dao.get_buckets_loading()
        minutes_in_hour = 24 * 60
        if len(buckets_loadings) < minutes_in_hour:
            for bucket in range(minutes_in_hour):
                if bucket not in buckets_loadings:
                    return await create_task_and_generate_dto(bucket)

        return await create_task_and_generate_dto(
            min(buckets_loadings.items(), key=lambda x: x[1])[0]
        )


class GetTasks:
    def __init__(self, parsing_task_dao: ParsingTaskDAO):
        self.parsing_task_dao: ParsingTaskDAO = parsing_task_dao

    async def __call__(
        self, set_processing: bool, tasks_limit: int
    ) -> AsyncIterable[ParsingTaskDTO]:
        now = datetime.now(timezone.utc)
        current_minute = now.hour * 60 + now.minute
        tasks_persistence = await self.parsing_task_dao.get_scheduled_tasks(
            current_minute_of_day=current_minute, limit=tasks_limit
        )
        for task in tasks_persistence:
            if set_processing:
                task.status = ParsingTaskStatus.PROCESSING
                await self.parsing_task_dao.save(task)

            next_run = calculate_next_run(
                task.bucket,
                task.last_parsed_at,
                task.created_at,
                task.status,
            )
            yield ParsingTaskDTO.from_persistence(
                task, int(next_run.timestamp()) if next_run else None
            )

        await self.parsing_task_dao.commit()


class SchedulerProvider(Provider):
    add_task: CompositeDependencySource = provide(
        AddTask,
        provides=AddTask,
        scope=Scope.REQUEST,
    )
    get_tasks: CompositeDependencySource = provide(
        GetTasks,
        provides=GetTasks,
        scope=Scope.REQUEST,
    )
