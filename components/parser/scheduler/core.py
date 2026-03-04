from collections.abc import AsyncIterable
from datetime import datetime, timedelta, timezone

from dishka import Provider, Scope, provide
from dishka.dependency_source import CompositeDependencySource
from parser.dto import ParsingTask as ParsingTaskDTO
from parser.persistence import (
    ParsingTask as ParsingTaskPersistence,
)
from parser.persistence import (
    ParsingTaskDAO,
    ParsingTaskStatus,
    TaskClaimHistoryDAO,
)
from pydantic import HttpUrl, TypeAdapter
from sqlalchemy.exc import IntegrityError


def calculate_next_run(
    bucket: int,
    last_parsed_at: datetime | None,
    created_at: datetime,
    status: ParsingTaskStatus,
) -> datetime | None:
    if status not in (
        ParsingTaskStatus.IDLE,
        ParsingTaskStatus.SKIP,
    ):
        return None

    now = datetime.now(timezone.utc)
    this_hour_start = now.replace(minute=0, second=0, microsecond=0)

    bucket_time_this_hour = this_hour_start + timedelta(minutes=bucket)
    reference_time = last_parsed_at if last_parsed_at is not None else created_at

    return (
        bucket_time_this_hour
        if reference_time.astimezone(timezone.utc) < bucket_time_this_hour
        else bucket_time_this_hour + timedelta(hours=1)
    )


class AddTask:
    def __init__(self, parsing_task_dao: ParsingTaskDAO):
        self.parsing_task_dao: ParsingTaskDAO = parsing_task_dao

    async def __call__(self, url: str) -> ParsingTaskDTO:
        http_url = TypeAdapter(HttpUrl).validate_python(url)
        clean_url = f"{http_url.scheme}://{http_url.host}{http_url.path}".rstrip("/")

        existing_task = await self.parsing_task_dao.find_by_url(clean_url)
        if existing_task:
            return self._generate_dto_from_task(existing_task)

        buckets_loadings = await self.parsing_task_dao.get_buckets_loading()
        minutes_in_cycle = 60
        selected_bucket: int | None = None

        if len(buckets_loadings) < minutes_in_cycle:
            for bucket in range(minutes_in_cycle):
                if bucket not in buckets_loadings:
                    selected_bucket = bucket
                    break

        if selected_bucket is None:
            selected_bucket = min(buckets_loadings.items(), key=lambda x: x[1])[0]  # pyright: ignore[reportIndexIssue]

        try:
            return await self._create_task_and_generate_dto(clean_url, selected_bucket)
        except IntegrityError:
            await self.parsing_task_dao.rollback()

            existing_task = await self.parsing_task_dao.find_by_url(clean_url)
            if not existing_task:
                raise
            return self._generate_dto_from_task(existing_task)

    async def _create_task_and_generate_dto(
        self, url: str, bucket: int
    ) -> ParsingTaskDTO:
        new_task = await self.parsing_task_dao.create(url, bucket)
        await self.parsing_task_dao.refresh(new_task)

        result = self._generate_dto_from_task(new_task)
        await self.parsing_task_dao.commit()
        return result

    def _generate_dto_from_task(self, task: ParsingTaskPersistence) -> ParsingTaskDTO:
        next_run = calculate_next_run(
            task.bucket,
            task.last_parsed_at,
            task.created_at,
            task.status,
        )
        return ParsingTaskDTO.from_persistence(
            task, int(next_run.timestamp()) if next_run else None
        )


class GetTasks:
    def __init__(
        self,
        parsing_task_dao: ParsingTaskDAO,
        task_claim_history_dao: TaskClaimHistoryDAO,
    ):
        self.parsing_task_dao: ParsingTaskDAO = parsing_task_dao
        self.task_claim_history_dao: TaskClaimHistoryDAO = task_claim_history_dao

    async def __call__(
        self, set_processing: bool, tasks_limit: int, worker_id: str = "scheduler"
    ) -> AsyncIterable[ParsingTaskDTO]:
        tasks_persistence = await self.parsing_task_dao.get_scheduled_tasks(
            limit=tasks_limit
        )
        for task in tasks_persistence:
            if set_processing:
                await self.task_claim_history_dao.create(
                    task_id=task.id,
                    worker_id=worker_id,
                )

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


class ClaimTask:
    def __init__(
        self,
        parsing_task_dao: ParsingTaskDAO,
        task_claim_history_dao: TaskClaimHistoryDAO,
    ):
        self.parsing_task_dao: ParsingTaskDAO = parsing_task_dao
        self.task_claim_history_dao: TaskClaimHistoryDAO = task_claim_history_dao

    async def __call__(self, worker_id: str) -> ParsingTaskDTO | None:
        """Атомарно забирает одну задачу и записывает в историю.

        Args:
            worker_id: идентификатор воркера

        Returns:
            ParsingTaskDTO если задача найдена, иначе None
        """
        tasks = list(await self.parsing_task_dao.get_scheduled_tasks(limit=1))

        if len(tasks) == 0:
            return None
        now = datetime.now(timezone.utc)
        task = tasks[0]
        task.last_parsed_at = now
        await self.task_claim_history_dao.create(
            task_id=task.id,
            worker_id=worker_id,
        )

        next_run = calculate_next_run(
            task.bucket,
            task.last_parsed_at,
            task.created_at,
            task.status,
        )
        parsing_task_dto = ParsingTaskDTO.from_persistence(
            task, int(next_run.timestamp()) if next_run else None
        )
        await self.parsing_task_dao.commit()
        return parsing_task_dto


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
    claim_task: CompositeDependencySource = provide(
        ClaimTask,
        provides=ClaimTask,
        scope=Scope.REQUEST,
    )
