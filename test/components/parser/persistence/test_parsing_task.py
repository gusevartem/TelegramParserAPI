from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import (
    Channel,
    ChannelDAO,
    ParsingTaskDAO,
    ParsingTaskStatus,
)
from sqlalchemy.exc import IntegrityError


@pytest_asyncio.fixture
async def channel_dao(request_container: AsyncContainer) -> ChannelDAO:
    return await request_container.get(ChannelDAO)


@pytest_asyncio.fixture
async def parsing_task_dao(request_container: AsyncContainer) -> ParsingTaskDAO:
    return await request_container.get(ParsingTaskDAO)


@pytest_asyncio.fixture
async def test_channel(channel_dao: ChannelDAO) -> Channel:
    """Creates a basic channel for testing."""
    channel = await channel_dao.create(
        id=654321,
        name="Parsing Task Channel",
        description="Description",
    )
    await channel_dao.commit()
    return channel


@pytest.mark.asyncio
class TestParsingTaskCreateRead:
    """Tests basic CRUD operations for ParsingTask."""

    async def test_create_and_read_task(
        self,
        parsing_task_dao: ParsingTaskDAO,
        test_channel: Channel,
    ):
        # 1. Create a task
        task = await parsing_task_dao.create(
            url="https://t.me/test_parsing_task",
            bucket=30,
        )
        # Assign to channel
        task.channel_id = test_channel.id
        await parsing_task_dao.commit()

        assert task.id is not None
        assert task.url == "https://t.me/test_parsing_task"
        assert task.bucket == 30
        assert task.status == ParsingTaskStatus.IDLE
        assert task.channel_id == test_channel.id

        # 2. Read back by ID
        fetched = await parsing_task_dao.find_by_id(task.id)
        assert fetched is not None
        assert fetched.url == "https://t.me/test_parsing_task"

        # 3. Read back by URL
        fetched_url = await parsing_task_dao.find_by_url(
            "https://t.me/test_parsing_task"
        )
        assert fetched_url is not None
        assert fetched_url.id == task.id

        # 4. Read back by channel id
        tasks_by_channel = await parsing_task_dao.find_by_channel_id(test_channel.id)
        assert len(tasks_by_channel) == 1
        assert tasks_by_channel[0].id == task.id

        # 5. Read by unknown URL
        not_found = await parsing_task_dao.find_by_url("https://t.me/unknown_task")
        assert not_found is None


@pytest.mark.asyncio
class TestParsingTaskConstraints:
    """Tests the constraints on ParsingTask (bucket range, unique URL)."""

    async def test_unique_url_constraint(
        self,
        parsing_task_dao: ParsingTaskDAO,
    ):
        await parsing_task_dao.create(
            url="https://t.me/duplicate_url",
            bucket=10,
        )
        await parsing_task_dao.commit()

        with pytest.raises(IntegrityError):
            await parsing_task_dao.create(
                url="https://t.me/duplicate_url",
                bucket=20,
            )

    async def test_bucket_range_constraint_negative(
        self,
        parsing_task_dao: ParsingTaskDAO,
    ):
        # Negative bucket
        with pytest.raises(IntegrityError):
            await parsing_task_dao.create(
                url="https://t.me/negative_bucket",
                bucket=-1,
            )

    async def test_bucket_range_constraint_max(
        self,
        parsing_task_dao: ParsingTaskDAO,
    ):
        # Bucket >= 60
        with pytest.raises(IntegrityError):
            await parsing_task_dao.create(
                url="https://t.me/big_bucket",
                bucket=60,
            )


@pytest.mark.asyncio
class TestParsingTaskQueries:
    """Tests customized queries like get_buckets_loading and get_scheduled_tasks."""

    async def test_get_buckets_loading(
        self,
        parsing_task_dao: ParsingTaskDAO,
    ):
        # Create tasks in various states and buckets
        await parsing_task_dao.create(url="url1", bucket=5)

        t2 = await parsing_task_dao.create(url="url2", bucket=5)
        t2.status = ParsingTaskStatus.SKIP

        t3 = await parsing_task_dao.create(url="url3", bucket=5)
        t3.status = ParsingTaskStatus.ERROR

        await parsing_task_dao.create(url="url4", bucket=15)
        await parsing_task_dao.commit()

        buckets_loading = await parsing_task_dao.get_buckets_loading()
        # bucket 5 should have 2 (1 IDLE, 1 PROCESSING, ERROR is ignored)
        # bucket 15 should have 1 (IDLE)
        assert buckets_loading.get(5) == 2
        assert buckets_loading.get(15) == 1
        assert buckets_loading.get(10) is None

    async def test_get_scheduled_tasks(
        self,
        parsing_task_dao: ParsingTaskDAO,
    ):
        now_utc = datetime.now(timezone.utc)
        this_hour_start = now_utc.replace(minute=0, second=0, microsecond=0)

        # Current minute to ensure we can schedule properly
        # We test based on bucket <= current minute
        current_minute = now_utc.minute
        test_bucket = current_minute

        # Task 1: Never parsed, created in a previous hour -> SHOULD BE SCHEDULED
        t1 = await parsing_task_dao.create(url="task1_scheduled", bucket=test_bucket)

        await parsing_task_dao.commit()
        t1.created_at = this_hour_start - timedelta(hours=1)
        await parsing_task_dao.flush()

        # Task 2: Last parsed in a previous hour -> SHOULD BE SCHEDULED
        t2 = await parsing_task_dao.create(url="task2_scheduled", bucket=test_bucket)
        t2.last_parsed_at = this_hour_start - timedelta(hours=2)
        await parsing_task_dao.commit()

        t3 = await parsing_task_dao.create(
            url="task3_not_scheduled", bucket=test_bucket
        )
        t3.status = ParsingTaskStatus.SKIP
        t3.created_at = this_hour_start - timedelta(hours=1)
        await parsing_task_dao.commit()

        await parsing_task_dao.create(url="task4_not_scheduled", bucket=test_bucket)
        # created_at is automatically roughly now
        await parsing_task_dao.commit()

        # Task 5: last_parsed_at this hour -> SHOULD NOT BE SCHEDULED
        t5 = await parsing_task_dao.create(
            url="task5_not_scheduled", bucket=test_bucket
        )
        t5.last_parsed_at = now_utc
        await parsing_task_dao.commit()

        # Task 6: Scheduled but bucket is > current_minute (if minute isn't 59)
        # -> SHOULD NOT BE SCHEDULED
        if current_minute < 59:
            t6 = await parsing_task_dao.create(
                url="task6_not_scheduled", bucket=current_minute + 1
            )
            await parsing_task_dao.commit()
            t6.created_at = this_hour_start - timedelta(hours=1)
            await parsing_task_dao.flush()

        tasks = await parsing_task_dao.get_scheduled_tasks()
        scheduled_urls = [t.url for t in tasks]

        assert "task1_scheduled" in scheduled_urls
        assert "task2_scheduled" in scheduled_urls
        assert "task3_not_scheduled" not in scheduled_urls
        assert "task4_not_scheduled" not in scheduled_urls
        assert "task5_not_scheduled" not in scheduled_urls
        assert "task6_not_scheduled" not in scheduled_urls
