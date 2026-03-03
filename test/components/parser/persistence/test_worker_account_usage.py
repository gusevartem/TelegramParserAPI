import random
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import TelegramClient, TelegramClientDAO, WorkerAccountUsageDAO


@pytest_asyncio.fixture
async def telegram_client_dao(request_container: AsyncContainer) -> TelegramClientDAO:
    return await request_container.get(TelegramClientDAO)


@pytest_asyncio.fixture
async def worker_usage_dao(request_container: AsyncContainer) -> WorkerAccountUsageDAO:
    return await request_container.get(WorkerAccountUsageDAO)


@pytest_asyncio.fixture
async def test_telegram_client(
    telegram_client_dao: TelegramClientDAO,
) -> TelegramClient:
    """Creates a basic TelegramClient for testing."""
    client = await telegram_client_dao.create(
        telegram_id=random.randint(1000000, 99999999),
        phone=f"+{random.randint(1000000000, 9999999999)}",
        api_id=12345,
        api_hash="testhash",
        device_model="test_device",
        system_version="test_system",
        app_version="test_app",
        lang_code="en",
        system_lang_code="en",
        session_string="test_session_string",
    )
    await telegram_client_dao.commit()
    return client


@pytest.mark.asyncio
class TestWorkerAccountUsageCreateRead:
    """Tests basic CRUD operations, isolated in its own class scope DB."""

    async def test_create_and_read_usage(
        self,
        worker_usage_dao: WorkerAccountUsageDAO,
        test_telegram_client: TelegramClient,
    ):
        # 1. Create worker usage
        locked_until_dt = datetime.now(timezone.utc) + timedelta(hours=1)

        usage = await worker_usage_dao.create(
            telegram_id=test_telegram_client.telegram_id,
            worker_id="worker_alpha",
            locked_until=locked_until_dt,
            reason="parsing_job",
        )
        await worker_usage_dao.commit()
        await worker_usage_dao.refresh(usage)

        assert usage.id is not None
        assert usage.telegram_id == test_telegram_client.telegram_id
        assert usage.worker_id == "worker_alpha"
        assert usage.reason == "parsing_job"
        assert usage.locked_until.replace(tzinfo=timezone.utc) == locked_until_dt
        assert usage.created_at is not None

        # 2. Read back
        fetched = await worker_usage_dao.find_by_id(usage.id)
        assert fetched is not None
        assert fetched.telegram_id == test_telegram_client.telegram_id
        assert fetched.worker_id == "worker_alpha"
        assert fetched.reason == "parsing_job"
        assert fetched.locked_until.replace(tzinfo=timezone.utc) == locked_until_dt
        assert fetched.created_at == usage.created_at

    async def test_exists_by_id(
        self,
        worker_usage_dao: WorkerAccountUsageDAO,
        test_telegram_client: TelegramClient,
    ):
        usage = await worker_usage_dao.create(
            telegram_id=test_telegram_client.telegram_id,
            worker_id="worker_beta",
            locked_until=datetime.now(timezone.utc) + timedelta(hours=2),
            reason="maintenance",
        )
        await worker_usage_dao.commit()

        exists_after = await worker_usage_dao.exists_by_id(usage.id)
        assert exists_after is True


@pytest.mark.asyncio
class TestWorkerAccountUsageList:
    """Tests the listing and bulk reading functionalities."""

    async def test_list_usages(
        self,
        worker_usage_dao: WorkerAccountUsageDAO,
        test_telegram_client: TelegramClient,
    ):
        # Create multiple usages
        for i in range(5):
            await worker_usage_dao.create(
                telegram_id=test_telegram_client.telegram_id,
                worker_id=f"worker_list_{i}",
                locked_until=datetime.now(timezone.utc) + timedelta(hours=i),
                reason=f"task_{i}",
            )
        await worker_usage_dao.commit()

        # List them
        usages = await worker_usage_dao.list()
        assert len(usages) == 5

        # Test offset and limit
        limited_usages = await worker_usage_dao.list(skip=2, limit=2)
        assert len(limited_usages) == 2
