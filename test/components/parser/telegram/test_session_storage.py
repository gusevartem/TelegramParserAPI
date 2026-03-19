import asyncio
import random
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import TelegramClient, TelegramClientDAO, WorkerAccountUsageDAO
from parser.telegram import FloodWait
from parser.telegram.session_storage import ITelegramSessionStorage
from telethon.crypto import AuthKey
from telethon.sessions.string import StringSession


@pytest_asyncio.fixture
async def session_storage(request_container: AsyncContainer) -> ITelegramSessionStorage:
    return await request_container.get(ITelegramSessionStorage)


@pytest_asyncio.fixture
async def telegram_client_dao(request_container: AsyncContainer) -> TelegramClientDAO:
    return await request_container.get(TelegramClientDAO)


@pytest_asyncio.fixture
async def usage_dao(request_container: AsyncContainer) -> WorkerAccountUsageDAO:
    return await request_container.get(WorkerAccountUsageDAO)


@pytest.fixture(scope="class")
def telegram_id() -> int:
    return random.randint(1000000, 9999999)


@pytest_asyncio.fixture
async def telegram_client(
    telegram_client_dao: TelegramClientDAO, telegram_id: int
) -> TelegramClient:
    session = StringSession()
    session.set_dc(0, "127.0.0.1", 80)
    session.auth_key = AuthKey(b"0" * 256)

    return await telegram_client_dao.create(
        telegram_id=telegram_id,
        phone=f"+{random.randint(1000000000, 9999999999)}",
        api_id=12345,
        api_hash="testhash",
        device_model="test_device",
        system_version="test_system",
        app_version="test_app",
        lang_code="en",
        system_lang_code="en",
        session_string=session.save(),
    )


@pytest.mark.asyncio
class TestPostgreSQLSessionStorageBase:
    async def test_add_session(
        self,
        session_storage: ITelegramSessionStorage,
        telegram_client_dao: TelegramClientDAO,
        telegram_client: TelegramClient,
    ):
        session_string = telegram_client.session_string
        if session_string is None:
            raise ValueError("Client has no session string")

        await session_storage.add_session(telegram_client.telegram_id, session_string)
        await telegram_client_dao.commit()
        client = await telegram_client_dao.find_by_id(telegram_client.telegram_id)
        assert client is not None
        assert client.session_string == session_string

    async def test_get_session_success(
        self,
        session_storage: ITelegramSessionStorage,
        usage_dao: WorkerAccountUsageDAO,
        telegram_client_dao: TelegramClientDAO,
        telegram_id: int,
    ):
        telegram_client = await telegram_client_dao.find_by_id(telegram_id)
        if telegram_client is None:
            raise ValueError("Client not found")
        session_string = telegram_client.session_string
        if session_string is None:
            raise ValueError("Client has no session string")

        async with session_storage.get_session() as telegram_session:
            assert telegram_session.user_id == telegram_client.telegram_id
            assert telegram_session.session.save() == session_string

            usages = await usage_dao.list()
            assert len(usages) == 1
            assert usages[0].telegram_id == telegram_client.telegram_id

    async def test_get_session_timeout(
        self,
        session_storage: ITelegramSessionStorage,
    ):
        with pytest.raises(
            TimeoutError, match="Cannot get session from mysql database"
        ):
            async with session_storage.get_session(timeout=1):
                pass


@pytest.mark.asyncio
class TestPostgreSQLSessionFloodWait:
    async def test_get_session_flood_wait(
        self,
        session_storage: ITelegramSessionStorage,
        telegram_client_dao: TelegramClientDAO,
        usage_dao: WorkerAccountUsageDAO,
        telegram_client: TelegramClient,
    ):
        session_string = telegram_client.session_string
        if session_string is None:
            raise ValueError("Client has no session string")

        await session_storage.add_session(telegram_client.telegram_id, session_string)
        await telegram_client_dao.commit()

        with pytest.raises(FloodWait):
            async with session_storage.get_session() as _:
                await asyncio.sleep(1.1)
                raise FloodWait(seconds=1, message="Flood wait mock")

        telegram_client = await telegram_client_dao.refresh(telegram_client)
        usages = await usage_dao.list()
        assert len(usages) == 2
        flood_wait_record = next(u for u in usages if u.reason == "flood_wait")

        now = datetime.now(timezone.utc)
        expected_locked_until = now + timedelta(seconds=1)
        assert (
            expected_locked_until
            < flood_wait_record.locked_until.replace(tzinfo=timezone.utc)
            < expected_locked_until + timedelta(seconds=20)
        )

    async def test_get_session_with_wait_for_flood_wait(
        self,
        session_storage: ITelegramSessionStorage,
        telegram_client_dao: TelegramClientDAO,
        usage_dao: WorkerAccountUsageDAO,
        telegram_id: int,
    ):
        telegram_client = await telegram_client_dao.find_by_id(telegram_id)
        if telegram_client is None:
            raise ValueError("Client not found")
        print("Telegram Client")
        print(telegram_client.telegram_id)
        print("Usages")
        usages = await usage_dao.list()
        for usage in usages:
            print(usage)
            print(usage.telegram_id)
            print(usage.locked_until)
            print(usage.reason)
            print(usage.created_at)
            print("-----")
        async with session_storage.get_session(timeout=20) as session:
            assert session.user_id == telegram_client.telegram_id
            assert session.session.save() == telegram_client.session_string
