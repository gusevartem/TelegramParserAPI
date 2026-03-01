import random
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import (
    TelegramClientDAO,
    WorkerAccountUsageDAO,
)


@pytest_asyncio.fixture
async def client_dao(request_container: AsyncContainer) -> TelegramClientDAO:
    return await request_container.get(TelegramClientDAO)


@pytest_asyncio.fixture
async def usage_dao(request_container: AsyncContainer) -> WorkerAccountUsageDAO:
    return await request_container.get(WorkerAccountUsageDAO)


@pytest.fixture
def client_data() -> dict[str, Any]:
    return {
        "telegram_id": random.randint(1000000, 9999999),
        "phone": f"+{random.randint(1000000000, 9999999999)}",
        "api_id": 12345,
        "api_hash": "testhash",
        "device_model": "test_device",
        "system_version": "test_system",
        "app_version": "test_app",
        "lang_code": "en",
        "system_lang_code": "en",
        "session_string": "test_session_string",
    }


@pytest.mark.asyncio
class TestTelegramClientCreateRead:
    """Tests basic CRUD operations for TelegramClient."""

    async def test_create_and_read_client(
        self,
        client_dao: TelegramClientDAO,
        client_data: dict[str, Any],
    ):
        # 1. Create a client
        client = await client_dao.create(**client_data)
        await client_dao.commit()

        assert client.telegram_id == client_data["telegram_id"]
        assert getattr(client, "session_string", None) == client_data["session_string"]

        # 2. Read back
        fetched = await client_dao.find_by_id(client_data["telegram_id"])
        assert fetched is not None
        assert fetched.phone == client_data["phone"]

    async def test_exists_by_id(
        self,
        client_dao: TelegramClientDAO,
        client_data: dict[str, Any],
    ):
        await client_dao.create(**client_data)
        await client_dao.commit()

        assert await client_dao.exists_by_id(client_data["telegram_id"]) is True
        assert await client_dao.exists_by_id(999999999) is False


@pytest.mark.asyncio
class TestTelegramClientUpdate:
    """Tests update operations for TelegramClient."""

    async def test_update_client(
        self,
        client_dao: TelegramClientDAO,
        client_data: dict[str, Any],
    ):
        client = await client_dao.create(**client_data)
        await client_dao.commit()

        client.phone = "+0987654321"
        client.banned = True
        await client_dao.commit()

        fetched = await client_dao.find_by_id(client_data["telegram_id"])
        assert fetched is not None
        assert fetched.phone == "+0987654321"
        assert fetched.banned is True


@pytest.mark.asyncio
class TestTelegramClientList:
    """Tests listing and bulk reading functionalities."""

    async def test_empty_clients(self, client_dao: TelegramClientDAO):
        clients = await client_dao.list()
        assert len(clients) == 0

    async def test_list_clients(
        self, client_dao: TelegramClientDAO, client_data: dict[str, Any]
    ):
        for i in range(5):
            data = client_data.copy()
            data["telegram_id"] += i + 1  # prevent duplicate PK
            data["phone"] = f"+12345678{i}"
            await client_dao.create(**data)
        await client_dao.commit()

        clients = await client_dao.list()
        assert len(clients) == 5

        limited_clients = await client_dao.list(skip=2, limit=2)
        assert len(limited_clients) == 2


@pytest.mark.asyncio
class TestTelegramClientSpecial:
    """Tests special DAO methods for TelegramClient."""

    async def test_is_working_clients_exists(
        self,
        client_dao: TelegramClientDAO,
        client_data: dict[str, Any],
    ):
        # Initial check
        assert await client_dao.is_working_clients_exists() is False

        # Add a working client
        await client_dao.create(**client_data)
        await client_dao.commit()
        assert await client_dao.is_working_clients_exists() is True

        # Add a banned client
        data_banned = client_data.copy()
        data_banned["telegram_id"] = 999
        data_banned["phone"] = "+111"
        client_banned = await client_dao.create(**data_banned)
        client_banned.banned = True
        await client_dao.commit()

        # Add a client without session string
        data_no_session = client_data.copy()
        data_no_session["telegram_id"] = 888
        data_no_session["phone"] = "+222"
        data_no_session["session_string"] = None
        await client_dao.create(**data_no_session)
        await client_dao.commit()

        # Should still be True because of the first client
        assert await client_dao.is_working_clients_exists() is True

    async def test_find_with_proxy(
        self,
        client_dao: TelegramClientDAO,
        client_data: dict[str, Any],
    ):
        await client_dao.create(**client_data)
        await client_dao.commit()

        client = await client_dao.find_with_proxy(client_data["telegram_id"])
        assert client is not None
        assert client.proxy is None


@pytest.mark.asyncio
class TestTelegramClientFindAvailable:
    """Tests find_available_account in isolation."""

    async def test_find_available_account(
        self,
        client_dao: TelegramClientDAO,
        usage_dao: WorkerAccountUsageDAO,
        client_data: dict[str, Any],
    ):
        import asyncio

        # 1. Create a client
        await client_dao.create(**client_data)
        await client_dao.commit()

        # It should be available initially
        available = await client_dao.find_available_account()
        assert available is not None
        assert available.telegram_id == client_data["telegram_id"]

        # 2. Add usage record
        await usage_dao.create(
            telegram_id=client_data["telegram_id"],
            worker_id="worker_1",
            locked_until=datetime.now(timezone.utc) + timedelta(hours=20),
        )
        await usage_dao.commit()

        # 3. Try to find an available account - should return None
        available = await client_dao.find_available_account()
        assert available is None

        # Sleep to ensure SQLite server_default=func.now() provides a distinct timestamp
        await asyncio.sleep(1.1)

        # 4. Add an expired usage record
        await usage_dao.create(
            telegram_id=client_data["telegram_id"],
            worker_id="worker_1",
            locked_until=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        await usage_dao.commit()

        # 5. Try to find an available account - should return the client
        available = await client_dao.find_available_account()
        assert available is not None
        assert available.telegram_id == client_data["telegram_id"]
