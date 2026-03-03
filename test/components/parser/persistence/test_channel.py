import random
import uuid

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import ChannelDAO, MediaDAO


@pytest_asyncio.fixture
async def channel_dao(request_container: AsyncContainer) -> ChannelDAO:
    return await request_container.get(ChannelDAO)


@pytest_asyncio.fixture
async def media_dao(request_container: AsyncContainer) -> MediaDAO:
    return await request_container.get(MediaDAO)


@pytest.mark.asyncio
class TestChannelCreateRead:
    """Tests basic CRUD operations for Channel, isolated in its own class scope DB."""

    async def test_create_and_read_channel(
        self,
        channel_dao: ChannelDAO,
    ):
        channel_id = random.randint(10000, 9999999)
        # 1. Create a channel
        channel = await channel_dao.create(
            id=channel_id,
            name="Test Channel",
            description="Test Description",
        )
        await channel_dao.commit()

        assert channel.id == channel_id
        assert channel.name == "Test Channel"
        assert channel.description == "Test Description"
        assert channel.logo_id is None

        # 2. Read back the channel
        fetched = await channel_dao.find_by_id(channel.id)
        assert fetched is not None
        assert fetched.id == channel_id
        assert fetched.name == "Test Channel"
        assert fetched.description == "Test Description"

    async def test_create_and_read_channel_with_logo(
        self,
        channel_dao: ChannelDAO,
        media_dao: MediaDAO,
    ):
        channel_id = random.randint(10000, 9999999)
        # Create media
        media_id = uuid.uuid4()
        media = await media_dao.create(
            id=media_id,
            mime_type="image/jpeg",
            size_bytes=1024,
            file_name="test.jpg",
        )
        await media_dao.commit()

        # Create channel with logo
        channel = await channel_dao.create(
            id=channel_id,
            name="Channel With Logo",
            logo=media,
        )
        await channel_dao.commit()

        assert channel.id == channel_id
        assert channel.logo_id == media_id

        # Read back with loaded logo
        fetched = await channel_dao.find_by_id_with_loaded_logo(channel.id)
        assert fetched is not None
        assert fetched.id == channel_id
        assert fetched.logo is not None
        assert fetched.logo.id == media_id
        assert fetched.logo.mime_type == "image/jpeg"

    async def test_find_by_id_with_loaded_logo_without_logo(
        self, channel_dao: ChannelDAO
    ):
        channel_id = random.randint(10000, 9999999)
        await channel_dao.create(id=channel_id, name="No Logo")
        await channel_dao.commit()

        fetched = await channel_dao.find_by_id_with_loaded_logo(channel_id)
        assert fetched is not None
        assert fetched.logo is None

    async def test_exists_by_id(self, channel_dao: ChannelDAO):
        channel_id = random.randint(10000, 9999999)
        await channel_dao.create(id=channel_id, name="Test Exists")
        await channel_dao.commit()

        assert await channel_dao.exists_by_id(channel_id) is True
        assert await channel_dao.exists_by_id(channel_id + 1) is False

    async def test_channel_timestamps_and_refresh(self, channel_dao: ChannelDAO):
        channel_id = random.randint(10000, 9999999)
        channel = await channel_dao.create(id=channel_id, name="Timestamps Test")
        await channel_dao.commit()

        await channel_dao.refresh(channel)

        assert channel.recorded_at is not None
        assert channel.updated_at is not None

    async def test_get_ids(self, channel_dao: ChannelDAO):
        id1 = random.randint(10000, 9999999)
        id2 = random.randint(10000, 9999999)
        await channel_dao.create(id=id1, name="Channel 1")
        await channel_dao.create(id=id2, name="Channel 2")
        await channel_dao.commit()

        ids = await channel_dao.get_ids()
        assert id1 in ids
        assert id2 in ids


@pytest.mark.asyncio
class TestChannelUpdate:
    """Tests update operation for Channel."""

    async def test_update_channel(
        self,
        channel_dao: ChannelDAO,
    ):
        channel_id = random.randint(10000, 9999999)
        channel = await channel_dao.create(
            id=channel_id,
            name="Original Name",
        )
        await channel_dao.commit()

        channel.name = "Updated Name"
        channel.description = "New Description"
        await channel_dao.commit()

        fetched = await channel_dao.find_by_id(channel_id)
        assert fetched is not None
        assert fetched.name == "Updated Name"
        assert fetched.description == "New Description"


@pytest.mark.asyncio
class TestChannelList:
    """Tests the listing and bulk reading functionalities."""

    async def test_list_channels(
        self,
        channel_dao: ChannelDAO,
    ):
        base_id = random.randint(100000, 9999999)
        # Create multiple channels
        for i in range(5):
            await channel_dao.create(
                id=base_id + i,
                name=f"Channel {i}",
            )
        await channel_dao.commit()

        # List them
        channels = await channel_dao.list()
        assert len(channels) >= 5

        # Test offset and limit
        limited_channels = await channel_dao.list(skip=2, limit=2)
        assert len(limited_channels) == 2
