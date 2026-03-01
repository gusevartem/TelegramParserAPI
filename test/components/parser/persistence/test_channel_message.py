from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import (
    Channel,
    ChannelDAO,
    ChannelMessageDAO,
    ChannelMessageStatisticDAO,
    MediaDAO,
)
from sqlalchemy.exc import IntegrityError


@pytest_asyncio.fixture
async def channel_dao(request_container: AsyncContainer) -> ChannelDAO:
    return await request_container.get(ChannelDAO)


@pytest_asyncio.fixture
async def channel_message_dao(request_container: AsyncContainer) -> ChannelMessageDAO:
    return await request_container.get(ChannelMessageDAO)


@pytest_asyncio.fixture
async def statistic_dao(
    request_container: AsyncContainer,
) -> ChannelMessageStatisticDAO:
    return await request_container.get(ChannelMessageStatisticDAO)


@pytest_asyncio.fixture
async def media_dao(request_container: AsyncContainer) -> MediaDAO:
    return await request_container.get(MediaDAO)


@pytest_asyncio.fixture
async def test_channel(channel_dao: ChannelDAO) -> Channel:
    """Creates a basic channel for testing."""
    channel = await channel_dao.create(
        id=54321,
        name="Test Channel for Messages",
        description="Test Description",
    )
    await channel_dao.commit()
    return channel


@pytest.mark.asyncio
class TestChannelMessageCreateRead:
    """Tests basic CRUD operations."""

    async def test_create_and_read_message(
        self,
        channel_message_dao: ChannelMessageDAO,
        test_channel: Channel,
    ):
        # 1. Create a message
        created_dt = datetime.now(timezone.utc)
        message = await channel_message_dao.create(
            channel=test_channel,
            channel_message_id=100,
            created_at=created_dt,
            text="First Test Message",
        )
        await channel_message_dao.commit()

        assert message.id is not None
        assert message.channel_message_id == 100
        assert message.channel_id == test_channel.id
        assert message.text == "First Test Message"

        # 2. Read back the message by ID
        fetched = await channel_message_dao.find_by_id(message.id)
        assert fetched is not None
        assert fetched.channel_message_id == 100
        assert fetched.text == "First Test Message"

        # 3. Read back by channel_id and message_id
        fetched_specific = await channel_message_dao.find_by_channel_id_and_message_id(
            channel_id=test_channel.id, message_id=100
        )
        assert fetched_specific is not None
        assert fetched_specific.id == message.id


@pytest.mark.asyncio
class TestChannelMessageConstraints:
    """Tests the unique constraints of channel message records."""

    async def test_unique_channel_message_constraint(
        self,
        channel_message_dao: ChannelMessageDAO,
        test_channel: Channel,
    ):
        created_dt = datetime.now(timezone.utc)
        # Create first message
        await channel_message_dao.create(
            channel=test_channel,
            channel_message_id=200,
            created_at=created_dt,
            text="First duplicate",
        )
        await channel_message_dao.commit()

        # Try to create a second message with the same channel_id and channel_message_id
        with pytest.raises(IntegrityError):
            await channel_message_dao.create(
                channel=test_channel,
                channel_message_id=200,
                created_at=created_dt,
                text="Second duplicate",
            )


@pytest.mark.asyncio
class TestChannelMessageQueries:
    """Tests the querying and bulk reading functionalities."""

    async def test_get_channel_messages(
        self,
        channel_message_dao: ChannelMessageDAO,
        test_channel: Channel,
    ):
        # Create multiple messages
        for i in range(5):
            await channel_message_dao.create(
                channel=test_channel,
                channel_message_id=300 + i,
                created_at=datetime(2025, 1, i + 1, tzinfo=timezone.utc),
                text=f"Message {i}",
            )
        await channel_message_dao.commit()

        # Get newest first (skip=0, limit=None)
        messages_newest = await channel_message_dao.get_channel_messages(
            channel_id=test_channel.id,
            sorting="newest",
            skip=0,
            limit=None,
        )
        assert len(messages_newest) == 5
        assert messages_newest[0].channel_message_id == 304  # Newest first
        assert messages_newest[-1].channel_message_id == 300

        # Get oldest first (skip=0, limit=None)
        messages_oldest = await channel_message_dao.get_channel_messages(
            channel_id=test_channel.id,
            sorting="oldest",
            skip=0,
            limit=None,
        )
        assert len(messages_oldest) == 5
        assert messages_oldest[0].channel_message_id == 300  # Oldest first
        assert messages_oldest[-1].channel_message_id == 304

        # Test skip and limit
        limited_messages = await channel_message_dao.get_channel_messages(
            channel_id=test_channel.id,
            sorting="oldest",
            skip=2,
            limit=2,
        )
        assert len(limited_messages) == 2
        assert limited_messages[0].channel_message_id == 302
        assert limited_messages[1].channel_message_id == 303


@pytest.mark.asyncio
class TestChannelMessageRelationships:
    """Tests for eagerly loading relationships."""

    async def test_find_with_loaded_statistics_and_media(
        self,
        channel_message_dao: ChannelMessageDAO,
        statistic_dao: ChannelMessageStatisticDAO,
        media_dao: MediaDAO,
        test_channel: Channel,
    ):
        # Create message
        message = await channel_message_dao.create(
            channel=test_channel,
            channel_message_id=500,
            created_at=datetime.now(timezone.utc),
            text="Message with relations",
        )
        await message.awaitable_attrs.media_links

        # Create statistic attached to message
        await statistic_dao.create(
            message=message,
            views=1000,
        )

        # Create media
        media_id = uuid4()
        media_item = await media_dao.create(
            id=media_id,
            mime_type="image/jpeg",
            size_bytes=1024,
            file_name="photo.jpg",
        )
        message.media.append(media_item)
        await channel_message_dao.commit()

        # Now fetch with eager loading
        loaded_message = (
            await channel_message_dao.find_with_loaded_statistics_and_media(message.id)
        )
        assert loaded_message is not None

        # Verify eager-loaded properties
        assert len(loaded_message.statistics) == 1
        assert loaded_message.statistics[0].views == 1000

        assert len(loaded_message.media_links) == 1
        assert loaded_message.media_links[0].media_item is not None
        assert loaded_message.media_links[0].media_item.id == media_id
