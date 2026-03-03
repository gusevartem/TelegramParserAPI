from datetime import datetime, timezone

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import (
    Channel,
    ChannelDAO,
    ChannelMessage,
    ChannelMessageDAO,
    ChannelMessageStatisticDAO,
)


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
async def test_channel(channel_dao: ChannelDAO) -> Channel:
    """Creates a basic channel for testing."""
    channel = await channel_dao.create(
        id=12345,
        name="Test Channel",
        description="Test Description",
    )
    await channel_dao.commit()
    return channel


@pytest_asyncio.fixture
async def test_message(
    channel_message_dao: ChannelMessageDAO, test_channel: Channel
) -> ChannelMessage:
    """Creates a basic message linked to the test_channel."""
    message = await channel_message_dao.create(
        channel=test_channel,
        channel_message_id=999,
        created_at=datetime.now(timezone.utc),
        text="Hello World!",
    )
    await channel_message_dao.commit()
    return message


@pytest.mark.asyncio
class TestChannelMessageStatisticCreateRead:
    """Tests basic CRUD operations, isolated in its own class scope DB."""

    async def test_create_and_read_statistic(
        self,
        statistic_dao: ChannelMessageStatisticDAO,
        test_message: ChannelMessage,
    ):
        # 1. Create a statistic for the message
        statistic = await statistic_dao.create(
            message=test_message,
            views=1000,
        )
        await statistic_dao.commit()

        assert statistic.id is not None
        assert statistic.views == 1000
        assert statistic.channel_message_id == test_message.id

        # 2. Read back the statistic
        fetched = await statistic_dao.find_by_id(statistic.id)
        assert fetched is not None
        assert fetched.views == 1000
        assert fetched.channel_message_id == test_message.id


@pytest.mark.asyncio
class TestChannelMessageStatisticImmutability:
    """Tests the immutability constraint of statistic records."""

    async def test_statistic_immutability(
        self,
        statistic_dao: ChannelMessageStatisticDAO,
        test_message: ChannelMessage,
    ):
        statistic = await statistic_dao.create(
            message=test_message,
            views=500,
        )
        await statistic_dao.commit()

        # Try to modify
        statistic.views = 600
        with pytest.raises(
            Exception, match="ChannelMessageStatistic records are immutable"
        ):
            await statistic_dao.commit()


@pytest.mark.asyncio
class TestChannelMessageStatisticList:
    """Tests the listing and bulk reading functionalities."""

    async def test_list_statistics(
        self,
        statistic_dao: ChannelMessageStatisticDAO,
        test_message: ChannelMessage,
    ):
        # Create multiple statistics
        for i in range(5):
            await statistic_dao.create(
                message=test_message,
                views=100 * (i + 1),
            )
        await statistic_dao.commit()

        # List them
        stats = await statistic_dao.list()
        assert len(stats) == 5
        assert all(s.channel_message_id == test_message.id for s in stats)

        # Test offset and limit
        limited_stats = await statistic_dao.list(skip=2, limit=2)
        assert len(limited_stats) == 2
        assert limited_stats[0].views == 300
        assert limited_stats[1].views == 400
