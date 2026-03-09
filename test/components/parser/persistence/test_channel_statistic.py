import asyncio
import random
import uuid

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from parser.persistence import (
    Channel,
    ChannelDAO,
    ChannelStatisticDAO,
)


@pytest_asyncio.fixture
async def channel_dao(request_container: AsyncContainer) -> ChannelDAO:
    return await request_container.get(ChannelDAO)


@pytest_asyncio.fixture
async def statistic_dao(
    request_container: AsyncContainer,
) -> ChannelStatisticDAO:
    return await request_container.get(ChannelStatisticDAO)


@pytest_asyncio.fixture
async def test_channel(channel_dao: ChannelDAO) -> Channel:
    """Creates a basic channel for testing."""
    channel = await channel_dao.create(
        id=random.randint(10000, 9999999),
        name="Test Channel",
        description="Test Description",
    )
    await channel_dao.commit()
    return channel


@pytest.mark.asyncio
class TestChannelStatisticCreateRead:
    """Tests basic CRUD operations, isolated in its own class scope DB."""

    async def test_create_and_read_statistic(
        self,
        statistic_dao: ChannelStatisticDAO,
        test_channel: Channel,
    ):
        # 1. Create a statistic for the channel
        statistic = await statistic_dao.create(
            channel=test_channel,
            subscribers_count=1000,
            views=500,
            posts_count=100,
            views_24h=50,
            views_48h=100,
            views_72h=150,
            views_96h=200,
            views_120h=250,
            views_144h=300,
            views_168h=350,
            posts_count_24h=10,
            posts_count_48h=20,
            posts_count_72h=30,
            posts_count_96h=40,
            posts_count_120h=50,
            posts_count_144h=60,
            posts_count_168h=70,
        )
        await statistic_dao.commit()

        assert statistic.id is not None
        assert statistic.subscribers_count == 1000
        assert statistic.views == 500
        assert statistic.posts_count == 100
        assert statistic.channel_id == test_channel.id

        # 2. Read back the statistic
        fetched = await statistic_dao.find_by_id(statistic.id)
        assert fetched is not None
        assert fetched.subscribers_count == 1000
        assert fetched.views == 500
        assert fetched.posts_count == 100
        assert fetched.channel_id == test_channel.id

        # 3. Check exists_by_id
        assert await statistic_dao.exists_by_id(statistic.id) is True

        # 4. Check nonexistent exists_by_id
        assert await statistic_dao.exists_by_id(uuid.uuid4()) is False


@pytest.mark.asyncio
class TestChannelStatisticImmutability:
    """Tests the immutability constraint of statistic records."""

    async def test_statistic_immutability(
        self,
        statistic_dao: ChannelStatisticDAO,
        test_channel: Channel,
    ):
        statistic = await statistic_dao.create(
            channel=test_channel,
            subscribers_count=500,
            views=250,
            posts_count=50,
            views_24h=50,
            views_48h=100,
            views_72h=150,
            views_96h=200,
            views_120h=250,
            views_144h=300,
            views_168h=350,
            posts_count_24h=10,
            posts_count_48h=20,
            posts_count_72h=30,
            posts_count_96h=40,
            posts_count_120h=50,
            posts_count_144h=60,
            posts_count_168h=70,
        )
        await statistic_dao.commit()

        # Try to modify
        statistic.views = 600
        with pytest.raises(Exception, match="ChannelStatistic records are immutable"):
            await statistic_dao.commit()


@pytest.mark.asyncio
class TestChannelStatisticList:
    """Tests the listing and bulk reading functionalities."""

    async def test_empty_statistics(
        self,
        statistic_dao: ChannelStatisticDAO,
        test_channel: Channel,
    ):
        latest = await statistic_dao.get_latest_by_channel_id(test_channel.id)
        assert latest is None

        stats = await statistic_dao.get_channel_statistics(
            channel_id=test_channel.id, sorting="newest", skip=0, limit=None
        )
        assert len(stats) == 0

    async def test_base_list_statistics(
        self,
        statistic_dao: ChannelStatisticDAO,
        test_channel: Channel,
    ):
        for i in range(5):
            await statistic_dao.create(
                channel=test_channel,
                subscribers_count=100 + i,
                views=50 + i,
                posts_count=10 + i,
                views_24h=50,
                views_48h=100,
                views_72h=150,
                views_96h=200,
                views_120h=250,
                views_144h=300,
                views_168h=350,
                posts_count_24h=10,
                posts_count_48h=20,
                posts_count_72h=30,
                posts_count_96h=40,
                posts_count_120h=50,
                posts_count_144h=60,
                posts_count_168h=70,
            )
        await statistic_dao.commit()

        all_stats = await statistic_dao.list()
        assert len(all_stats) == 5

        limited_stats = await statistic_dao.list(skip=2, limit=2)
        assert len(limited_stats) == 2

    async def test_get_latest_by_channel_id(
        self,
        statistic_dao: ChannelStatisticDAO,
        test_channel: Channel,
    ):
        # Create multiple statistics with a delay to ensure distinct sqlite timestamps
        for i in range(3):
            if i > 0:
                await asyncio.sleep(1.1)
            await statistic_dao.create(
                channel=test_channel,
                subscribers_count=1000 + i,
                views=500 + i,
                posts_count=100 + i,
                views_24h=50,
                views_48h=100,
                views_72h=150,
                views_96h=200,
                views_120h=250,
                views_144h=300,
                views_168h=350,
                posts_count_24h=10,
                posts_count_48h=20,
                posts_count_72h=30,
                posts_count_96h=40,
                posts_count_120h=50,
                posts_count_144h=60,
                posts_count_168h=70,
            )
            await statistic_dao.commit()

        # Get latest
        latest = await statistic_dao.get_latest_by_channel_id(test_channel.id)
        assert latest is not None
        assert latest.subscribers_count == 1002
        assert latest.views == 502
        assert latest.posts_count == 102

    async def test_get_channel_statistics_newest(
        self,
        statistic_dao: ChannelStatisticDAO,
        test_channel: Channel,
    ):
        for i in range(3):
            if i > 0:
                await asyncio.sleep(1.1)
            await statistic_dao.create(
                channel=test_channel,
                subscribers_count=100 + i,
                views=50 + i,
                posts_count=10 + i,
                views_24h=50,
                views_48h=100,
                views_72h=150,
                views_96h=200,
                views_120h=250,
                views_144h=300,
                views_168h=350,
                posts_count_24h=10,
                posts_count_48h=20,
                posts_count_72h=30,
                posts_count_96h=40,
                posts_count_120h=50,
                posts_count_144h=60,
                posts_count_168h=70,
            )
            await statistic_dao.commit()

        stats = await statistic_dao.get_channel_statistics(
            channel_id=test_channel.id, sorting="newest", skip=0, limit=None
        )
        assert len(stats) == 3
        # Newest first means the last inserted (index 2) should be first
        assert stats[0].subscribers_count == 102
        assert stats[2].subscribers_count == 100

        # Test offset and limit
        limited_stats = await statistic_dao.get_channel_statistics(
            channel_id=test_channel.id, sorting="newest", skip=1, limit=1
        )
        assert len(limited_stats) == 1
        assert limited_stats[0].subscribers_count == 101

    async def test_get_channel_statistics_oldest(
        self,
        statistic_dao: ChannelStatisticDAO,
        test_channel: Channel,
    ):
        for i in range(3):
            if i > 0:
                await asyncio.sleep(1.1)
            await statistic_dao.create(
                channel=test_channel,
                subscribers_count=100 + i,
                views=50 + i,
                posts_count=10 + i,
                views_24h=50,
                views_48h=100,
                views_72h=150,
                views_96h=200,
                views_120h=250,
                views_144h=300,
                views_168h=350,
                posts_count_24h=10,
                posts_count_48h=20,
                posts_count_72h=30,
                posts_count_96h=40,
                posts_count_120h=50,
                posts_count_144h=60,
                posts_count_168h=70,
            )
            await statistic_dao.commit()

        stats = await statistic_dao.get_channel_statistics(
            channel_id=test_channel.id, sorting="oldest", skip=0, limit=None
        )
        assert len(stats) == 3
        # Oldest first means the first inserted (index 0) should be first
        assert stats[0].subscribers_count == 100
        assert stats[2].subscribers_count == 102

        # Test offset and limit
        limited_stats = await statistic_dao.get_channel_statistics(
            channel_id=test_channel.id, sorting="oldest", skip=1, limit=2
        )
        assert len(limited_stats) == 2
        assert limited_stats[0].subscribers_count == 101
        assert limited_stats[1].subscribers_count == 102
