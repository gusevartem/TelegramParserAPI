import logging
from tortoise import Tortoise
from .config import TORTOISE_ORM
from shared_models import Channel as ChannelSharedModel
from shared_models.database.update_or_create_channel import (
    UpdateOrCreateChannelResponse,
)
from shared_models.database.get_channel import GetChannelRequest, GetChannelResponse
from shared_models.database.errors import (
    ChannelDoesNotExistError,
    StatsDoesNotExistError,
)
from shared_models.database.get_channels_ids import GetChannelsIdsResponse
from shared_models.database.get_24h_statistics import (
    Get24hStatisticsRequest,
    Get24hStatisticsResponse,
    StatisticsSorting,
    StatisticsItem,
)
from shared_models.database.get_channel_by_link import (
    GetChannelByLinkRequest,
    GetChannelByLinkResponse,
)
from .models import Channel, ChannelStatistics
from tortoise.exceptions import DoesNotExist


class Database:
    def __init__(self) -> None:
        self.logging = logging.getLogger("database")

    async def connect(self):
        self.logging.info("Initializing database")
        await Tortoise.init(config=TORTOISE_ORM)
        await Tortoise.generate_schemas()
        self.logging.info("Database initialized")

    async def close(self):
        await Tortoise.close_connections()
        self.logging.info("Database closed")

    # Methods
    @staticmethod
    async def update_or_create_channel(
        ctx, channel: ChannelSharedModel
    ) -> UpdateOrCreateChannelResponse:
        self: Database = ctx["Database_instance"]
        self.logging.info(f"Updating or creating channel {channel.name}")
        result, created = await Channel.update_or_create(
            id=channel.channel_id,
            defaults={
                "name": channel.name,
                "description": channel.description,
                "link": channel.link,
            },
        )
        await ChannelStatistics.create(
            channel=result,
            subscribers=channel.subscribers,
            views_24h=channel.views,
            posts_count=channel.posts_count,
        )

        return UpdateOrCreateChannelResponse(record_created=created)

    @staticmethod
    async def get_channel(ctx, request: GetChannelRequest) -> GetChannelResponse:
        self: Database = ctx["Database_instance"]
        try:
            channel = await Channel.get(id=request.channel_id)
        except DoesNotExist:
            self.logging.error(f"Channel with id {request.channel_id} does not exist")
            raise ChannelDoesNotExistError(request.channel_id)

        statistics = (
            await ChannelStatistics.filter(channel=channel)
            .order_by("-recorded_at")
            .first()
        )

        if statistics is None:
            self.logging.error(
                f"Statistics for channel with id {request.channel_id} do not exist"
            )
            raise StatsDoesNotExistError(request.channel_id)

        channel_response = ChannelSharedModel(
            channel_id=channel.id,
            link=channel.link,
            name=channel.name,
            description=channel.description,
            subscribers=statistics.subscribers,
            views=statistics.views_24h,
            posts_count=statistics.posts_count,
        )

        return GetChannelResponse(
            last_update=int(statistics.recorded_at.timestamp()),
            channel=channel_response,
        )

    @staticmethod
    async def get_channel_by_link(
        ctx, request: GetChannelByLinkRequest
    ) -> GetChannelByLinkResponse:
        self: Database = ctx["Database_instance"]
        link = request.channel_link
        if link.startswith("https://"):
            link = link.removeprefix("https://")
        elif link.startswith("http://"):
            link = link.removeprefix("http://")

        try:
            channel = await Channel.get(link=link)
        except DoesNotExist:
            self.logging.error(f"Channel with link {link} does not exist")
            raise ChannelDoesNotExistError(link)
        statistics = (
            await ChannelStatistics.filter(channel=channel)
            .order_by("-recorded_at")
            .first()
        )
        if statistics is None:
            self.logging.error(f"Statistics for channel with link {link} do not exist")
            raise StatsDoesNotExistError(link)
        channel_response = ChannelSharedModel(
            channel_id=channel.id,
            link=channel.link,
            name=channel.name,
            description=channel.description,
            subscribers=statistics.subscribers,
            views=statistics.views_24h,
            posts_count=statistics.posts_count,
        )
        return GetChannelByLinkResponse(
            last_update=int(statistics.recorded_at.timestamp()),
            channel=channel_response,
        )

    @staticmethod
    async def get_channels_ids(ctx) -> GetChannelsIdsResponse:
        ids = await Channel.all().values_list("id", flat=True)
        return GetChannelsIdsResponse(channel_ids=ids)  # type: ignore

    @staticmethod
    async def get_24h_statistics(
        ctx, request: Get24hStatisticsRequest
    ) -> Get24hStatisticsResponse:
        self: Database = ctx["Database_instance"]
        try:
            channel = await Channel.get(id=request.channel_id)
        except DoesNotExist:
            self.logging.error(f"Channel with id {request.channel_id} does not exist")
            raise ChannelDoesNotExistError(request.channel_id)

        order_by = (
            "-recorded_at"
            if request.sorting == StatisticsSorting.NEWEST
            else "recorded_at"
        )

        statistics = await ChannelStatistics.filter(channel=channel).order_by(order_by)

        data = [
            StatisticsItem(
                views=stat.views_24h,
                subscribers=stat.subscribers,
                posts_count=stat.posts_count,
                time=int(stat.recorded_at.timestamp()),
            )
            for stat in statistics
        ]

        return Get24hStatisticsResponse(sorting=request.sorting, data=data)
