from __future__ import annotations

from uuid import UUID

from parser.persistence import (
    Channel as ChannelPersistence,
)
from parser.persistence import (
    ChannelMessage as ChannelMessagePersistence,
)
from parser.persistence import (
    ChannelMessageStatistic as ChannelMessageStatisticPersistence,
)
from parser.persistence import (
    ChannelStatistic as ChannelStatisticPersistence,
)
from parser.persistence import (
    Media as MediaPersistence,
)
from parser.persistence import ParsingTask as ParsingTaskPersistence
from parser.persistence import ParsingTaskStatus, ProxyType
from parser.persistence import TelegramClientProxy as TelegramClientProxyPersistence
from pydantic import BaseModel


class Media(BaseModel):
    id: UUID
    mime_type: str
    size_bytes: int
    file_name: str
    recorded_at: int

    @staticmethod
    def from_persistence(media: MediaPersistence) -> Media:
        return Media(
            id=media.id,
            mime_type=media.mime_type,
            size_bytes=media.size_bytes,
            file_name=media.file_name,
            recorded_at=int(media.recorded_at.timestamp()),
        )


class MediaWithURL(Media):
    url: str

    @staticmethod
    def from_media(media: Media, url: str) -> MediaWithURL:
        return MediaWithURL(**media.model_dump(), url=url)


class ChannelStatistic(BaseModel):
    subscribers_count: int
    views: int
    posts_count: int
    recorded_at: int

    @staticmethod
    def from_persistence(statistic: ChannelStatisticPersistence) -> ChannelStatistic:
        return ChannelStatistic(
            subscribers_count=statistic.subscribers_count,
            views=statistic.views,
            posts_count=statistic.posts_count,
            recorded_at=int(statistic.recorded_at.timestamp()),
        )


class Channel(BaseModel):
    id: int
    name: str
    description: str | None
    logo: Media | None
    newest_statistic: ChannelStatistic
    recorded_at: int
    updated_at: int

    @staticmethod
    def from_persistence(
        channel: ChannelPersistence, newest_statistic: ChannelStatisticPersistence
    ) -> Channel:
        return Channel(
            id=channel.id,
            name=channel.name,
            description=channel.description,
            logo=Media.from_persistence(channel.logo) if channel.logo else None,
            newest_statistic=ChannelStatistic.from_persistence(newest_statistic),
            recorded_at=int(channel.recorded_at.timestamp()),
            updated_at=int(channel.updated_at.timestamp()),
        )


class ChannelMessageStatistic(BaseModel):
    views: int
    recorded_at: int

    @staticmethod
    def from_persistence(
        statistic: ChannelMessageStatisticPersistence,
    ) -> ChannelMessageStatistic:
        return ChannelMessageStatistic(
            views=statistic.views,
            recorded_at=int(statistic.recorded_at.timestamp()),
        )


class ChannelMessage(BaseModel):
    id: int
    created_at: int
    text: str
    media: list[Media]
    statistics: list[ChannelMessageStatistic]
    recorded_at: int
    updated_at: int

    @staticmethod
    def from_persistence(message: ChannelMessagePersistence) -> ChannelMessage:
        return ChannelMessage(
            id=message.id,
            created_at=int(message.created_at.timestamp()),
            text=message.text,
            media=[Media.from_persistence(media) for media in message.media],
            statistics=[
                ChannelMessageStatistic.from_persistence(statistic)
                for statistic in message.statistics
            ],
            recorded_at=int(message.recorded_at.timestamp()),
            updated_at=int(message.updated_at.timestamp()),
        )


class TelegramCredentials(BaseModel):
    api_id: int
    api_hash: str
    device_model: str
    system_version: str
    app_version: str
    lang_code: str
    system_lang_code: str


class ProxySettings(BaseModel):
    proxy_type: ProxyType
    host: str
    port: int
    username: str | None
    password: str | None

    @staticmethod
    def to_persistence(proxy: ProxySettings) -> TelegramClientProxyPersistence:
        return TelegramClientProxyPersistence(
            proxy_type=proxy.proxy_type,
            host=proxy.host,
            port=proxy.port,
            username=proxy.username,
            password=proxy.password,
        )

    @staticmethod
    def from_persistence(proxy: TelegramClientProxyPersistence) -> ProxySettings:
        return ProxySettings(
            proxy_type=proxy.proxy_type,
            host=proxy.host,
            port=proxy.port,
            username=proxy.username,
            password=proxy.password,
        )


class ParsingTask(BaseModel):
    id: UUID
    url: str
    channel_id: int | None
    status: ParsingTaskStatus
    next_run_at: int | None
    last_parsed_at: int | None
    created_at: int

    @staticmethod
    def from_persistence(
        task: ParsingTaskPersistence, next_run_at: int | None
    ) -> ParsingTask:
        return ParsingTask(
            id=task.id,
            url=task.url,
            channel_id=task.channel_id,
            status=task.status,
            next_run_at=next_run_at,
            last_parsed_at=int(task.last_parsed_at.timestamp())
            if task.last_parsed_at
            else None,
            created_at=int(task.created_at.timestamp()),
        )
