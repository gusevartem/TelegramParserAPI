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
from parser.persistence import ProxyType
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


class MediaWithLink(Media):
    link: str

    @staticmethod
    def from_media(media: Media, link: str) -> MediaWithLink:
        return MediaWithLink(**media.model_dump(), link=link)


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
    link: str
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
            link=channel.link,
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
