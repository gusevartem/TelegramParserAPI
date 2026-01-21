from uuid import UUID

from pydantic import BaseModel


class Media(BaseModel):
    id: UUID
    mime_type: str
    size_bytes: int
    recorded_at: int


class MediaWithLink(Media):
    link: str


class ChannelStatistic(BaseModel):
    subscribers_count: int
    views: int
    posts_count: int
    recorded_at: int


class Channel(BaseModel):
    id: int
    link: str
    name: str
    description: str | None
    logo: Media | None
    newest_statistic: ChannelStatistic
    recorder_at: int
    updated_at: int


class ChannelMessageStatistic(BaseModel):
    views: int
    recorded_at: int


class ChannelMessage(BaseModel):
    id: int
    created_at: int
    text: str
    media: list[Media]
    statistics: list[ChannelMessageStatistic]
    recorded_at: int
    updated_at: int
