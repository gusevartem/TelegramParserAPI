from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, override
from uuid import UUID

from sqlalchemy import ForeignKey, Text, func
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ._base import BaseDAO, BaseModel

if TYPE_CHECKING:
    from .channel import Channel
    from .media import Media


class ChannelMessage(BaseModel):
    __tablename__: str = "channel_message"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    created_at: Mapped[datetime] = mapped_column()
    text: Mapped[str] = mapped_column(Text)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey(Channel.id, ondelete="CASCADE"), index=True
    )

    recorded_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )

    channel: Mapped[Channel] = relationship(back_populates="messages")
    media_links: Mapped[list[MessageMediaLink]] = relationship(
        back_populates="message", cascade="all, delete-orphan", passive_deletes=True
    )
    media: AssociationProxy[list[Media]] = association_proxy(
        "media_links", "media_item"
    )


class ChannelMessageDAO(BaseDAO[ChannelMessage, int]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, ChannelMessage)

    @override
    async def create(
        self, channel: Channel, id: int, created_at: datetime, text: str
    ) -> ChannelMessage:
        new_message = ChannelMessage(
            id=id,
            created_at=created_at,
            text=text,
            channel=channel,
        )
        await self.save(new_message)
        return new_message


class MessageMediaLink(BaseModel):
    __tablename__: str = "message_media_link"

    message_id: Mapped[int] = mapped_column(
        ForeignKey(ChannelMessage.id, ondelete="CASCADE"), primary_key=True
    )
    media_id: Mapped[UUID] = mapped_column(ForeignKey(Media.id, ondelete="CASCADE"))

    message: Mapped[ChannelMessage] = relationship(back_populates="media_links")
    media_item: Mapped[Media] = relationship()
