from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Literal, override
from uuid import UUID

from sqlalchemy import ForeignKey, Text, func, select, BigInteger
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload

from ._base import BaseDAO, BaseModel

if TYPE_CHECKING:
    from .channel import Channel
    from .channel_message_statistic import ChannelMessageStatistic
    from .media import Media


class ChannelMessage(BaseModel):
    __tablename__: str = "channel_message"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    created_at: Mapped[datetime] = mapped_column()
    text: Mapped[str] = mapped_column(Text)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channel.id", ondelete="CASCADE"), index=True
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
    statistics: Mapped[list[ChannelMessageStatistic]] = relationship(
        back_populates="message", cascade="all, delete-orphan", passive_deletes=True
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

    async def get_channel_messages(
        self,
        channel_id: int,
        sorting: Literal["newest", "oldest"],
        skip: int,
        limit: int | None,
    ) -> list[ChannelMessage]:
        stmt = (
            select(ChannelMessage)
            .where(ChannelMessage.channel_id == channel_id)
            .options(
                selectinload(ChannelMessage.statistics),
                selectinload(ChannelMessage.media_links).joinedload(
                    MessageMediaLink.media_item
                ),
            )
            .offset(skip)
        )

        if sorting == "newest":
            stmt = stmt.order_by(ChannelMessage.created_at.desc())
        else:
            stmt = stmt.order_by(ChannelMessage.created_at.asc())

        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def find_with_loaded_statistics_and_media(
        self, message_id: int
    ) -> ChannelMessage | None:
        stmt = (
            select(ChannelMessage)
            .options(
                selectinload(ChannelMessage.statistics),
                selectinload(ChannelMessage.media_links).joinedload(
                    MessageMediaLink.media_item
                ),
            )
            .where(ChannelMessage.id == message_id)
        )

        result = await self._session.execute(stmt)
        return result.scalars().first()


class MessageMediaLink(BaseModel):
    __tablename__: str = "message_media_link"

    media_id: Mapped[UUID] = mapped_column(
        ForeignKey("media.id", ondelete="CASCADE"), primary_key=True
    )
    message_id: Mapped[int] = mapped_column(
        ForeignKey("channel_message.id", ondelete="CASCADE"),
    )

    message: Mapped[ChannelMessage] = relationship(back_populates="media_links")
    media_item: Mapped[Media] = relationship()
