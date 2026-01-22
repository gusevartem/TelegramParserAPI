from __future__ import annotations

from datetime import datetime
from typing import Any, override
from uuid import UUID, uuid4

from sqlalchemy import String, event, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from ._base import BaseDAO, BaseModel


class Media(BaseModel):
    __tablename__: str = "media"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    mime_type: Mapped[str] = mapped_column(String(255))
    size_bytes: Mapped[int] = mapped_column()
    file_name: Mapped[str] = mapped_column(String(255))

    recorded_at: Mapped[datetime] = mapped_column(server_default=func.now())


class MediaDAO(BaseDAO[Media, UUID]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Media)

    @override
    async def create(self, mime_type: str, size_bytes: int) -> Media:
        new_media = Media(
            mime_type=mime_type,
            size_bytes=size_bytes,
        )
        await self.save(new_media)
        return new_media


@event.listens_for(Media, "before_update")
def receive_before_update(_mapper: Any, _connection: Any, _target: Any) -> None:
    raise RuntimeError("Media records are immutable and cannot be updated.")
