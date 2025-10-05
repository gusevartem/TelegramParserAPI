from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from enum import StrEnum


class MessageMediaType(StrEnum):
    PHOTO = "photo"
    DOCUMENT = "document"


class MessageMedia(BaseModel):
    mime_type: Optional[str]
    media_type: MessageMediaType
    data: Optional[bytes] = None
    id: Optional[UUID] = None


class Message(BaseModel):
    message_id: int
    date: int
    text: str
    views: Optional[int]
    media: list[MessageMedia] = []
