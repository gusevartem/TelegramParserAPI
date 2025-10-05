from pydantic import BaseModel
from uuid import UUID


class GetMediaRequest(BaseModel):
    media_id: UUID


class GetMediaResponse(BaseModel):
    media: bytes
