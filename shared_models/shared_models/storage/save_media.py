from pydantic import BaseModel
from uuid import UUID


class SaveMediaRequest(BaseModel):
    media_id: UUID
    media: bytes
