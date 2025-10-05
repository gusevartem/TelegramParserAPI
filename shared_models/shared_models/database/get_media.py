from pydantic import RootModel, BaseModel
from ..message import MessageMedia
from uuid import UUID


class GetMediaRequest(BaseModel):
    media_id: UUID


class GetMediaResponse(RootModel):
    root: MessageMedia
