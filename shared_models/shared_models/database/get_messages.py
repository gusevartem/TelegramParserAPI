from pydantic import RootModel, BaseModel
from ..message import Message


class GetMessagesRequest(BaseModel):
    channel_id: int


class GetMessagesResponse(RootModel):
    root: list[Message]
