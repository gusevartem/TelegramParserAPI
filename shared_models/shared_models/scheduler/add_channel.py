from pydantic import BaseModel
from ..channel import Channel
from ..message import Message


class AddChannelRequest(BaseModel):
    channel_link: str


class AddChannelResponse(BaseModel):
    channel: Channel
    messages: list[Message]
    success: bool
