from pydantic import BaseModel
from ..channel import Channel


class AddChannelRequest(BaseModel):
    channel_link: str


class AddChannelResponse(BaseModel):
    channel: Channel
    success: bool
