from pydantic import BaseModel
from .. import Channel


class GetChannelRequest(BaseModel):
    channel_id: int


class GetChannelResponse(BaseModel):
    last_update: int
    channel: Channel
