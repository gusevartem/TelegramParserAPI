from pydantic import BaseModel
from .. import Channel


class GetChannelByLinkRequest(BaseModel):
    channel_link: str


class GetChannelByLinkResponse(BaseModel):
    last_update: int
    channel: Channel
