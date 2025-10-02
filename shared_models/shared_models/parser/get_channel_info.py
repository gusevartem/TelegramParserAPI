from pydantic import BaseModel
from ..channel import Channel
from typing_extensions import Optional


class GetChannelInfoRequest(BaseModel):
    channel_link: str
    get_logo: bool = False


class GetChannelInfoResponse(BaseModel):
    channel: Channel
    logo: Optional[bytes]
