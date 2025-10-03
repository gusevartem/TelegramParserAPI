from pydantic import BaseModel
from shared_models import Channel


class AddChannelResponse(BaseModel):
    channel: Channel
