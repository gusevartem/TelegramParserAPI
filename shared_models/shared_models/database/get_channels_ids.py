from pydantic import BaseModel
from typing import List


class GetChannelsIdsResponse(BaseModel):
    channel_ids: List[int]
