from pydantic import BaseModel
from typing import List
from enum import Enum


class Get24hStatisticsRequest(BaseModel):
    channel_id: int
    sorting: "StatisticsSorting"


class Get24hStatisticsResponse(BaseModel):
    sorting: "StatisticsSorting"
    data: List["StatisticsItem"]


class StatisticsSorting(str, Enum):
    NEWEST = "newest"
    OLDEST = "oldest"


class StatisticsItem(BaseModel):
    views: int
    subscribers: int
    posts_count: int
    time: int
