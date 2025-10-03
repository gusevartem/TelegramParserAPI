from datetime import datetime


class TimeSlot:
    def __init__(self, start_time: datetime) -> None:
        self.start_time = start_time
        self.__channels: list[int] = []

    def add_channel(self, channel_id: int) -> None:
        self.__channels.append(channel_id)

    def channels_count(self) -> int:
        return len(self.__channels)

    def __iter__(self):
        yield from self.__channels
