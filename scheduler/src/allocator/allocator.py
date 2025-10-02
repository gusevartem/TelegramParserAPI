from datetime import datetime, timedelta
from .time_slot import TimeSlot
from typing import Optional


class Allocator:
    def __init__(
        self,
        slots_count: int,
        allocation_interval_minutes: int,
        channels: list[int] = [],
    ) -> None:
        self.slots_count = slots_count
        self.allocation_interval_minutes = allocation_interval_minutes
        self.__slots = self.__create_slots()

        minimal_slot_index = 0
        for channel_id in channels:
            slot = self.__slots[minimal_slot_index % self.slots_count]
            slot.add_channel(channel_id)
            minimal_slot_index += 1

    def get_next_channels(self) -> Optional[list[int]]:
        if len(self.__slots) == 0:
            raise ValueError("No slots available")
        slot = self.__slots[0]
        if slot.start_time <= datetime.now():
            slot = self.__slots.pop(0)
            return [i for i in slot]
        return None

    def __create_slots(self) -> list[TimeSlot]:
        return [
            TimeSlot(
                datetime.now() + timedelta(minutes=self.allocation_interval_minutes * i)
            )
            for i in range(self.slots_count)
        ]
