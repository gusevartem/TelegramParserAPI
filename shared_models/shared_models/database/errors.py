class ChannelDoesNotExistError(Exception):
    def __init__(self, channel_id):
        self.channel_id = channel_id

    def __str__(self):
        return f"Channel with id {self.channel_id} does not exist."


class StatsDoesNotExistError(Exception):
    def __init__(self, channel_id):
        self.channel_id = channel_id

    def __str__(self):
        return f"Stats for channel with id {self.channel_id} do not exist."
