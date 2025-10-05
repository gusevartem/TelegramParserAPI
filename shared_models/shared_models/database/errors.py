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


class MediaDoesNotExistError(Exception):
    def __init__(self, media_id):
        self.media_id = media_id

    def __str__(self):
        return f"Media with id {self.media_id} does not exist."
