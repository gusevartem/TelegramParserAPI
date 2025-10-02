class LogoNotFoundError(Exception):
    def __init__(self, channel_id: int):
        self.channel_id = channel_id

    def __str__(self):
        return f"Logo not found for channel {self.channel_id}"
