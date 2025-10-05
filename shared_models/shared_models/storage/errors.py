from uuid import UUID


class LogoNotFoundError(Exception):
    def __init__(self, channel_id: int):
        self.channel_id = channel_id

    def __str__(self):
        return f"Logo not found for channel {self.channel_id}"


class MediaNotFoundError(Exception):
    def __init__(self, media_id: UUID):
        self.media_id = media_id

    def __str__(self):
        return f"Media with id {self.media_id} not found"
