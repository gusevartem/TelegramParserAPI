class InvalidChannelLink(Exception):
    def __init__(self, url: str, details: str):
        self.url = url
        self.details = details

    def __str__(self):
        return f"Invalid channel link: {self.url}. Details: {self.details}"


class FloodWait(Exception):
    def __init__(self, seconds: int):
        self.seconds = seconds

    def __str__(self):
        return f"Flood wait: {self.seconds} seconds"


class UserBan(Exception):
    def __init__(self, details: str):
        self.details = details

    def __str__(self):
        return f"User ban: {self.details}"


class CannotGetChannelInfo(Exception):
    def __init__(self, url: str):
        self.url = url

    def __str__(self):
        return f"Cannot get channel info for {self.url}"


class SessionPasswordNeeded(Exception):
    def __str__(self):
        return "2fa password provided. Add 2FA.txt file to archive"
