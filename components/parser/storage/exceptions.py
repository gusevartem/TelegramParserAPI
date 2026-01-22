class StorageException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class MediaTooLargeError(StorageException):
    def __init__(self, file_name: str, max_size: int, file_size: int) -> None:
        super().__init__(
            f"File {file_name} is too large. "
            + f"Max size: {max_size} bytes. "
            + f"File size: {file_size} bytes"
        )


class ConfigError(StorageException):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class MediaNotFoundError(StorageException):
    def __init__(self, key: str) -> None:
        super().__init__(f"Key {key} does not exist")


class MaxRetriesExceededError(StorageException):
    def __init__(self, retries: int, code: str | None = None) -> None:
        super().__init__(
            f"S3 error after {retries} retries{': ' + code if code else ''}"
        )
