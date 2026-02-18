class WorkerException(Exception):
    pass


class TaskExists(WorkerException):
    def __init__(self, message: str, channel_id: int):
        self.channel_id: int = channel_id
        super().__init__(message)


class TaskError(WorkerException):
    pass


class TemporaryCannotProcessTask(WorkerException):
    pass
