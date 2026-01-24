class NoWorkingClientsFoundError(Exception):
    pass


class AllClientsAreBusyError(Exception):
    pass


class ClientBanned(Exception):
    """При бане клиента нужно выбрасывать это исключение.
    Оно будет обработано в RabbitMQSessionStorage: сессия не сохранится в очередь.
    Telegram: удалит сессию из базы данных."""

    pass


class FloodWait(Exception):
    """В случае FloodWait у клиента нужно выбрасывать это исключение.
    Оно будет обработано в RabbitMQSessionStorage: сессия уйдет полежать на e.seconds"""

    def __init__(self, seconds: int, message: str) -> None:
        self.seconds: int = seconds
        super().__init__()
