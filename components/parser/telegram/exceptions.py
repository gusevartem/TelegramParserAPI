class TelegramException(Exception):
    """Базовое исключение для пакета telegram."""

    ...


class NoWorkingClientsFoundError(TelegramException):
    """Возникает в Telegram.get_client при отсутствии рабочих клиентов в базе данных
    (все клиенты отмечены как banned или ни одного клиента нет в базе данных)."""

    ...


class InvalidClient(TelegramException):
    """Универсальное исключение,
    сигнализирующее о том, что текущий клиент не валиден."""

    def __init__(self, message: str, user_id: int | None = None) -> None:
        self.message: str = message
        self.user_id: int | None = user_id
        super().__init__(message)


class ClientBanned(TelegramException):
    """При бане клиента нужно выбрасывать это исключение.
    MySQLSessionStorage пометит аккаунт как banned и не будет его выдавать."""

    ...


class ChannelAccessDenied(TelegramException):
    """Аккаунт забанен в конкретном канале, но сессия остаётся рабочей.
    Задача должна быть помечена как ошибка, сессия продолжает работу."""

    ...


class FloodWait(TelegramException):
    """В случае FloodWait у клиента нужно выбрасывать это исключение.
    MySQLSessionStorage выставит locked_until = now + seconds, аккаунт будет
    пропускаться в выборке до истечения блокировки."""

    def __init__(self, seconds: int, message: str) -> None:
        self.seconds: int = seconds
        super().__init__()
