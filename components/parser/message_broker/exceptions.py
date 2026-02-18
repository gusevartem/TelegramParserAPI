class MessageBrokerException(Exception):
    """Базовое исключение для работы с брокером сообщений."""


class InvalidMessageError(MessageBrokerException):
    """Выбрасывается, если произошла ошибка при валидации сообщения."""


class InvalidTask(MessageBrokerException):
    """Нужно выбросить в случае, если задача невалидна.
    Тогда она не будет заново добавлена в очередь"""
