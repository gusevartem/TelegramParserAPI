# TelegramParserAPI

Сервис для сбора, хранения и предоставления статистики Telegram-каналов. Получает данные через Telegram MTProto API (Telethon), сохраняет в MySQL, медиафайлы — в S3-совместимое хранилище (MinIO). Доступ к данным — через REST API.

## Архитектура

Проект построен по **Polylith** монорепо-архитектуре:

```
bases/parser/         ← точки входа (исполняемые компоненты)
  api/                  FastAPI приложение
  worker/               воркер обработки задач
  scheduler_loop/       планировщик очереди

components/parser/    ← переиспользуемые библиотечные компоненты
  dto/                  Pydantic DTO-модели
  logging/              настройка structlog
  message_broker/       абстракция RabbitMQ (aio-pika)
  opentele/             конвертация Telegram-сессий
  persistence/          SQLAlchemy ORM-модели + DAO
  scheduler/            логика bucket-планирования задач
  storage/              абстракция S3/MinIO
  telegram/             пул Telegram-клиентов

projects/             ← деплоируемые сервисы (каждый с Dockerfile)
  api/                  → bases/parser/api
  worker/               → bases/parser/worker
  migrator/             запуск миграций Alembic
  scheduler/            → bases/parser/scheduler_loop
```

## Как работает

### Поток данных

```
Scheduler ──► RabbitMQ ──► Worker ──► MySQL + MinIO
                                │
                    API ◄───────┘
```

### Scheduler

Запускается каждые 30 секунд. Выбирает из MySQL задачи (`parsing_task`), чей **bucket** (минута 0–59) попадает в текущее временное окно, и публикует их в очередь RabbitMQ. Задачи равномерно распределяются по бакетам при создании, обеспечивая равномерную нагрузку в течение часа — каждый канал парсится примерно раз в час.

### Worker

Держит одно активное Telegram-соединение за сессию (до `account_lock_hours`, по умолчанию 20 ч). Получает задачу из очереди, через Telethon запрашивает у Telegram:

- метаданные канала (название, описание, ID)
- логотип — скачивает и загружает в MinIO
- список последних сообщений и их просмотры
- агрегированную статистику: подписчики, суммарные просмотры и количество постов за окна 24–168 часов (с шагом 24 ч)

Результат сохраняется в MySQL. При `FloodWait` или бане аккаунта воркер завершает сессию, аккаунт помечается как `banned`.

### API

FastAPI-приложение с двумя группами эндпоинтов:

**`/parser`** — управление (требует `SECRET_KEY`):
| Метод | Путь | Описание |
|-------|------|----------|
| `POST` | `/parser/schedule` | Добавить канал в очередь парсинга |
| `GET` | `/parser/task` | Статус задачи по ID |
| `POST` | `/parser/client` | Добавить Telegram-аккаунт (`.session` файл) |

**`/public`** — чтение данных (без авторизации):
| Метод | Путь | Описание |
|-------|------|----------|
| `GET` | `/public/channel` | Данные канала + последняя статистика |
| `GET` | `/public/channel/ids` | Список всех ID каналов |
| `GET` | `/public/channel/statistics` | История статистики канала |
| `GET` | `/public/channel/messages` | Сообщения канала |
| `GET` | `/public/media` | Presigned URL для медиафайла |

## Модели данных

| Таблица | Описание |
|---------|----------|
| `channel` | Канал: ID, название, описание, ссылка на логотип |
| `channel_statistic` | Снимок статистики: подписчики, просмотры/посты за 24–168 ч |
| `channel_message` | Сообщение канала: текст, дата, ID в Telegram |
| `channel_message_statistic` | Статистика просмотров сообщения в момент времени |
| `media` | Медиафайл (логотип): MIME, размер, имя файла в S3 |
| `parsing_task` | Задача парсинга: URL канала, bucket, статус, дата последнего парсинга |
| `task_claim_history` | История захвата задач воркерами |
| `telegram_client` | Telegram-аккаунт: session string, API credentials, флаг бана |

## Технологии

| Категория | Инструмент |
|-----------|-----------|
| API-фреймворк | FastAPI + Uvicorn |
| Telegram MTProto | Telethon |
| Message broker | RabbitMQ (aio-pika) |
| ORM + миграции | SQLAlchemy (async) + Alembic |
| База данных | MySQL 8 |
| Объектное хранилище | MinIO (S3-compatible) |
| Dependency injection | Dishka |
| Валидация / конфиг | Pydantic + pydantic-settings |
| Structured logging | structlog |
| Distributed tracing | OpenTelemetry + Jaeger |
| Контейнеризация | Docker + Docker Compose |
| Монорепо-архитектура | Polylith |
| Управление зависимостями | Poetry |
| Тестирование | pytest |
| Линтер / типизация | ruff + mypy |
