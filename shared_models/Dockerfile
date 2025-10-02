FROM python:3.11-bullseye

WORKDIR /app
COPY shared_models /app/shared_models/shared_models
COPY pyproject.toml /app/shared_models/pyproject.toml
RUN pip install --no-cache-dir /app/shared_models