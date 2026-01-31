FROM python:3.12-slim-bookworm

# Install regular dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg ca-certificates locales tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set timezone
RUN ln -sf /usr/share/zoneinfo/UTC /etc/localtime \
    && echo "UTC" > /etc/timezone
ENV TZ=UTC
ENV TIME_ZONE=UTC

RUN python -m pip install --upgrade pip

# Add user
RUN useradd -m migrator
USER migrator
WORKDIR /home/migrator/app

# Install migrator dependencies
ARG wheel=migrator-0.1.0-py3-none-any.whl
COPY --chown=migrator:migrator ./dist/$wheel .
RUN pip install --no-cache-dir --upgrade --no-warn-script-location /home/migrator/app/$wheel

CMD ["python", "-m", "parser.migrator", "upgrade", "head"]