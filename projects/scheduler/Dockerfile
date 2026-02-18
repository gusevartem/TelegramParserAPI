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
RUN useradd -m scheduler
USER scheduler
WORKDIR /home/scheduler/app

# Install scheduler dependencies
ARG wheel=scheduler-0.1.0-py3-none-any.whl
COPY --chown=scheduler:scheduler ./dist/$wheel .
RUN pip install --no-cache-dir --upgrade --no-warn-script-location /home/scheduler/app/$wheel

CMD ["python", "-m", "parser.scheduler_loop"]