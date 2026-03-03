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
RUN useradd -m worker
USER worker
WORKDIR /home/worker/app
ENV PATH="/home/worker/.local/bin:$PATH"

# Install regular dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install worker project
ARG wheel=worker-0.1.0-py3-none-any.whl
COPY --chown=worker:worker ./dist/$wheel .
RUN pip install --no-cache-dir --upgrade --no-warn-script-location /home/worker/app/$wheel

COPY --chown=worker:worker entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["/home/worker/app/entrypoint.sh"]

CMD ["worker"]