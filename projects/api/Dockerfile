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
RUN useradd -m api
USER api
WORKDIR /home/api/app
ENV PATH="/home/api/.local/bin:$PATH"

# Install regular dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install api project
ARG wheel=api-0.1.0-py3-none-any.whl
COPY --chown=api:api ./dist/$wheel .
RUN pip install --no-cache-dir --upgrade --no-warn-script-location /home/api/app/$wheel

COPY --chown=api:api entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["/home/api/app/entrypoint.sh"]

CMD ["api"]