FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    postgresql-client \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @openai/codex@0.134.0
RUN mkdir -p /root/.codex

COPY pyproject.toml .
# Include the `telegram` extra (Telethon) so the telegram_gateway_worker and the
# Telegram backfill client can run in-container. Telethon is pure-Python and
# small; the app still imports it lazily, so non-Telegram deployments are
# unaffected by its presence.
RUN pip install --no-cache-dir '.[telegram]'

COPY . .

ENV PYTHONPATH=/app

EXPOSE 8000
CMD ["uvicorn", "services.app.gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
