FROM python:3.12-slim

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

# Install the github-intel interface (ADR-0004 first interface) into the shared
# image so the gateway, observation_writer (inline enricher), and the
# extension_workers supervisor all discover its `company_os.*` entry points.
# --no-deps keeps the baked company-os; pin a tag/commit for reproducible builds,
# or pass --build-arg GITHUB_INTEL_REF="" to build a bare core image without it.
# (Vendored alternative: COPY a local checkout and `pip install --no-deps ./path`.)
ARG GITHUB_INTEL_REF=main
RUN if [ -n "$GITHUB_INTEL_REF" ]; then \
      pip install --no-cache-dir --no-deps \
        "fyralis-github-intel @ git+https://github.com/Fyralisinc/github-intel@${GITHUB_INTEL_REF}"; \
    fi

EXPOSE 8000
CMD ["uvicorn", "services.app.gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
