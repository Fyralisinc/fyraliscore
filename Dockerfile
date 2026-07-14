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

COPY pyproject.toml README.md ./
ARG INSTALL_BROWSER_AGENT=1
RUN if [ "$INSTALL_BROWSER_AGENT" = "1" ]; then \
      pip install --no-cache-dir ".[browser-agent]" && \
      python -m playwright install --with-deps chromium; \
    else \
      pip install --no-cache-dir .; \
    fi

COPY . .

ENV PYTHONPATH=/app
ENV FYRALIS_SOURCE_AUTO_CONNECT_EXECUTE_BROWSER_DOM=1
ENV FYRALIS_SOURCE_AUTO_CONNECT_HEADLESS_BROWSER=1

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
