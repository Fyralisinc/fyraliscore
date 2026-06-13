FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY . .

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
