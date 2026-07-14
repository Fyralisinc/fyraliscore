# ============================================================================
# Fyralis BYOC — onboarding CLI image (WS-ONBOARD)
# ----------------------------------------------------------------------------
# A small image that carries the onboarding CLI (`onboard` / `offboard`) plus the
# committed control-plane primitives it reuses (ca/, signing/, lib/). It is NOT a
# long-running service: it is invoked on demand to enroll/offboard a tenant, e.g.
#
#   docker compose -f docker-compose.control-plane.yml \
#     run --rm onboarding onboard --tenant acme --region us-east --plan standard
#
# Build context is the control-plane/ ROOT (so ca/, signing/, lib/ are copied in);
# see service.compose.yml `build.context: ..`.
# ============================================================================
FROM python:3.12-slim

WORKDIR /app

# Runtime deps: cryptography (CA/signing), pydantic (records), httpx (console),
# fastapi/starlette + uvicorn (embedded console for dev/demo).
RUN pip install --no-cache-dir \
        "cryptography>=42" \
        "pydantic>=2" \
        "httpx>=0.27" \
        "fastapi>=0.110" \
        "uvicorn>=0.29" \
    && useradd --create-home --uid 10001 onboard

# Copy the primitives this CLI imports plus the onboarding package itself.
# (Build context is control-plane/.)
COPY lib/        /app/lib/
COPY ca/         /app/ca/
COPY signing/    /app/signing/
COPY onboarding/ /app/onboarding/

ENV PYTHONUNBUFFERED=1

USER onboard

# Default to showing onboard help; the compose `command`/`run` overrides this.
ENTRYPOINT ["python", "/app/onboarding/cli.py"]
CMD ["--help"]
