# Fyralis config-distribution service (WS-CONFIG / P5) — signed per-deployment config bundles.
#
# Build context is the CONTROL-PLANE ROOT (control-plane/), NOT config-dist/, so the image
# can include the signing/ package the service imports at runtime (sign_bundle, verify_bundle,
# signing_lib) and the lib/ package (TelemetryTier validation). All config signing/verifying is
# delegated to that real library (C2/I6) — we never re-implement crypto.
#
#   docker build -f config-dist/Dockerfile -t fyralis/config-dist .
#
# In docker-compose this is wired with build.context: . and build.dockerfile:
# config-dist/Dockerfile (see config-dist/service.compose.yml).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Tight runtime surface: crypto (ed25519 signing), the API stack, pydantic.
# cryptography ships manylinux wheels so no build toolchain is needed.
RUN pip install --no-cache-dir \
        "cryptography>=42.0" \
        "fastapi>=0.110" \
        "uvicorn[standard]>=0.29" \
        "pydantic>=2.0"

# The service imports signing/ (sign/verify) and lib/ (TelemetryTier) at runtime.
COPY signing/ /app/signing/
COPY lib/ /app/lib/
COPY config-dist/ /app/config-dist/

# Make the sibling packages importable the same way the modules do at top level:
# /app on the path so `import lib` resolves; /app/signing so `import sign_bundle`
# / `import verify_bundle` / `import signing_lib` resolve; /app/config-dist for the
# service + store + publish_config modules.
ENV PYTHONPATH=/app:/app/config-dist:/app/signing

# The signed-config store + the signing home live under a persisted data dir so
# versions and the signing key survive restarts. Mount a volume at /data.
ENV CONFIG_DIST_STORE_ROOT=/data/store \
    CONFIG_DIST_SIGNING_HOME=/data/signing-home \
    CONFIG_DIST_PORT=8090

# Drop privileges — the service never needs root. The /data dir is created and
# owned by the service user so it can bootstrap the signing key + write versions.
RUN useradd --system --no-create-home --uid 10005 configdist \
    && mkdir -p /data \
    && chown -R configdist:configdist /app /data
USER configdist

EXPOSE 8090

# Serve the signed bundle; the agent's AGENT_CONFIG_URL points at
# http://config-dist:8090/config/<deployment_id> and verifies-before-apply (I6).
CMD ["python", "-m", "uvicorn", "config_service:app", \
     "--app-dir", "/app/config-dist", "--host", "0.0.0.0", "--port", "8090"]
