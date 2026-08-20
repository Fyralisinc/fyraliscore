# Fyralis BYOC — operator fleet console (P4). FastAPI/uvicorn over the C4 fleet
# registry, port 8080 on cp-net.
#
# Build context is the CONTROL-PLANE ROOT (control-plane/), NOT console/, so the
# image can include control-plane/lib (the shared C4 DeploymentRecord + health
# math the console imports as `import lib...`):
#
#   docker build -f console/Dockerfile -t fyralis/console .
#
# In docker-compose this is wired with build.context: . and
# build.dockerfile: console/Dockerfile (see console/service.compose.yml).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Runtime deps only (the console is a thin HTTP/HTML layer).
COPY console/requirements.txt /app/console/requirements.txt
RUN pip install --no-cache-dir -r /app/console/requirements.txt

# The console imports control-plane/lib at runtime (DeploymentRecord + health
# derivation). Copy lib/ and the console code. SPRINT_PLAN.md is copied too so
# the root-anchor in app.py/store.py resolves the control-plane root inside the
# image (it walks up looking for SPRINT_PLAN.md / lib/deployment.py).
COPY lib/ /app/lib/
# The control surface SIGNS operator-pushed desired config/release (deps.signer,
# I6) and APPENDS to the hash-chained audit log (deps.audit, I5), so the image
# also needs the signing/ + audit/ primitives (same as the audit image). The
# gitignored private key + trust root are NOT baked (see .dockerignore) — they are
# bind-mounted read-only at runtime (compose).
COPY signing/ /app/signing/
COPY audit/ /app/audit/
# The D1 metering feature router imports the metering engine (mimir_client +
# rollup) to render per-tenant usage.
COPY metering/ /app/metering/
COPY SPRINT_PLAN.md /app/SPRINT_PLAN.md
COPY console/ /app/console/

# Persistence: /app/console/data (registry + desired/applied sidecars) and
# /app/console/audit-data (the hash-chained audit log; see CONSOLE_AUDIT_LOG).
# Bind volumes here in compose to survive restarts. Create them + drop privileges.
RUN mkdir -p /app/console/data /app/console/audit-data \
    && useradd --system --no-create-home --uid 10002 console \
    && chown -R console:console /app
USER console

# C4 / P4 contract: the console listens on 8080 (cp-net).
EXPOSE 8080
ENV CP_CONSOLE_HOST=0.0.0.0 \
    CP_CONSOLE_PORT=8080 \
    CP_CONSOLE_DATA_DIR=/app/console/data

# uvicorn entrypoint (app.py runs uvicorn.run on CP_CONSOLE_PORT under __main__).
WORKDIR /app/console
CMD ["python", "/app/console/app.py"]
