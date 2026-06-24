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
COPY SPRINT_PLAN.md /app/SPRINT_PLAN.md
COPY console/ /app/console/

# Persistence lives under /app/console/data (bind a volume here in compose to
# survive restarts). Create it and drop privileges — the console never needs root.
RUN mkdir -p /app/console/data \
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
