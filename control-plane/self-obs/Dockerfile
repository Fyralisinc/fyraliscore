# =============================================================================
# control-plane / self-obs / Dockerfile  —  cp-self-obs exporter (WS-SELFOBS)
# -----------------------------------------------------------------------------
# Build context is THIS dir (control-plane/self-obs). The exporter is self-
# contained — pure stdlib + prometheus_client — so unlike the console/agent
# images it does NOT need the control-plane root context. This keeps WS-SELFOBS
# dir-disjoint: the image carries nothing from a sibling service dir.
#
#   docker build -t fyralis/cp-self-obs ./control-plane/self-obs
# =============================================================================
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CP_SELFOBS_LISTEN_HOST=0.0.0.0 \
    CP_SELFOBS_LISTEN_PORT=9110

WORKDIR /app

# Deps first for layer caching.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# The exporter + (for image-side self-test convenience) the self-test.
COPY cp_exporter.py /app/cp_exporter.py
COPY selftest.py /app/selftest.py

# Run as a non-root user (the exporter opens no privileged ports).
RUN useradd --create-home --uid 10001 selfobs
USER 10001:10001

EXPOSE 9110

# python:3.12-slim has no curl; probe our own /metrics with the stdlib.
HEALTHCHECK --interval=20s --timeout=4s --retries=3 --start-period=10s \
  CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9110/metrics', timeout=3).status==200 else 1)"]

ENTRYPOINT ["python", "/app/cp_exporter.py"]
