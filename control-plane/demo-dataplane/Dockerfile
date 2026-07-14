# demo-dataplane — tiny golden-12 SLI metrics stub for the testable bring-up.
#
# Build context is THIS dir (self-contained, stdlib-only — no pip install).
#   docker build -f demo-dataplane/Dockerfile -t fyralis/demo-dataplane demo-dataplane
#
# This is a DEMO fixture, not the real data plane. In production the installer
# points the boundary collector at the real data-plane targets.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEMO_DP_HOST=0.0.0.0 \
    DEMO_DP_PORT=9300

WORKDIR /app
COPY metrics_stub.py /app/metrics_stub.py

# Drop privileges — the stub never needs root.
RUN useradd --system --no-create-home --uid 10002 demo \
    && chown -R demo:demo /app
USER demo

EXPOSE 9300

HEALTHCHECK --interval=20s --timeout=4s --retries=3 --start-period=5s \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9300/healthz',timeout=3).status==200 else 1)"]

CMD ["python", "/app/metrics_stub.py"]
