# Fyralis BYOC data-plane AGENT — outbound-only heartbeat daemon (WS-AGENT).
#
# This image runs IN THE CUSTOMER VPC. It reaches OUT to the vendor console
# (POST /api/v1/heartbeat) and pulls signed config/release bundles; it NEVER
# opens a listening socket (I2). There is therefore NO EXPOSE line, by design —
# nothing reaches in to the agent.
#
# Build context is the CONTROL-PLANE ROOT (control-plane/), NOT agent/, so the
# image can include the sibling packages the agent imports at runtime:
#   - signing/   (verify_bundle / signing_lib)  -> verify-before-apply (I6)
#   - lib/       (DeploymentRecord, tiers, ...)  -> the C4 record + health (C4)
#
#   docker build -f agent/Dockerfile -t fyralis/agent .
#
# In docker-compose this is wired with build.context: . and build.dockerfile:
# agent/Dockerfile (see agent/service.compose.yml).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Tight runtime surface: crypto (ed25519 verify), pydantic (the C4 record), and
# requests (the ONE outbound https client). No server framework is installed —
# the agent has no inbound listener (I2). cryptography ships manylinux wheels.
RUN pip install --no-cache-dir \
        "cryptography>=42.0" \
        "pydantic>=2.0" \
        "requests>=2.31" \
        "structlog>=24.1"
# (control-plane/lib/__init__.py configures structured logging on import, so the
# agent image — which imports lib — needs structlog at runtime.)

# The agent imports signing/ + lib/ at runtime; copy them plus the agent itself.
# The trust root (public keys only) is copied so the agent can verify bundles;
# the PRIVATE signing key is NEVER in this image (the agent only verifies).
COPY lib/      /app/lib/
COPY signing/  /app/signing/
COPY agent/    /app/agent/

# Make the sibling packages importable exactly as the modules do at top level:
# control-plane root (for `import lib`), the signing dir (for `import signing_lib`),
# and the agent dir (for `import agent`/`config`/...).
ENV PYTHONPATH=/app:/app/signing:/app/agent

# Drop privileges — the agent never needs root and never binds a port.
# /var/lib/fyralis-agent is the compose `agent-state` volume mountpoint (durable
# heartbeat buffer for I3 + the reconcile applied-config dir). Create it OWNED BY
# the agent uid in the image so a fresh named volume inherits writable ownership
# (otherwise docker creates the mountpoint root-owned and uid 10010 can't write).
RUN useradd --system --no-create-home --uid 10010 agent \
    && mkdir -p /app/agent/applied-config /var/lib/fyralis-agent/applied-config \
    && chown -R agent:agent /app /var/lib/fyralis-agent
USER agent

WORKDIR /app/agent

# The license bundle + trust root are mounted at runtime (see service.compose.yml);
# the buffer + applied-config live under /app/agent (mount a volume to persist
# the buffer across restarts so an offline backlog survives — I3).
CMD ["python", "agent.py"]
