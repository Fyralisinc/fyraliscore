# Fyralis tenant auth proxy — mTLS termination + X-Scope-OrgID injection (P2).
#
# Build context is the CONTROL-PLANE ROOT (control-plane/), NOT auth-proxy/, so
# the image can include the ca/ modules the proxy imports at runtime (ca_lib,
# registry, verify_chain) and the tenant_registry.json it consults.
#
#   docker build -f auth-proxy/Dockerfile -t fyralis/auth-proxy .
#
# In docker-compose this is wired with build.context: . and build.dockerfile:
# auth-proxy/Dockerfile (see docker-compose.control-plane.yml).
FROM python:3.12-slim

# Minimal, security-relevant only. cryptography ships manylinux wheels so no
# build toolchain is needed.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Just the libraries the proxy actually needs at runtime (kept tight on purpose;
# the proxy must not drag in the whole control-plane dependency surface).
RUN pip install --no-cache-dir \
        "cryptography>=42.0" \
        "httpx>=0.27" \
        "h11>=0.14"

# The proxy reads ca/ca_lib.py, ca/registry.py, ca/verify_chain.py at runtime,
# and consults ca/tenant_registry.json. Copy the ca/ package and the proxy code.
# (lib/ is NOT required by the proxy runtime — it reuses ca/ directly.)
COPY ca/ /app/ca/
COPY auth-proxy/ /app/auth-proxy/

# Drop privileges — the proxy never needs root.
RUN useradd --system --no-create-home --uid 10001 proxy \
    && chown -R proxy:proxy /app
USER proxy

# mTLS listener (matches DEFAULT_LISTEN_PORT).
EXPOSE 8443

# Defaults; override via compose env. The CA chain + registry default to the
# in-image ca/ copy, but in compose they are bind-mounted from the live ca/ so
# revocations take effect without a rebuild.
ENV AUTH_PROXY_CA_CHAIN=/app/ca/pki/ca-chain.crt \
    AUTH_PROXY_TENANT_REGISTRY=/app/ca/tenant_registry.json \
    AUTH_PROXY_LISTEN_HOST=0.0.0.0 \
    AUTH_PROXY_LISTEN_PORT=8443 \
    AUTH_PROXY_UPSTREAM_URL=http://mimir:9009

# TLS server cert/key MUST be provided (mounted) at runtime:
#   AUTH_PROXY_TLS_CERT / AUTH_PROXY_TLS_KEY
# (mint with auth-proxy/gen_server_cert.py).

CMD ["python", "/app/auth-proxy/proxy.py"]
