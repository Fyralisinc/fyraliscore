# Fyralis license service — issue/validate signed licenses + manage the revocation list (P4).
#
# Build context is the CONTROL-PLANE ROOT (control-plane/), NOT licensing/, so the image
# can include the signing/ package the service imports at runtime (sign_bundle, verify_bundle,
# signing_lib) — license signing/verification is delegated to that real library (C2/I6).
#
#   docker build -f licensing/Dockerfile -t fyralis/license-service .
#
# In docker-compose this is wired with build.context: . and build.dockerfile:
# licensing/Dockerfile (see licensing/service.compose.yml).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Tight runtime surface: crypto (ed25519 signing), the API stack, and pydantic.
# cryptography ships manylinux wheels so no build toolchain is needed.
RUN pip install --no-cache-dir \
        "cryptography>=42.0" \
        "fastapi>=0.110" \
        "uvicorn[standard]>=0.29" \
        "pydantic>=2.0"

# The service imports signing/ at runtime (sign_bundle/verify_bundle/signing_lib).
# The PRIVATE signing key (signing/keys/, gitignored) is NOT baked into the image;
# it is mounted at runtime so the build artifact carries no secret material.
COPY signing/ /app/signing/
COPY licensing/ /app/licensing/

# Make the sibling packages importable the same way the modules do at top level.
ENV PYTHONPATH=/app/licensing:/app/signing

# Drop privileges — the service never needs root.
RUN useradd --system --no-create-home --uid 10002 license \
    && chown -R license:license /app
USER license

EXPOSE 8088

# The service mints with the trust root's active key (mounted at /app/signing) and
# reads/writes the revocation list at /app/licensing/revocations.json (mount a volume
# to persist it). Validation re-derives canonical bytes + verifies against the trust root.
CMD ["python", "-m", "uvicorn", "service:app", "--app-dir", "/app/licensing", \
     "--host", "0.0.0.0", "--port", "8088"]
