# ============================================================================
# Fyralis BYOC — audit log + break-glass image (WS-AUDIT, FR-G / I5)
# ----------------------------------------------------------------------------
# A small image carrying the audit CLI (audit append/verify/list + break-glass
# request/approve/check/...) plus the committed signing/ primitives it reuses for
# the signed checkpoint (C2 / I6). It is NOT a long-running daemon: it is invoked
# on demand, e.g.
#
#   docker compose -f docker-compose.control-plane.yml \
#     run --rm audit audit verify
#   docker compose -f docker-compose.control-plane.yml \
#     run --rm audit breakglass request --actor sre@fyralis \
#         --scope tenant:acme/logs:read --ttl 900
#
# Build context is the control-plane/ ROOT (so signing/ is copied in); see
# service.compose.yml `build.context: ..`.
# ============================================================================
FROM python:3.12-slim

WORKDIR /app

# Only runtime dep is cryptography (ed25519 sign/verify for the signed checkpoint,
# via control-plane/signing). The hash chain itself is stdlib (hashlib/json).
RUN pip install --no-cache-dir "cryptography>=42" \
    && useradd --create-home --uid 10001 audit

# Copy the primitives this CLI imports plus the audit package itself.
# (Build context is control-plane/.)
COPY signing/ /app/signing/
COPY audit/   /app/audit/

ENV PYTHONUNBUFFERED=1

# The audit log + grant store live on a persisted volume in prod (see compose).
ENV AUDIT_LOG=/data/audit.log.jsonl
ENV AUDIT_STORE=/data/breakglass_grants.json

USER audit

# Default to the CLI help; compose `command`/`run` overrides this.
ENTRYPOINT ["python", "/app/audit/cli.py"]
CMD ["--help"]
