# ============================================================================
# Fyralis BYOC Control Plane — metering job image (WS-METER)
# ----------------------------------------------------------------------------
# IMPORTANT — the build context MUST be the control-plane ROOT (.), not metering/,
#   so the image can include:
#     - signing/   (sign_bundle / verify_bundle / signing_lib)  -> sign + verify
#                  the usage rollup (C2 / I6 / FR-F2 tamper-evidence)
#     - metering/  (mimir_client / rollup / export / selftest)  -> this workstream
#
# The metering job READS aggregate Tier-1 metrics from Mimir (X-Scope-OrgID per
# tenant) and WRITES a SIGNED per-tenant usage rollup. It is a CONTROL-PLANE batch
# job (run on a schedule / on demand); it never dials a data plane (I2).
# ============================================================================
FROM python:3.12-slim

WORKDIR /app

# Only the deps metering needs: httpx (Mimir query client) + cryptography (signing).
RUN pip install --no-cache-dir "httpx>=0.27" "cryptography>=42"

# Bring in the signing lib (the dependency) + the metering code.
COPY signing/ /app/signing/
COPY metering/ /app/metering/

# Put both dirs on the path the same way the CLIs do (flat imports: signing_lib,
# mimir_client, rollup, export).
ENV PYTHONPATH=/app/metering:/app/signing

# Default to the self-test (a real, offline proof the image works end to end).
# Override `command:` in compose to run rollup.py / export.py for real tenants.
CMD ["python", "/app/metering/selftest.py"]
