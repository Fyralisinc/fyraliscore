# Fyralis BYOC — release registry (WS-RELEASE / P5). FastAPI/uvicorn over the
# on-disk signed-bundle registry; serves bundles the data-plane agent verifies
# before apply (I6). Port 8090 on cp-net.
#
# Build context is the CONTROL-PLANE ROOT (control-plane/), NOT release/, so the
# image can include control-plane/signing (verify-before-publish/serve) and
# control-plane/lib (RFC-3339 + C4 helpers the release code reuses):
#
#   docker build -f release/Dockerfile -t fyralis/release-registry .
#
# In docker-compose this is wired with build.context: .. and
# build.dockerfile: release/Dockerfile (see release/service.compose.yml, run as an
# overlay on docker-compose.control-plane.yml).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Runtime deps: FastAPI/uvicorn (serve) + cryptography (verify) + pydantic.
RUN pip install --no-cache-dir \
    "fastapi>=0.110" "uvicorn[standard]>=0.29" "cryptography>=42.0" "pydantic>=2.0"

# The release code reuses committed siblings at runtime:
#   signing/  -> sign_bundle / verify_bundle / signing_lib (ed25519, I6)
#   lib/      -> RFC-3339 + C4 helpers
#   release/  -> build_release / publish / rollout
# SPRINT_PLAN.md is copied so the root-anchor (_bootstrap walks up to the CP root)
# resolves inside the image.
COPY signing/ /app/signing/
COPY lib/ /app/lib/
COPY SPRINT_PLAN.md /app/SPRINT_PLAN.md
COPY release/ /app/release/

# The registry root holds published bundles + index.json (bind a volume here in
# compose to survive restarts). Drop privileges — serving never needs root.
RUN mkdir -p /app/release/_registry \
    && useradd --system --no-create-home --uid 10005 release \
    && chown -R release:release /app
USER release

EXPOSE 8090
ENV CP_RELEASE_REGISTRY=/app/release/_registry \
    CP_RELEASE_HOST=0.0.0.0 \
    CP_RELEASE_PORT=8090

WORKDIR /app/release
# Serve the registry. (build_release.py and rollout.py are run on demand as CD /
# operator CLIs inside this image, e.g. `docker run … python release/rollout.py …`.)
CMD ["python", "/app/release/publish.py", "serve", \
     "--registry", "/app/release/_registry", "--host", "0.0.0.0", "--port", "8090"]
