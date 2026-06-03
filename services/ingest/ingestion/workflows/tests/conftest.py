"""Shared fixtures for the workflow subprocess E2E tests.

M6.7 (A27.6): `shard_fetch`'s subprocess entrypoint writes each fetched
record to S3 before publishing (decision 1.3). The OAuth→completion
subprocess tests spawn the real `shard_fetch` (+ normalizer +
observation_writer) and therefore need a real S3 endpoint. `moto_s3_server`
runs a fake S3 (moto's HTTP server — the `[server]` extra) for the test
session and exports `S3_ENDPOINT_URL` / `S3_RAW_BUCKET` + dummy AWS creds
into the environment, which the spawned subprocesses inherit via
`os.environ.copy()`. Tests opt in with
`pytest.mark.usefixtures("moto_s3_server")`.
"""
from __future__ import annotations

import os
import socket

import pytest


_DEFAULT_PORT = 5500
_BUCKET = "test-raw-bucket"
_ENV_KEYS = (
    "S3_ENDPOINT_URL", "S3_RAW_BUCKET",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
)


def _pick_port(preferred: int) -> int:
    """Return `preferred` if free, else an ephemeral port. Keeps reruns
    robust if a prior session's server didn't release the port."""
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", preferred))
        return preferred
    except OSError:
        s2 = socket.socket()
        s2.bind(("127.0.0.1", 0))
        port = s2.getsockname()[1]
        s2.close()
        return port
    finally:
        s.close()


@pytest.fixture(scope="session")
def moto_s3_server():
    """Session-scoped fake S3 (moto HTTP server) for subprocess tests.

    Yields the endpoint URL. Sets S3 env vars for the session and
    restores them on teardown; creates the raw bucket up front. The
    spawned M6 subprocesses read these env vars at startup (A27.4).
    """
    import boto3
    from moto.server import ThreadedMotoServer

    prev = {k: os.environ.get(k) for k in _ENV_KEYS}
    port = _pick_port(_DEFAULT_PORT)
    endpoint = f"http://127.0.0.1:{port}"

    server = ThreadedMotoServer(port=port)
    server.start()
    try:
        os.environ["S3_ENDPOINT_URL"] = endpoint
        os.environ["S3_RAW_BUCKET"] = _BUCKET
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

        s3 = boto3.client(
            "s3", endpoint_url=endpoint, region_name="us-east-1",
            aws_access_key_id="testing", aws_secret_access_key="testing",
        )
        # Idempotent: a leftover bucket from a prior run is fine.
        try:
            s3.create_bucket(Bucket=_BUCKET)
        except Exception:  # noqa: BLE001 — already-exists is success
            pass

        yield endpoint
    finally:
        server.stop()
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
