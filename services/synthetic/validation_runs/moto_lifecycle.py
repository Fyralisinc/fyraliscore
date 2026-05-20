"""Runner-owned moto S3 server (A29 / Decision 9).

The validation runner is standalone Python, not pytest — so it can't
borrow the `moto_s3_server` pytest fixture. It owns the fake-S3 server's
lifecycle itself: spawn a `ThreadedMotoServer` at startup, export the S3
env vars the spawned M6 subprocesses read (`S3_ENDPOINT_URL` /
`S3_RAW_BUCKET` + dummy AWS creds), create the raw bucket, and tear the
server down at the end. One-command invocation brings up everything; the
operator doesn't have to remember to start S3 first.

Mirrors `services/ingestion/workflows/tests/conftest.py::moto_s3_server`
(same `_pick_port` ephemeral-fallback discipline so reruns don't collide
on a port a prior session didn't release).
"""
from __future__ import annotations

import contextlib
import logging
import os
import socket
from collections.abc import Iterator

import boto3
from moto.server import ThreadedMotoServer


log = logging.getLogger(__name__)

_DEFAULT_PORT = 5600
_BUCKET = "fyralis-raw"
_ENV_KEYS = (
    "S3_ENDPOINT_URL", "S3_RAW_BUCKET",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
)


def _pick_port(preferred: int) -> int:
    """Return `preferred` if free, else an ephemeral port."""
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


@contextlib.contextmanager
def moto_s3(bucket: str = _BUCKET) -> Iterator[str]:
    """Spawn a moto S3 server for the run's duration.

    Yields the endpoint URL. Sets the S3 env vars (inherited by the
    harness subprocesses via `os.environ.copy()`) and the raw bucket;
    restores the prior env + stops the server on exit. Idempotent on the
    bucket (a leftover from a prior run is fine).
    """
    prev = {k: os.environ.get(k) for k in _ENV_KEYS}
    port = _pick_port(_DEFAULT_PORT)
    endpoint = f"http://127.0.0.1:{port}"

    server = ThreadedMotoServer(port=port)
    server.start()
    log.info("validation.moto: started at %s", endpoint)
    try:
        os.environ["S3_ENDPOINT_URL"] = endpoint
        os.environ["S3_RAW_BUCKET"] = bucket
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

        s3 = boto3.client(
            "s3", endpoint_url=endpoint, region_name="us-east-1",
            aws_access_key_id="testing", aws_secret_access_key="testing",
        )
        with contextlib.suppress(Exception):
            s3.create_bucket(Bucket=bucket)

        yield endpoint
    finally:
        server.stop()
        log.info("validation.moto: stopped")
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
