"""RequestContextMiddleware HTTP metrics — http_requests_total /
http_request_duration_seconds.

The middleware labels by the matched route TEMPLATE (never the raw
path, so `/v1/things/abc` lands under `/v1/things/{thing_id}`), and
collapses no-route 404 scans into route="unmatched".

The metric families live in the process-global lib.observability
registry, so every assertion is a before/after DELTA via the family's
`.get(**labels)` accessor — never an absolute value.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from lib.observability import counter, histogram
from services.app.gateway.middleware import RequestContextMiddleware


# Re-lookup of the families registered by the middleware module import:
# name + label set must match exactly.
def _requests():
    return counter(
        "http_requests_total", "lookup", ("method", "route", "status"),
    )


def _duration():
    return histogram(
        "http_request_duration_seconds", "lookup", ("method", "route"),
    )


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)

    @app.get("/v1/things/{thing_id}")
    def get_thing(thing_id: str) -> dict[str, str]:
        return {"id": thing_id}

    return TestClient(app)


def test_matched_route_uses_route_template_label(client: TestClient) -> None:
    labels = dict(method="GET", route="/v1/things/{thing_id}", status="200")
    duration_labels = dict(method="GET", route="/v1/things/{thing_id}")
    before = _requests().get(**labels)
    before_count = _duration().get_count(**duration_labels)

    resp = client.get("/v1/things/abc")

    assert resp.status_code == 200
    assert resp.json() == {"id": "abc"}
    # X-Request-Id proves the middleware actually ran on this path.
    assert resp.headers["X-Request-Id"]
    assert _requests().get(**labels) - before == 1
    assert _duration().get_count(**duration_labels) - before_count == 1
    # The raw path must NOT have become a label value (cardinality rule).
    assert (
        _requests().get(
            method="GET", route="/v1/things/abc", status="200",
        )
        == 0
    )


def test_unmatched_route_collapses_to_unmatched(client: TestClient) -> None:
    labels = dict(method="GET", route="unmatched", status="404")
    before = _requests().get(**labels)

    resp = client.get("/nope")

    assert resp.status_code == 404
    assert _requests().get(**labels) - before == 1
