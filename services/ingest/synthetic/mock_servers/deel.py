"""services/ingest/synthetic/mock_servers/deel.py — local Deel REST mock.

A real, threaded HTTP server that mimics the Deel v1 endpoints the ingestion
path touches, so a sandbox can drive the REAL DeelClient + fetcher +
reconciler against it with no Deel credentials:

  GET /contracts
      All contracts visible to the token (seed-time enumeration).
  GET /contract/{id}
      One contract (the fetcher's state-snapshot probe).
  GET /contract/{id}/payments?limit&offset&start
      Paginated payments. Two modes:
        - full  (no `start`)              -> all payments for the contract.
        - incremental (`start=<date>`)    -> the contract's delta payments.

Fixtures: {contract_id: {"contract": {...}, "payments": [...], "delta": [...]}}
where each payment is a raw Deel payment object. The mock does not
synthesize them so the sandbox controls exactly what lands.

The client is pointed here via the spammer single-host base
(`SYNTHETIC_SOURCE_API_BASE=<base>` -> `<base>/deel`); the handler matches on
the `/contracts` / `/contract/...` path SUFFIX so the prefix doesn't matter.

Usage:
    server, base_url = start_mock_deel(fixtures)
    try:
        ...  # base_url -> SYNTHETIC_SOURCE_API_BASE
    finally:
        server.shutdown()
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


DeelFixtures = dict[str, dict[str, Any]]

_CONTRACT_PAYMENTS_RE = re.compile(r"/contract/([^/]+)/payments$")
_CONTRACT_RE = re.compile(r"/contract/([^/]+)$")


def _make_handler(fixtures: DeelFixtures, hits: dict[str, int]):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # noqa: D401
            return

        def _json(self, status: int, body: Any) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

            m = _CONTRACT_PAYMENTS_RE.search(path)
            if m:
                self._handle_payments(m.group(1), params)
                return
            if path.endswith("/contracts"):
                hits["contracts"] = hits.get("contracts", 0) + 1
                contracts = [
                    fx.get("contract", {"id": cid})
                    for cid, fx in fixtures.items()
                ]
                self._json(200, {"contracts": contracts, "total": len(contracts)})
                return
            m = _CONTRACT_RE.search(path)
            if m:
                cid = m.group(1)
                hits[f"contract:{cid}"] = hits.get(f"contract:{cid}", 0) + 1
                fx = fixtures.get(cid)
                if fx is None:
                    self._json(404, {"error": f"no contract {cid}"})
                    return
                self._json(200, fx.get("contract", {"id": cid}))
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_payments(self, contract_id: str, params: dict[str, str]) -> None:
            hits[f"payments:{contract_id}"] = hits.get(f"payments:{contract_id}", 0) + 1
            fx = fixtures.get(contract_id)
            if fx is None:
                self._json(200, {"payments": [], "total": 0})
                return
            incremental = bool(params.get("start"))
            pool = list(fx.get("delta", [])) if incremental else list(fx.get("payments", []))
            try:
                limit = int(params.get("limit", "100") or "100")
            except ValueError:
                limit = 100
            try:
                offset = int(params.get("offset", "0") or "0")
            except ValueError:
                offset = 0
            page = pool[offset:offset + limit]
            self._json(200, {"payments": page, "total": len(pool)})

    return _Handler


def start_mock_deel(
    fixtures: DeelFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url)`. Point the client at `base_url` via
    `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under `/deel`). Call
    `server.shutdown()` to stop.
    """
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["DeelFixtures", "start_mock_deel"]
