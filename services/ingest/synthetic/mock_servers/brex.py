"""services/ingest/synthetic/mock_servers/brex.py — local Brex banking REST mock.

A real, threaded HTTP server that mimics the Brex v1 endpoints the ingestion
path touches, so a sandbox can drive the REAL BrexClient + fetcher +
reconciler against it with no Brex credentials:

  GET /accounts
      All accounts visible to the token (seed-time enumeration + balances).
  GET /account/{id}
      One account (the fetcher's balance-snapshot probe).
  GET /account/{id}/transactions?limit&offset&start
      Paginated transactions. Two modes:
        - full  (no `start`)              -> all transactions for the account.
        - incremental (`start=<date>`)    -> the account's delta transactions.

Fixtures: {account_id: {"account": {...}, "transactions": [...], "delta": [...]}}
where each transaction is a raw Brex transaction object. The mock does not
synthesize them so the sandbox controls exactly what lands.

The client is pointed here via the spammer single-host base
(`SYNTHETIC_SOURCE_API_BASE=<base>` -> `<base>/brex`); the handler matches on
the `/accounts` / `/account/...` path SUFFIX so the prefix doesn't matter.

Usage:
    server, base_url = start_mock_brex(fixtures)
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


BrexFixtures = dict[str, dict[str, Any]]

_ACCOUNT_TXNS_RE = re.compile(r"/account/([^/]+)/transactions$")
_ACCOUNT_RE = re.compile(r"/account/([^/]+)$")


def _make_handler(fixtures: BrexFixtures, hits: dict[str, int]):
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

            m = _ACCOUNT_TXNS_RE.search(path)
            if m:
                self._handle_transactions(m.group(1), params)
                return
            if path.endswith("/accounts"):
                hits["accounts"] = hits.get("accounts", 0) + 1
                accounts = [
                    fx.get("account", {"id": aid})
                    for aid, fx in fixtures.items()
                ]
                self._json(200, {"accounts": accounts, "total": len(accounts)})
                return
            m = _ACCOUNT_RE.search(path)
            if m:
                aid = m.group(1)
                hits[f"account:{aid}"] = hits.get(f"account:{aid}", 0) + 1
                fx = fixtures.get(aid)
                if fx is None:
                    self._json(404, {"error": f"no account {aid}"})
                    return
                self._json(200, fx.get("account", {"id": aid}))
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_transactions(self, account_id: str, params: dict[str, str]) -> None:
            hits[f"txns:{account_id}"] = hits.get(f"txns:{account_id}", 0) + 1
            fx = fixtures.get(account_id)
            if fx is None:
                self._json(200, {"transactions": [], "total": 0})
                return
            incremental = bool(params.get("start"))
            pool = list(fx.get("delta", [])) if incremental else list(fx.get("transactions", []))
            try:
                limit = int(params.get("limit", "100") or "100")
            except ValueError:
                limit = 100
            try:
                offset = int(params.get("offset", "0") or "0")
            except ValueError:
                offset = 0
            page = pool[offset:offset + limit]
            self._json(200, {"transactions": page, "total": len(pool)})

    return _Handler


def start_mock_brex(
    fixtures: BrexFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url)`. Point the client at `base_url` via
    `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under `/brex`). Call
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


__all__ = ["BrexFixtures", "start_mock_brex"]
