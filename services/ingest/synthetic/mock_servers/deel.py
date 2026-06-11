"""Local Deel REST v2 mock for running the real DeelClient in sandboxes."""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


DeelFixtures = dict[str, dict[str, Any]]

_CONTRACT_RE = re.compile(r"/contracts/([^/]+)$")


def _make_handler(fixtures: DeelFixtures, hits: dict[str, int]):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
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

            if path.endswith("/contracts"):
                hits["contracts"] = hits.get("contracts", 0) + 1
                self._json(200, _page(_contracts(), params))
                return
            m = _CONTRACT_RE.search(path)
            if m:
                cid = m.group(1)
                hits[f"contract:{cid}"] = hits.get(f"contract:{cid}", 0) + 1
                fx = fixtures.get(cid)
                if fx is None:
                    self._json(404, {"error": f"no contract {cid}"})
                    return
                self._json(200, {"data": fx.get("contract", {"id": cid})})
                return
            if path.endswith("/invoices"):
                hits["invoices"] = hits.get("invoices", 0) + 1
                self._json(200, _page(_invoices(params), params))
                return
            self._json(404, {"error": f"no GET route {path}"})

    def _contracts() -> list[dict[str, Any]]:
        return [
            dict(fx.get("contract", {"id": cid}))
            for cid, fx in fixtures.items()
        ]

    def _invoices(params: dict[str, str]) -> list[dict[str, Any]]:
        contract_id = params.get("contract_id")
        floor = params.get("created_after")
        out: list[dict[str, Any]] = []
        for cid, fx in fixtures.items():
            if contract_id and contract_id != cid:
                continue
            source_key = "delta" if floor and isinstance(fx.get("delta"), list) else "payments"
            for payment in fx.get(source_key, []):
                if not isinstance(payment, dict):
                    continue
                row = dict(payment)
                row.setdefault("contract_id", cid)
                if floor and _invoice_date(row) < floor[:10]:
                    continue
                out.append(row)
        return out

    return _Handler


def _page(items: list[dict[str, Any]], params: dict[str, str]) -> dict[str, Any]:
    try:
        limit = int(params.get("limit", "100") or "100")
    except ValueError:
        limit = 100
    try:
        offset = int(params.get("offset", "0") or "0")
    except ValueError:
        offset = 0
    page = items[offset:offset + limit]
    return {"data": page, "page": {"cursor": None, "total_rows": len(items)}}


def _invoice_date(invoice: dict[str, Any]) -> str:
    value = (
        invoice.get("createdAt")
        or invoice.get("created_at")
        or invoice.get("issued_at")
        or invoice.get("invoice_date")
        or ""
    )
    return value[:10] if isinstance(value, str) else ""


def start_mock_deel(
    fixtures: DeelFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["DeelFixtures", "start_mock_deel"]
