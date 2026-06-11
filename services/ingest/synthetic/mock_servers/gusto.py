"""services/ingest/synthetic/mock_servers/gusto.py — local Gusto mock.

A real, threaded HTTP server that mimics the REAL Gusto `/v1` read surface
(VERIFIED against docs.gusto.com) the ingestion path touches, so a sandbox can
drive the REAL GustoClient + fetcher + reconciler against it with no Gusto
credentials:

  GET .../v1/companies/{company_uuid}/employees?page=&per=
      Bare JSON array of employee objects. Paginated by `page`/`per`; the
      response carries the real count headers `X-Total-Count` / `X-Page` /
      `X-Per-Page` / `X-Total-Pages`.
  GET .../v1/companies/{company_uuid}/payrolls?page=&per=&start_date=&end_date=
      Bare JSON array of payroll objects, same pagination headers. The
      inclusive day-granular `start_date`/`end_date` window filters on
      `check_date` (the sandbox always drives `date_filter_by=check_date`).
  GET .../v1/companies/{company_uuid}
      Connectivity probe — single company object.

Fixtures: {entity_kind: [rows]} where each row is a raw REAL-shaped Gusto
object (employee: uuid/version/first_name/...; payroll: payroll_uuid/
check_date/processed/totals/...). The dict is held by reference — a sandbox
can mutate the lists between phases (append a payroll, bump an employee
version) to simulate live drift. An optional "company" key overrides the
probe body.

Pointed at via `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under
`/gusto`); the handler matches the `/v1/companies/...` path SUFFIX so the
prefix doesn't matter.

Usage:
    server, base_url = start_mock_gusto(fixtures)
"""
from __future__ import annotations

import json
import math
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


GustoFixtures = dict[str, Any]

_LIST_RE = re.compile(r"/v1/companies/([^/]+)/(employees|payrolls)$")
_COMPANY_RE = re.compile(r"/v1/companies/([^/]+)$")

# Wire entity segment -> fixture key (singular taxonomy).
_SEGMENT_TO_KIND = {"employees": "employee", "payrolls": "payroll"}


def _make_handler(fixtures: GustoFixtures, hits: dict[str, int]):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # noqa: D401
            return

        def _json(
            self, status: int, body: Any,
            extra_headers: dict[str, str] | None = None,
        ) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            for k, v in (extra_headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            m = _LIST_RE.search(path)
            if m:
                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                self._handle_list(m.group(2), params)
                return
            m = _COMPANY_RE.search(path)
            if m:
                hits["company"] = hits.get("company", 0) + 1
                company = fixtures.get("company")
                if not isinstance(company, dict):
                    company = {
                        "uuid": m.group(1),
                        "name": "Sandbox Co",
                        "company_status": "Approved",
                    }
                self._json(200, company)
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_list(self, segment: str, params: dict[str, str]) -> None:
            kind = _SEGMENT_TO_KIND[segment]
            hits[f"list:{kind}"] = hits.get(f"list:{kind}", 0) + 1
            pool = fixtures.get(kind)
            rows = list(pool) if isinstance(pool, list) else []

            if kind == "payroll":
                # Inclusive day-granular check_date window (the sandbox always
                # sends date_filter_by=check_date with a date bound).
                start_date = params.get("start_date")
                end_date = params.get("end_date")
                if start_date:
                    rows = [r for r in rows
                            if (r.get("check_date") or "") >= start_date]
                if end_date:
                    rows = [r for r in rows
                            if (r.get("check_date") or "") <= end_date]

            # docs.gusto.com pagination: page/per params, per default 25.
            try:
                page = max(1, int(params.get("page", "1")))
            except ValueError:
                page = 1
            try:
                per = max(1, min(100, int(params.get("per", "25"))))
            except ValueError:
                per = 25

            total = len(rows)
            page_rows = rows[(page - 1) * per: page * per]
            self._json(200, page_rows, extra_headers={
                "X-Total-Count": str(total),
                "X-Page": str(page),
                "X-Per-Page": str(per),
                "X-Total-Pages": str(max(1, math.ceil(total / per))),
            })

    return _Handler


def start_mock_gusto(
    fixtures: GustoFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread. Returns `(server, base_url)`."""
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["GustoFixtures", "start_mock_gusto"]
