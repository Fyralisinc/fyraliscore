"""services/ingest/synthetic/mock_servers/carta.py — local Carta mock.

A real, threaded HTTP server that mimics the Carta Issuer **v1alpha1** REST
surface the ingestion path touches, so a sandbox can drive the REAL
CartaClient + fetcher + reconciler against it with no Carta credentials:

  GET /v1alpha1/issuers
      Issuers visible to the token -> {"issuers": [...]} (single fixture
      issuer; one page).
  GET /v1alpha1/issuers/{id}
      One issuer (visibility check) -> {"issuer": {...}} or 404.
  GET /v1alpha1/issuers/{id}/{stakeholders|shareClasses|optionGrants|
      convertibleNotes}
      AIP-158 list: honours `pageSize` (default 25, capped 100) + opaque
      `pageToken`; responds {"<collection>": [...], "nextPageToken": "..."}
      with nextPageToken OMITTED on the last page. `lastModifiedDatetimeAfter`
      is honoured ONLY for optionGrants (rows whose `lastModifiedDatetime.value`
      is strictly greater — see mock_clients/carta.py for the boundary
      TODO(human)).
  POST /o/access_token/
      OAuth client_credentials mint (docs.carta.com client-credentials flow):
      returns {"access_token", "expires_in", "scope", "token_type": "Bearer"}
      — NO refresh_token (Carta has no refresh grant).

Fixtures: the `make_carta` dict (`{"firm_id", "issuer", "entities":
{entity_type: [rows]}}`) — rows are raw v1alpha1 entities with protobuf
wrapper objects. The dict is read LIVE on every request, so a sandbox can
mutate a row in place (e.g. exercise an option grant) to simulate a poll-window
change.

Pointed at via `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under
`/carta`); handlers match path SUFFIXES so the prefix doesn't matter.

Usage:
    server, base_url = start_mock_carta(fixtures)
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


# The make_carta fixture dict: {"firm_id", "page_size"?, "issuer"?, "entities"}.
CartaFixtures = dict[str, Any]

# URL collection segment -> the fixture "entities" key (the shard taxonomy).
_COLLECTION_ENTITY_TYPES: dict[str, str] = {
    "stakeholders": "stakeholder",
    "shareClasses": "shareClass",
    "optionGrants": "optionGrant",
    "convertibleNotes": "convertibleNote",
}

_LIST_RE = re.compile(
    r"/v1alpha1/issuers/([^/]+)/"
    r"(stakeholders|shareClasses|optionGrants|convertibleNotes)$"
)
_GET_ISSUER_RE = re.compile(r"/v1alpha1/issuers/([^/]+)$")

_DEFAULT_PAGE_SIZE = 25
_MAX_PAGE_SIZE = 100


def _make_handler(fixtures: CartaFixtures, hits: dict[str, int]):
    def _issuer() -> dict[str, Any]:
        issuer = fixtures.get("issuer")
        if isinstance(issuer, dict) and issuer.get("id"):
            return issuer
        firm_id = str(fixtures.get("firm_id", ""))
        return {"id": firm_id, "legalName": "Sandbox Issuer"} if firm_id else {}

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

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            # OAuth client_credentials mint (trailing slash per docs).
            if path.rstrip("/").endswith("/o/access_token"):
                hits["token"] = hits.get("token", 0) + 1
                self._json(200, {
                    "access_token": "mock-carta-access-token",
                    "expires_in": 3600,
                    "scope": (
                        "read_issuer_info read_issuer_stakeholders "
                        "read_issuer_shareclasses read_issuer_securities"
                    ),
                    "token_type": "Bearer",
                })
                return
            self._json(404, {"error": f"no POST route {path}"})

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

            if not self.headers.get("Authorization"):
                self._json(401, {"error": "missing Authorization header"})
                return

            m = _LIST_RE.search(path)
            if m:
                self._handle_list(unquote(m.group(1)), m.group(2), params)
                return

            if path.endswith("/v1alpha1/issuers"):
                hits["issuers"] = hits.get("issuers", 0) + 1
                issuer = _issuer()
                self._json(200, {"issuers": [issuer] if issuer else []})
                return

            m = _GET_ISSUER_RE.search(path)
            if m:
                hits["get_issuer"] = hits.get("get_issuer", 0) + 1
                issuer = _issuer()
                if issuer and str(issuer.get("id")) == unquote(m.group(1)):
                    self._json(200, {"issuer": issuer})
                else:
                    self._json(404, {"error": "issuer not found"})
                return

            self._json(404, {"error": f"no GET route {path}"})

        def _handle_list(
            self, issuer_id: str, collection: str, params: dict[str, str],
        ) -> None:
            hits[f"list:{collection}"] = hits.get(f"list:{collection}", 0) + 1
            issuer = _issuer()
            if not issuer or str(issuer.get("id")) != issuer_id:
                self._json(404, {"error": "issuer not found"})
                return

            entity_type = _COLLECTION_ENTITY_TYPES[collection]
            rows = list(fixtures.get("entities", {}).get(entity_type, []))

            # optionGrants is the ONLY collection with the delta filter.
            bound = params.get("lastModifiedDatetimeAfter")
            if bound and collection == "optionGrants":
                rows = [
                    r for r in rows
                    if (_last_modified(r) or "") > bound
                ]

            try:
                requested = int(params.get("pageSize", _DEFAULT_PAGE_SIZE))
            except ValueError:
                requested = _DEFAULT_PAGE_SIZE
            # Values above the cap are coerced server-side (real behaviour).
            per_page = min(max(1, requested), _MAX_PAGE_SIZE)

            offset = _decode_token(params.get("pageToken"))
            if offset is None:
                self._json(400, {"error": "malformed pageToken"})
                return
            page = rows[offset:offset + per_page]
            end = offset + len(page)

            body: dict[str, Any] = {collection: page}
            # nextPageToken is OMITTED on the last page (AIP-158 terminal).
            if page and end < len(rows):
                body["nextPageToken"] = f"off:{end}"
            self._json(200, body)

    return _Handler


def _last_modified(row: dict[str, Any]) -> str | None:
    wrapper = row.get("lastModifiedDatetime")
    if isinstance(wrapper, dict):
        v = wrapper.get("value")
        return v if isinstance(v, str) else None
    return None


def _decode_token(token: str | None) -> int | None:
    if not token:
        return 0
    if token.startswith("off:"):
        try:
            return max(0, int(token[4:]))
        except ValueError:
            return None
    return None


def start_mock_carta(
    fixtures: CartaFixtures, *, host: str = "127.0.0.1", port: int = 0,
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


__all__ = ["CartaFixtures", "start_mock_carta"]
