"""Local Brex v2 REST mock for running the real BrexClient in sandboxes."""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


BrexFixtures = dict[str, dict[str, Any]]

_CASH_TXNS_RE = re.compile(r"/v2/transactions/cash/([^/]+)$")


def _make_handler(fixtures: BrexFixtures, hits: dict[str, int]):
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

            if path.endswith("/v2/accounts/cash"):
                hits["accounts:cash"] = hits.get("accounts:cash", 0) + 1
                self._json(200, _account_page("cash", params))
                return
            if path.endswith("/v2/accounts/card"):
                hits["accounts:card"] = hits.get("accounts:card", 0) + 1
                self._json(200, {"items": _accounts("card")})
                return
            m = _CASH_TXNS_RE.search(path)
            if m:
                self._handle_transactions(m.group(1), params)
                return
            if path.endswith("/v2/transactions/card/primary"):
                self._handle_card_transactions(params)
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_transactions(self, account_id: str, params: dict[str, str]) -> None:
            hits[f"txns:cash:{account_id}"] = hits.get(f"txns:cash:{account_id}", 0) + 1
            fx = fixtures.get(account_id)
            if fx is None:
                pool = []
            elif params.get("posted_at_start") and isinstance(fx.get("delta"), list):
                pool = list(fx.get("delta", []))
            else:
                pool = list(fx.get("transactions", []))
            self._json(200, _txn_page(pool, params))

        def _handle_card_transactions(self, params: dict[str, str]) -> None:
            hits["txns:card:primary"] = hits.get("txns:card:primary", 0) + 1
            pool: list[dict[str, Any]] = []
            use_delta = bool(params.get("posted_at_start"))
            for aid, fx in fixtures.items():
                acct = dict(fx.get("account", {"id": aid}))
                acct_kind = str(
                    acct.get("_fyralis_account_kind")
                    or acct.get("type")
                    or acct.get("kind")
                    or "cash"
                ).lower()
                if acct_kind not in {"card", "credit_card", "primary_card"}:
                    continue
                txns = fx.get("delta") if use_delta and isinstance(fx.get("delta"), list) else fx.get("transactions")
                if isinstance(txns, list):
                    pool.extend(x for x in txns if isinstance(x, dict))
            self._json(200, _txn_page(pool, params))

    def _accounts(kind: str, *, include_txns: bool = False) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for aid, fx in fixtures.items():
            acct = dict(fx.get("account", {"id": aid}))
            acct_kind = str(
                acct.get("_fyralis_account_kind")
                or acct.get("type")
                or acct.get("kind")
                or "cash"
            ).lower()
            is_card = acct_kind in {"card", "credit_card", "primary_card"}
            if (kind == "card") != is_card:
                continue
            acct.setdefault("id", aid)
            acct.setdefault("_fyralis_account_kind", "card" if is_card else "cash")
            if include_txns and isinstance(fx.get("transactions"), list):
                acct["transactions"] = fx["transactions"]
            elif "transactions" in acct:
                acct.pop("transactions", None)
            out.append(acct)
        return out

    def _account_page(kind: str, params: dict[str, str]) -> dict[str, Any]:
        items = _accounts(kind)
        page, next_cursor = _paginate(items, params, default_limit=1000)
        return {"items": page, "next_cursor": next_cursor}

    return _Handler


def _txn_page(pool: list[dict[str, Any]], params: dict[str, str]) -> dict[str, Any]:
    floor = params.get("posted_at_start")
    if floor:
        pool = [txn for txn in pool if _txn_date(txn) >= floor[:10]]
    page, next_cursor = _paginate(pool, params, default_limit=100)
    return {"items": page, "next_cursor": next_cursor}


def _paginate(
    items: list[dict[str, Any]], params: dict[str, str], *, default_limit: int,
) -> tuple[list[dict[str, Any]], str | None]:
    try:
        limit = int(params.get("limit", str(default_limit)) or str(default_limit))
    except ValueError:
        limit = default_limit
    try:
        offset = int(params.get("cursor", "0") or "0")
    except ValueError:
        offset = 0
    page = items[offset:offset + limit]
    next_offset = offset + len(page)
    return page, (str(next_offset) if page and next_offset < len(items) else None)


def _txn_date(txn: dict[str, Any]) -> str:
    iso = (
        txn.get("postedAt")
        or txn.get("posted_at")
        or txn.get("createdAt")
        or txn.get("created_at")
        or ""
    )
    return iso[:10] if isinstance(iso, str) else ""


def start_mock_brex(
    fixtures: BrexFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["BrexFixtures", "start_mock_brex"]
