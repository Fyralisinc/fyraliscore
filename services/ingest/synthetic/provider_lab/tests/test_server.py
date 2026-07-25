from __future__ import annotations

import httpx

from services.ingest.synthetic.provider_lab.server import start_provider_lab


def test_loopback_server_serves_and_reseeds_canonical_adapter() -> None:
    first = {
        "acct-1": {
            "id": "acct-1",
            "type": "cash",
            "transactions": [{"id": "tx-1"}],
        }
    }
    server = start_provider_lab({"brex": [first]})
    try:
        response = httpx.get(
            server.url("brex", "/v2/accounts/cash"),
            headers={"Authorization": "Bearer lab"},
        )
        assert response.status_code == 200
        assert [item["id"] for item in response.json()["items"]] == ["acct-1"]
        assert server.request_count(
            source="brex",
            route_id="brex.cash_accounts",
        ) == 1

        second = {
            "acct-2": {
                "id": "acct-2",
                "type": "cash",
                "transactions": [{"id": "tx-2"}],
            }
        }
        server.replace_fixtures("brex", [second])
        response = httpx.get(
            server.url("brex", "/v2/accounts/cash"),
            headers={"Authorization": "Bearer lab"},
        )
        assert [item["id"] for item in response.json()["items"]] == ["acct-2"]
    finally:
        server.shutdown()


def test_loopback_server_rejects_non_loopback_bind() -> None:
    try:
        from services.ingest.synthetic.provider_lab.server import (
            ProviderLabServer,
        )

        ProviderLabServer(host="0.0.0.0")
    except ValueError as exc:
        assert "127.0.0.1" in str(exc)
    else:  # pragma: no cover - fail loudly if the production guard regresses
        raise AssertionError("non-loopback Provider Lab bind was accepted")
