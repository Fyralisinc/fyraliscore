"""Virtual-clock renewal behavior for the eight R2 Provider Lab sources."""
from __future__ import annotations

import base64
import json
from typing import Any

import httpx
import pytest

from services.ingest.synthetic.provider_lab import build_provider_lab_app


def _transport(app: Any) -> httpx.ASGITransport:
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43127))


def _jwt_for(subject: str) -> str:
    def segment(value: dict[str, str]) -> str:
        return base64.urlsafe_b64encode(
            json.dumps(value, separators=(",", ":")).encode("utf-8")
        ).decode("ascii").rstrip("=")

    return f"{segment({'alg': 'none'})}.{segment({'sub': subject})}.signature"


def _lifecycle_state(
    *,
    initial_refresh_token: str | None = None,
) -> dict[str, Any]:
    lifecycle: dict[str, Any] = {
        "enabled": True,
        "access_ttl_seconds": 5,
        "refresh_ttl_seconds": 60,
        "watch_ttl_seconds": 5,
    }
    if initial_refresh_token is not None:
        lifecycle.update(
            {
                "initial_refresh_token": initial_refresh_token,
                "initial_refresh_expires_at": "2025-01-01T00:00:05Z",
            }
        )
    return {"renewal_lifecycle": lifecycle}


_WATCH_CASES = (
    (
        "gmail",
        "/gmail/token",
        "/gmail/gmail/v1/users/me/watch",
        {"assertion": _jwt_for("renewal-a@provider-lab.test")},
        {"topicName": "projects/provider-lab/topics/gmail"},
        "historyId",
    ),
    (
        "google_calendar",
        "/gcal/token",
        "/gcal/calendar/v3/calendars/renewal%40provider-lab.test/events/watch",
        {},
        {"id": "calendar-renewal-channel"},
        "resourceId",
    ),
    (
        "google_drive",
        "/gdrive/token",
        "/gdrive/drive/v3/changes/watch",
        {},
        {"id": "drive-renewal-channel"},
        "resourceId",
    ),
)


def _watch_body(source: str, channel_id: str) -> dict[str, str]:
    if source == "gmail":
        return {"topicName": "projects/provider-lab/topics/gmail"}
    return {
        "id": channel_id,
        "type": "web_hook",
        "address": "https://ingest.provider-lab.test/push",
        "token": "provider-lab-watch-token",
    }


async def _watch_lifecycle(
    client: httpx.AsyncClient,
    *,
    source: str,
    scope: str,
) -> dict[str, Any]:
    response = await client.get(
        f"/_lab/sources/{source}/watch-lifecycle",
        params={"scope": scope},
    )
    assert response.status_code == 200
    return response.json()


@pytest.mark.parametrize(
    "source,token_path,watch_path,token_data,watch_body,dynamic_field",
    _WATCH_CASES,
)
async def test_watch_renewals_are_virtual_time_and_scope_aware(
    source: str,
    token_path: str,
    watch_path: str,
    token_data: dict[str, str],
    watch_body: dict[str, str],
    dynamic_field: str,
) -> None:
    app = build_provider_lab_app()
    scope_a = f"{source}-scope-a"
    scope_b = f"{source}-scope-b"
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        configured = await client.put(
            f"/_lab/sources/{source}/state",
            json=_lifecycle_state(),
        )
        assert configured.status_code == 200

        token_a = await client.post(
            token_path,
            data=token_data,
            headers={"X-Provider-Lab-Scope": scope_a},
        )
        token_b = await client.post(
            token_path,
            data=token_data,
            headers={"X-Provider-Lab-Scope": scope_b},
        )
        assert token_a.status_code == token_b.status_code == 200
        access_a = token_a.json()["access_token"]
        assert access_a != token_b.json()["access_token"]

        first = await client.post(
            watch_path,
            json=watch_body,
            headers={
                "Authorization": f"Bearer {access_a}",
                "X-Provider-Lab-Scope": scope_a,
            },
        )
        assert first.status_code == 200
        assert await client.post("/_lab/clock/advance", json={"seconds": 6})
        renewed = await client.post(
            token_path,
            data=token_data,
            headers={"X-Provider-Lab-Scope": scope_a},
        )
        assert renewed.status_code == 200
        second = await client.post(
            watch_path,
            json=watch_body,
            headers={
                "Authorization": f"Bearer {renewed.json()['access_token']}",
                "X-Provider-Lab-Scope": scope_a,
            },
        )

    assert second.status_code == 200
    assert int(second.json()["expiration"]) - int(first.json()["expiration"]) == 6000
    assert second.json()[dynamic_field] != first.json()[dynamic_field]


@pytest.mark.parametrize(
    "source,token_path,watch_path,token_data,stop_path",
    (
        (
            "gmail",
            "/gmail/token",
            "/gmail/gmail/v1/users/me/watch",
            {"assertion": _jwt_for("renewal-a@provider-lab.test")},
            None,
        ),
        (
            "google_calendar",
            "/gcal/token",
            "/gcal/calendar/v3/calendars/renewal%40provider-lab.test/events/watch",
            {},
            "/gcal/calendar/v3/channels/stop",
        ),
        (
            "google_drive",
            "/gdrive/token",
            "/gdrive/drive/v3/changes/watch",
            {},
            "/gdrive/drive/v3/channels/stop",
        ),
    ),
)
async def test_watch_lifecycle_tracks_before_renewal_and_after_expiry(
    source: str,
    token_path: str,
    watch_path: str,
    token_data: dict[str, str],
    stop_path: str | None,
) -> None:
    """Certify active/replacement/expiry delivery paths without fake fields."""

    app = build_provider_lab_app()
    scope = f"{source}-watch-lifecycle"
    headers = {"X-Provider-Lab-Scope": scope}
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        configured = await client.put(
            f"/_lab/sources/{source}/state",
            json=_lifecycle_state(),
        )
        assert configured.status_code == 200

        first_token = await client.post(
            token_path,
            data=token_data,
            headers=headers,
        )
        assert first_token.status_code == 200
        first = await client.post(
            watch_path,
            json=_watch_body(source, f"{source}-channel-first"),
            headers={
                **headers,
                "Authorization": f"Bearer {first_token.json()['access_token']}",
            },
        )
        assert first.status_code == 200

        registered = await _watch_lifecycle(
            client,
            source=source,
            scope=scope,
        )
        assert registered["enabled"] is True
        assert registered["count"] == 1
        first_record = registered["watches"][0]
        assert first_record["active"] is True
        assert first_record["state"] == "active"

        # Four seconds of a five-second virtual TTL: the first notification
        # path is still live immediately before the renewal threshold.
        assert (await client.post("/_lab/clock/advance", json={"seconds": 4})).status_code == 200
        before_renewal = await _watch_lifecycle(
            client,
            source=source,
            scope=scope,
        )
        assert before_renewal["watches"][0]["active"] is True

        renewed_token = await client.post(
            token_path,
            data=token_data,
            headers=headers,
        )
        assert renewed_token.status_code == 200
        renewed = await client.post(
            watch_path,
            json=_watch_body(source, f"{source}-channel-renewed"),
            headers={
                **headers,
                "Authorization": f"Bearer {renewed_token.json()['access_token']}",
            },
        )
        assert renewed.status_code == 200

        after_renewal = await _watch_lifecycle(
            client,
            source=source,
            scope=scope,
        )
        assert after_renewal["count"] == 2
        old_record, renewed_record = after_renewal["watches"]
        assert renewed_record["active"] is True
        assert renewed_record["state"] == "active"

        if stop_path is None:
            # Gmail's users.watch is a replacement operation for one mailbox.
            assert old_record["active"] is False
            assert old_record["state"] == "replaced"
            assert old_record["inactive_reason"] == "replaced"
        else:
            # Google channels coexist until the renewal path stops the old
            # provider resource. That is why production persists new state
            # before it sends channels.stop for the old id/resource pair.
            assert old_record["active"] is True
            stopped = await client.post(
                stop_path,
                json={
                    "id": first_record["channel_id"],
                    "resourceId": first_record["resource_id"],
                },
                headers={
                    **headers,
                    "Authorization": (
                        f"Bearer {renewed_token.json()['access_token']}"
                    ),
                },
            )
            assert stopped.status_code == 200
            after_stop = await _watch_lifecycle(
                client,
                source=source,
                scope=scope,
            )
            old_record, renewed_record = after_stop["watches"]
            assert old_record["active"] is False
            assert old_record["state"] == "stopped"
            assert renewed_record["active"] is True

        # The second registration expires five seconds after the renewal;
        # advancing past it proves that delivery cannot remain active forever.
        assert (await client.post("/_lab/clock/advance", json={"seconds": 6})).status_code == 200
        after_expiry = await _watch_lifecycle(
            client,
            source=source,
            scope=scope,
        )
        assert after_expiry["watches"][1]["active"] is False
        assert after_expiry["watches"][1]["state"] == "expired"
        assert after_expiry["watches"][1]["inactive_reason"] == "expired"


async def test_replacing_lifecycle_fixture_clears_prior_watch_state() -> None:
    app = build_provider_lab_app()
    scope = "gmail-lifecycle-reset"
    headers = {"X-Provider-Lab-Scope": scope}
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        assert (
            await client.put("/_lab/sources/gmail/state", json=_lifecycle_state())
        ).status_code == 200
        token = await client.post(
            "/gmail/token",
            data={"assertion": _jwt_for("renewal-a@provider-lab.test")},
            headers=headers,
        )
        assert token.status_code == 200
        assert (
            await client.post(
                "/gmail/gmail/v1/users/me/watch",
                json=_watch_body("gmail", "unused"),
                headers={
                    **headers,
                    "Authorization": f"Bearer {token.json()['access_token']}",
                },
            )
        ).status_code == 200
        assert (await _watch_lifecycle(client, source="gmail", scope=scope))["count"] == 1

        replacement = await client.put(
            "/_lab/sources/gmail/state",
            json=_lifecycle_state(),
        )
        assert replacement.status_code == 200
        state = await _watch_lifecycle(client, source="gmail", scope=scope)

    assert state == {"enabled": True, "count": 0, "watches": []}


_OAUTH_CASES = (
    ("quickbooks", "/quickbooks/oauth2/v1/tokens/bearer"),
    ("gusto", "/gusto/oauth/token"),
    ("linkedin", "/linkedin/oauth/v2/accessToken"),
)


@pytest.mark.parametrize("source,token_path", _OAUTH_CASES)
async def test_oauth_refresh_lifecycle_rejects_old_expiry_and_accepts_renewal(
    source: str,
    token_path: str,
) -> None:
    app = build_provider_lab_app()
    scope = f"{source}-renewal-scope"
    old_refresh = f"old-{source}-refresh"
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        configured = await client.put(
            f"/_lab/sources/{source}/state",
            json=_lifecycle_state(initial_refresh_token=old_refresh),
        )
        assert configured.status_code == 200
        first = await client.post(
            token_path,
            data={"grant_type": "refresh_token", "refresh_token": old_refresh},
            headers={"X-Provider-Lab-Scope": scope},
        )
        assert first.status_code == 200
        first_token = first.json()
        assert first_token["access_token"].startswith("plr1.access.")
        assert first_token["refresh_token"].startswith("plr1.refresh.")
        assert await _oauth_resource_call(
            client,
            source=source,
            scope=scope,
            access_token=first_token["access_token"],
        ) == 200

        await client.post("/_lab/clock/advance", json={"seconds": 6})
        assert await _oauth_resource_call(
            client,
            source=source,
            scope=scope,
            access_token=first_token["access_token"],
        ) == 401
        expired_refresh = await client.post(
            token_path,
            data={"grant_type": "refresh_token", "refresh_token": old_refresh},
            headers={"X-Provider-Lab-Scope": scope},
        )
        assert expired_refresh.status_code == 400
        assert expired_refresh.json()["error"]["code"] == "invalid_grant"

        renewed = await client.post(
            token_path,
            data={
                "grant_type": "refresh_token",
                "refresh_token": first_token["refresh_token"],
            },
            headers={"X-Provider-Lab-Scope": scope},
        )
        assert renewed.status_code == 200
        assert renewed.json()["access_token"] != first_token["access_token"]
        assert await _oauth_resource_call(
            client,
            source=source,
            scope=scope,
            access_token=renewed.json()["access_token"],
        ) == 200


async def _oauth_resource_call(
    client: httpx.AsyncClient,
    *,
    source: str,
    scope: str,
    access_token: str,
) -> int:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Provider-Lab-Scope": scope,
    }
    if source == "quickbooks":
        response = await client.get(
            "/quickbooks/v3/company/realm/companyinfo/realm",
            headers=headers,
        )
    elif source == "gusto":
        response = await client.get(
            "/gusto/v1/companies/provider-lab-company",
            headers=headers,
        )
    elif source == "linkedin":
        response = await client.get(
            "/linkedin/posts",
            params={"q": "author", "author": "urn:li:organization:1"},
            headers={
                **headers,
                "LinkedIn-Version": "202501",
                "X-Restli-Protocol-Version": "2.0.0",
            },
        )
    else:  # pragma: no cover - parametrization is intentionally closed
        raise AssertionError(f"unsupported OAuth lifecycle source {source!r}")
    return response.status_code


@pytest.mark.parametrize(
    "source,token_path,resource_path",
    (
        ("ramp", "/ramp/token", "/ramp/business"),
        ("carta", "/carta/o/access_token/", "/carta/v1alpha1/issuers"),
    ),
)
async def test_client_credential_renewal_mints_scope_bound_live_tokens(
    source: str,
    token_path: str,
    resource_path: str,
) -> None:
    app = build_provider_lab_app()
    scope = f"{source}-renewal-scope"
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        await client.put(
            f"/_lab/sources/{source}/state",
            json=_lifecycle_state(),
        )
        bad_grant = await client.post(token_path, data={"grant_type": "password"})
        minted = await client.post(
            token_path,
            data={"grant_type": "client_credentials"},
            headers={"X-Provider-Lab-Scope": scope},
        )
        assert bad_grant.status_code == 400
        assert minted.status_code == 200
        first_access = minted.json()["access_token"]
        live = await client.get(
            resource_path,
            headers={
                "Authorization": f"Bearer {first_access}",
                "X-Provider-Lab-Scope": scope,
            },
        )
        await client.post("/_lab/clock/advance", json={"seconds": 6})
        expired = await client.get(
            resource_path,
            headers={
                "Authorization": f"Bearer {first_access}",
                "X-Provider-Lab-Scope": scope,
            },
        )
        reminted = await client.post(
            token_path,
            data={"grant_type": "client_credentials"},
            headers={"X-Provider-Lab-Scope": scope},
        )
        recovered = await client.get(
            resource_path,
            headers={
                "Authorization": f"Bearer {reminted.json()['access_token']}",
                "X-Provider-Lab-Scope": scope,
            },
        )

    assert live.status_code == 200
    assert expired.status_code == 401
    assert reminted.status_code == recovered.status_code == 200


async def test_default_fixture_responses_remain_static_without_lifecycle_state() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        calendar_token = await client.post("/gcal/token")
        quickbooks_token = await client.post(
            "/quickbooks/oauth2/v1/tokens/bearer",
            data={"grant_type": "refresh_token"},
        )
        calendar_watch = await client.post(
            "/gcal/calendar/v3/calendars/default/events/watch",
            json={"id": "static-channel"},
        )
        static_watch_state = await client.get(
            "/_lab/sources/google_calendar/watch-lifecycle",
        )

    assert calendar_token.json()["access_token"] == "sandbox-access-token"
    assert quickbooks_token.json()["access_token"] == "lab-quickbooks-access-token"
    assert calendar_watch.json()["expiration"] == "4102444800000"
    assert static_watch_state.status_code == 200
    assert static_watch_state.json() == {
        "enabled": False,
        "count": 0,
        "watches": [],
    }


@pytest.mark.parametrize(
    "stop_path",
    (
        "/gcal/calendar/v3/channels/stop",
        "/gdrive/drive/v3/channels/stop",
    ),
)
async def test_static_google_channel_stop_keeps_idempotent_fixture_behavior(
    stop_path: str,
) -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        # The compact static fixtures intentionally accepted an idempotent stop
        # without parsing a provider payload; opt-in lifecycle validation must
        # not change that legacy fixture contract.
        response = await client.post(stop_path, content=b"not-json")

    assert response.status_code == 200
    assert response.json() == {}
