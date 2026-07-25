"""OAuth and operator live-ingress surfaces derived from the source catalog."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from services.app.gateway.route_access import (
    GATEWAY_BEARER_BYPASS_PATH_POLICIES,
    RouteAccess,
)
from services.ingest.integrations.router import build_integrations_router
from services.ingest.source_contract import (
    DEDICATED_INGRESS_DEFINITIONS,
    OAUTH_INGRESS_CATALOG,
    PROVIDER_DEFINITIONS,
    SOURCE_CATALOG,
    SOURCE_LIVE_INGRESS_CATALOG,
    WEBHOOK_INGRESS_CATALOG,
    resolve_callable_reference,
    validate_provider_ingress_catalog,
)
from services.ingest.source_contract.catalog import (
    NON_SOURCE_CHANNEL_DEFINITIONS,
)


_EXPECTED_OAUTH_PATHS = {
    "slack": (
        "/integrations/slack/install",
        "/integrations/slack/callback",
        "shared_router",
    ),
    "github": (
        "/integrations/github/install",
        "/integrations/github/callback",
        "shared_router",
    ),
    "discord": (
        "/integrations/discord/install",
        "/integrations/discord/callback",
        "shared_router",
    ),
    "notion": (
        "/integrations/notion/install",
        "/integrations/notion/callback",
        "shared_router",
    ),
    "figma": (
        "/integrations/figma/oauth/start",
        "/integrations/figma/oauth/callback",
        "native_router",
    ),
    "facebook_pages": (
        "/integrations/facebook_pages/install",
        "/integrations/facebook_pages/callback",
        "shared_router",
    ),
}


def test_oauth_catalog_owns_every_callback_path_and_binding() -> None:
    assert {
        source_id: (
            ingress.install_path,
            ingress.callback_path,
            ingress.mount_mode,
        )
        for source_id, ingress in OAUTH_INGRESS_CATALOG.items()
    } == _EXPECTED_OAUTH_PATHS

    for ingress in OAUTH_INGRESS_CATALOG.values():
        assert callable(
            resolve_callable_reference(ingress.install_handler_binding)
        )
        assert callable(
            resolve_callable_reference(ingress.callback_handler_binding)
        )


def test_shared_oauth_router_is_mounted_from_the_catalog_only() -> None:
    actual = {
        (route.path, tuple(sorted(route.methods or ())))
        for route in build_integrations_router().routes
    }
    expected = {
        (path, ("GET",))
        for ingress in OAUTH_INGRESS_CATALOG.values()
        if ingress.mount_mode == "shared_router"
        for path in (ingress.install_path, ingress.callback_path)
    }
    assert actual == expected
    assert OAUTH_INGRESS_CATALOG["figma"].callback_path not in {
        path for path, _methods in actual
    }


def test_public_route_access_is_derived_from_ingress_contracts() -> None:
    for ingress in OAUTH_INGRESS_CATALOG.values():
        for path in (ingress.callback_path, *ingress.public_result_paths):
            assert (
                GATEWAY_BEARER_BYPASS_PATH_POLICIES[path].access
                is RouteAccess.SELF_AUTHENTICATED
            )
    for ingress in DEDICATED_INGRESS_DEFINITIONS:
        assert (
            GATEWAY_BEARER_BYPASS_PATH_POLICIES[ingress.route_path].access
            is RouteAccess.PROVIDER_SIGNED
        )


def test_live_ingress_endpoints_are_derived_without_a_source_route_map() -> None:
    for ingress in WEBHOOK_INGRESS_CATALOG.values():
        if ingress.source_id is not None:
            assert (
                SOURCE_LIVE_INGRESS_CATALOG[ingress.source_id]
                == ingress.route_path
            )
    for ingress in DEDICATED_INGRESS_DEFINITIONS:
        assert (
            SOURCE_LIVE_INGRESS_CATALOG[ingress.source_id]
            == ingress.route_path
        )

    assert SOURCE_LIVE_INGRESS_CATALOG["telegram"] == (
        "customer-cloud MTProto gateway worker"
    )
    assert "signal-cli HTTP JSON-RPC/SSE" in (
        SOURCE_LIVE_INGRESS_CATALOG["signal"]
    )
    assert "SQS/EventBridge" in SOURCE_LIVE_INGRESS_CATALOG["aws"]


def test_oauth_catalog_and_entries_are_immutable() -> None:
    with pytest.raises(TypeError):
        OAUTH_INGRESS_CATALOG["other"] = OAUTH_INGRESS_CATALOG["slack"]  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        OAUTH_INGRESS_CATALOG["slack"].callback_path = "/other"  # type: ignore[misc]


def test_validator_rejects_duplicate_oauth_callback_paths() -> None:
    github = PROVIDER_DEFINITIONS[1]
    conflict = replace(
        github.oauth_ingresses[0],
        callback_path=OAUTH_INGRESS_CATALOG["slack"].callback_path,
    )
    conflicting_github = replace(github, oauth_ingresses=(conflict,))
    providers = (
        PROVIDER_DEFINITIONS[0],
        conflicting_github,
        *PROVIDER_DEFINITIONS[2:],
    )

    with pytest.raises(ValueError, match="public route GET"):
        validate_provider_ingress_catalog(
            providers,
            tuple(SOURCE_CATALOG.values()),
            NON_SOURCE_CHANNEL_DEFINITIONS,
        )
