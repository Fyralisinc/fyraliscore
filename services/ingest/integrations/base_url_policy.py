"""Fail-closed base-URL policy for credential-bearing native-connect probes.

Native-connect routes accept provider credentials and immediately verify them
against a provider API.  A request-controlled URL at that boundary is both an
SSRF primitive and a credential-forwarding primitive.  This module centralizes
the rules so individual source routers cannot accidentally weaken them.

Fixed-host SaaS sources may use only their catalog endpoint in production
(including an exact operator-owned endpoint environment override).  Sources
whose endpoint is intrinsically installation-owned, currently Jira and
Grafana, may use a public HTTPS installation host.  Direct local/private
targets are rejected unless an exact endpoint override explicitly authorizes
the target.

Plain HTTP is never accepted in production.  Outside production it is limited
to loopback and requires an explicit Provider Lab URL, an exact per-endpoint
environment override, or an explicit test environment.
"""

from __future__ import annotations

import ipaddress
import os
from urllib.parse import unquote, urlsplit

from fastapi import HTTPException

from lib.integrations.endpoint_contract import provider_endpoint_definition
from lib.integrations.endpoints import endpoint
from lib.integrations.provider_lab import provider_lab_root_url
from lib.shared.env import env_name, is_prod


_TEST_ENVIRONMENTS = frozenset({"ci", "test", "testing"})
_LOCAL_HOST_SUFFIXES = (
    ".internal",
    ".lan",
    ".local",
    ".localhost",
    ".home",
)


def _bad(detail: str) -> HTTPException:
    return HTTPException(status_code=400, detail=detail)


def _normalized_url(raw: object, *, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise _bad(f"{field_name} is required")
    value = raw.strip().rstrip("/")
    if len(value) > 2_048 or any(ord(char) < 32 for char in value):
        raise _bad(f"{field_name} is not a valid provider URL")
    if "\\" in value:
        raise _bad(f"{field_name} must not contain backslashes")

    try:
        parsed = urlsplit(value)
        # Accessing .port forces urllib to validate malformed/out-of-range ports.
        parsed.port
    except ValueError as exc:
        raise _bad(f"{field_name} is not a valid provider URL") from exc

    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise _bad(f"{field_name} must be a full HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise _bad(f"{field_name} must not contain URL credentials")
    if parsed.query or parsed.fragment:
        raise _bad(f"{field_name} must not contain a query or fragment")

    decoded_path = unquote(parsed.path)
    if any(segment in {".", ".."} for segment in decoded_path.split("/")):
        raise _bad(f"{field_name} must not contain dot path segments")
    return value


def _same_url(left: str, right: str) -> bool:
    """Compare already validated bases without accepting host confusion."""

    left_parts = urlsplit(left)
    right_parts = urlsplit(right)
    return (
        left_parts.scheme.casefold(),
        left_parts.hostname.casefold() if left_parts.hostname else "",
        left_parts.port,
        left_parts.path.rstrip("/"),
    ) == (
        right_parts.scheme.casefold(),
        right_parts.hostname.casefold() if right_parts.hostname else "",
        right_parts.port,
        right_parts.path.rstrip("/"),
    )


def _is_loopback_host(hostname: str) -> bool:
    normalized = hostname.casefold().rstrip(".")
    if normalized == "localhost" or normalized.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _is_direct_nonpublic_host(hostname: str) -> bool:
    normalized = hostname.casefold().rstrip(".")
    if normalized == "localhost" or normalized.endswith(_LOCAL_HOST_SUFFIXES):
        return True
    try:
        return not ipaddress.ip_address(normalized).is_global
    except ValueError:
        return False


def _exact_endpoint_override(endpoint_name: str, candidate: str) -> bool:
    definition = provider_endpoint_definition(endpoint_name)
    configured = os.environ.get(definition.override_env, "").strip()
    if not configured:
        return False
    try:
        normalized = _normalized_url(
            configured,
            field_name=definition.override_env,
        )
    except HTTPException:
        # The request must not turn a malformed operator setting into an
        # alternate URL authorization path. endpoint() will remain diagnosable
        # through its normal startup/configuration checks.
        return False
    return _same_url(candidate, normalized)


def _provider_lab_authorizes(candidate: str) -> bool:
    try:
        root = provider_lab_root_url()
    except RuntimeError:
        return False
    if root is None:
        return False
    candidate_parts = urlsplit(candidate)
    root_parts = urlsplit(root)
    return (
        candidate_parts.scheme.casefold(),
        candidate_parts.hostname.casefold() if candidate_parts.hostname else "",
        candidate_parts.port,
    ) == (
        root_parts.scheme.casefold(),
        root_parts.hostname.casefold() if root_parts.hostname else "",
        root_parts.port,
    )


def native_connect_base_url(
    raw_value: object | None,
    *,
    endpoint_name: str,
    installation_owned: bool = False,
    allowed_hostname_suffixes: tuple[str, ...] = (),
    field_name: str = "base_url",
) -> str:
    """Resolve and validate one credential-bearing connect target.

    ``installation_owned`` is intentionally narrow.  It is for providers whose
    real host is part of installation identity rather than a fixed SaaS API
    host.  Fixed-host sources cannot redirect credentials through request data
    in production.
    """

    if raw_value is not None and not isinstance(raw_value, str):
        raise _bad(f"{field_name} must be a string URL")
    explicit = isinstance(raw_value, str) and bool(raw_value.strip())
    selected: object = raw_value if explicit else endpoint(endpoint_name)
    candidate = _normalized_url(selected, field_name=field_name)
    parsed = urlsplit(candidate)
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    exact_override = _exact_endpoint_override(endpoint_name, candidate)

    if parsed.scheme == "http":
        explicitly_test = env_name() in _TEST_ENVIRONMENTS
        lab_authorized = _provider_lab_authorizes(candidate)
        if (
            is_prod()
            or not _is_loopback_host(hostname)
            or not (exact_override or lab_authorized or explicitly_test)
        ):
            raise _bad(
                f"{field_name} must use HTTPS; loopback HTTP is allowed only "
                "for an explicit non-production Provider Lab/test endpoint",
            )
    elif is_prod() and _is_direct_nonpublic_host(hostname) and not exact_override:
        raise _bad(
            f"{field_name} targets a local/private host that is not authorized "
            "by the endpoint environment contract",
        )

    resolved = endpoint(endpoint_name)
    if is_prod() and not installation_owned:
        if not resolved:
            raise _bad(f"{field_name} has no configured production endpoint")
        canonical = _normalized_url(resolved, field_name=field_name)
        if not _same_url(candidate, canonical):
            raise _bad(
                f"{field_name} cannot override the catalog-owned provider "
                "endpoint in production",
            )

    if is_prod() and installation_owned and allowed_hostname_suffixes:
        suffixes = tuple(item.casefold().rstrip(".") for item in allowed_hostname_suffixes)
        if not exact_override and not any(
            hostname == suffix.lstrip(".") or hostname.endswith(suffix)
            for suffix in suffixes
        ):
            raise _bad(
                f"{field_name} host is outside the provider's allowed "
                "production domains",
            )

    return candidate


__all__ = ["native_connect_base_url"]
