"""Opt-in, virtual-clock renewal semantics for the R2 provider surfaces.

The Provider Lab normally preserves its compact static fixtures.  A fixture can
opt into this module by adding a ``renewal_lifecycle`` object with
``enabled: true`` to one of the eight R2 source states.  In that mode the lab
creates opaque, self-validating access/refresh identifiers from the request's
virtual clock and scope.  No wall clock, provider credential, or mutable
process-global token cache is involved.

The small token format is deliberately a Provider-Lab-only test primitive.  It
lets a focused lifecycle test prove all relevant states deterministically:
before expiry, a renewed credential works; after a virtual-clock advance, the
old credential is rejected while the previously renewed refresh credential can
mint a fresh access credential.  Default fixtures never call these helpers and
continue returning their historical static payloads unchanged.
"""
from __future__ import annotations

import base64
import hashlib
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping
from urllib.parse import parse_qs

from .protocol import ProviderRequest, ProviderResponse


LIFECYCLE_STATE_KEY = "renewal_lifecycle"
_TOKEN_PREFIX = "plr1"
_TOKEN_KINDS = frozenset({"access", "refresh"})
_SIGNING_SALT = "fyralis-provider-lab-renewal-v1"


@dataclass(frozen=True, slots=True)
class _LifecycleConfig:
    access_ttl_seconds: int
    refresh_ttl_seconds: int
    watch_ttl_seconds: int
    initial_refresh_token: str | None
    initial_refresh_expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class _LifecycleToken:
    kind: Literal["access", "refresh"]
    source: str
    scope: str
    expires_at: datetime


@dataclass(slots=True)
class _LifecycleWatch:
    """One live-provider-shaped push registration held by the lab only."""

    watch_id: str
    source: str
    scope: str
    target: str
    created_at: datetime
    expires_at: datetime
    channel_id: str | None
    resource_id: str | None
    history_id: str | None
    state: Literal["active", "expired", "replaced", "stopped"] = "active"
    inactive_at: datetime | None = None
    inactive_reason: Literal["expired", "replaced", "stopped"] | None = None


class LifecycleWatchRegistry:
    """Track opt-in Gmail and Google push registrations per lab instance.

    The normal Provider Lab fixture remains deliberately static.  Once a
    fixture enables ``renewal_lifecycle``, however, a renewal test needs more
    than a changing expiration timestamp: it must be able to show that the old
    notification path no longer delivers and the replacement does.  This
    registry is owned by one source adapter, is driven exclusively by the
    virtual clock, and is inspectable only through the Lab control plane.

    Gmail's ``users.watch`` replaces the current mailbox subscription. Google
    Calendar and Drive channels are independent provider resources; a new
    channel remains alongside the old one until the caller stops it or it
    expires.  That distinction mirrors the production renewal sequence,
    which persists a replacement and then calls ``channels.stop`` on the old
    channel.
    """

    def __init__(self, source: str) -> None:
        self._source = source
        self._lock = threading.RLock()
        self._next_id = 1
        self._records: list[_LifecycleWatch] = []

    def reset(self) -> None:
        """Clear only test-only lifecycle registrations for this adapter."""

        with self._lock:
            self._next_id = 1
            self._records.clear()

    def register(
        self,
        request: ProviderRequest,
        *,
        target: str,
        channel_id: str | None = None,
        resource_id: str | None = None,
        history_id: str | None = None,
    ) -> bool:
        """Record a successful lifecycle-only watch creation.

        ``False`` means the fixture did not opt into lifecycle behavior, so a
        static route must retain its historical response and no mutable state
        is created.
        """

        config = _config(request)
        if config is None:
            return False
        if request.source != self._source:
            raise ValueError(
                "lifecycle watch registry source does not match provider request",
            )
        if not target:
            raise ValueError("lifecycle watch target must be non-empty")
        if request.source == "gmail" and channel_id is not None:
            raise ValueError("Gmail lifecycle watches do not carry channel ids")
        if request.source != "gmail" and (
            not channel_id or not resource_id
        ):
            raise ValueError(
                "Google lifecycle watches require channel_id and resource_id",
            )

        expires_at = request.now + timedelta(seconds=config.watch_ttl_seconds)
        with self._lock:
            self._expire_due_locked(request.now)
            if request.source == "gmail":
                # The Gmail API maintains one current mailbox watch. Reissuing
                # it is the renewal/replacement operation, not a second live
                # callback path.
                for record in self._records:
                    if (
                        record.scope == request.scope
                        and record.target == target
                        and record.state == "active"
                    ):
                        self._deactivate_locked(record, "replaced", request.now)
            else:
                # Google channel IDs identify a provider resource. If a caller
                # reuses one before it has expired, the later registration is
                # the replacement for that same ID; different IDs coexist
                # until channels.stop is called, as they do at Google.
                for record in self._records:
                    if (
                        record.scope == request.scope
                        and record.channel_id == channel_id
                        and record.state == "active"
                    ):
                        self._deactivate_locked(record, "replaced", request.now)

            record = _LifecycleWatch(
                watch_id=f"{request.source}-watch-{self._next_id}",
                source=request.source,
                scope=request.scope,
                target=target,
                created_at=request.now,
                expires_at=expires_at,
                channel_id=channel_id,
                resource_id=resource_id,
                history_id=history_id,
            )
            self._next_id += 1
            self._records.append(record)
        return True

    def stop(
        self,
        request: ProviderRequest,
        *,
        channel_id: str | None = None,
        resource_id: str | None = None,
    ) -> int | None:
        """Stop matching opt-in watches and return their count.

        ``None`` denotes static-fixture mode.  The real Google stop operation
        is idempotent, so an unknown/already-inactive channel simply yields a
        zero count in lifecycle mode as well.
        """

        if _config(request) is None:
            return None
        if request.source != self._source:
            raise ValueError(
                "lifecycle watch registry source does not match provider request",
            )
        if request.source != "gmail" and (not channel_id or not resource_id):
            raise ValueError(
                "Google lifecycle channel stop requires channel_id and resource_id",
            )
        with self._lock:
            self._expire_due_locked(request.now)
            stopped = 0
            for record in self._records:
                if record.scope != request.scope or record.state != "active":
                    continue
                if request.source != "gmail" and (
                    record.channel_id != channel_id
                    or record.resource_id != resource_id
                ):
                    continue
                self._deactivate_locked(record, "stopped", request.now)
                stopped += 1
            return stopped

    def snapshot(
        self,
        *,
        now: datetime,
        source_state: Mapping[str, Any],
        scope: str | None = None,
        channel_id: str | None = None,
        resource_id: str | None = None,
    ) -> dict[str, Any]:
        """Return deterministic control-plane state without provider secrets."""

        if _config_from_state(source_state) is None:
            return {"enabled": False, "count": 0, "watches": []}
        with self._lock:
            self._expire_due_locked(now)
            records = [
                record
                for record in self._records
                if (scope is None or record.scope == scope)
                and (channel_id is None or record.channel_id == channel_id)
                and (resource_id is None or record.resource_id == resource_id)
            ]
            return {
                "enabled": True,
                "count": len(records),
                "watches": [self._serialize(record) for record in records],
            }

    def _expire_due_locked(self, now: datetime) -> None:
        for record in self._records:
            if record.state == "active" and now >= record.expires_at:
                self._deactivate_locked(record, "expired", record.expires_at)

    @staticmethod
    def _deactivate_locked(
        record: _LifecycleWatch,
        reason: Literal["expired", "replaced", "stopped"],
        at: datetime,
    ) -> None:
        record.state = reason
        record.inactive_reason = reason
        record.inactive_at = at

    @staticmethod
    def _serialize(record: _LifecycleWatch) -> dict[str, Any]:
        return {
            "watch_id": record.watch_id,
            "source": record.source,
            "scope": record.scope,
            "target": record.target,
            "created_at": _isoformat_z(record.created_at),
            "expires_at": _isoformat_z(record.expires_at),
            "channel_id": record.channel_id,
            "resource_id": record.resource_id,
            "history_id": record.history_id,
            "state": record.state,
            "active": record.state == "active",
            "inactive_at": (
                _isoformat_z(record.inactive_at)
                if record.inactive_at is not None
                else None
            ),
            "inactive_reason": record.inactive_reason,
        }


def lifecycle_token_response(
    request: ProviderRequest,
    *,
    token_type: str | None,
    include_refresh_token: bool = False,
    refresh_expiry_field: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Build a virtual-time token response, or ``None`` for static fixtures."""

    config = _config(request)
    if config is None:
        return None
    body = dict(extra or {})
    body.update(
        {
            "access_token": _issue_token(
                "access",
                source=request.source,
                scope=request.scope,
                now=request.now,
                ttl_seconds=config.access_ttl_seconds,
            ),
            "expires_in": config.access_ttl_seconds,
        }
    )
    if token_type is not None:
        body["token_type"] = token_type
    if include_refresh_token:
        body["refresh_token"] = _issue_token(
            "refresh",
            source=request.source,
            scope=request.scope,
            now=request.now,
            ttl_seconds=config.refresh_ttl_seconds,
        )
        if refresh_expiry_field is not None:
            body[refresh_expiry_field] = config.refresh_ttl_seconds
    return body


def validate_lifecycle_refresh_grant(
    request: ProviderRequest,
) -> ProviderResponse | None:
    """Accept one live configured/dynamic refresh token in lifecycle mode."""

    config = _config(request)
    if config is None:
        return None
    form = _form(request)
    if form.get("grant_type") != "refresh_token":
        return _oauth_error(
            "unsupported_grant_type",
            "Lifecycle refresh requires grant_type=refresh_token",
        )
    refresh_token = form.get("refresh_token")
    if not refresh_token:
        return _oauth_error(
            "invalid_grant",
            "Lifecycle refresh credential is required",
        )
    if refresh_token == config.initial_refresh_token:
        expiry = config.initial_refresh_expires_at
        if expiry is not None and request.now < expiry:
            return None
        return _oauth_error(
            "invalid_grant",
            "Lifecycle refresh credential is expired",
        )
    parsed = _parse_token(refresh_token)
    if (
        parsed is None
        or parsed.kind != "refresh"
        or parsed.source != request.source
        or parsed.scope != request.scope
        or request.now >= parsed.expires_at
    ):
        return _oauth_error(
            "invalid_grant",
            "Lifecycle refresh credential is invalid or expired",
        )
    return None


def validate_lifecycle_client_credentials(
    request: ProviderRequest,
) -> ProviderResponse | None:
    """Require the documented client-credentials grant only in lifecycle mode."""

    if _config(request) is None:
        return None
    if _form(request).get("grant_type") != "client_credentials":
        return _oauth_error(
            "unsupported_grant_type",
            "Lifecycle client credential mint requires grant_type=client_credentials",
        )
    return None


def require_lifecycle_access_token(
    request: ProviderRequest,
) -> ProviderResponse | None:
    """Reject absent, mismatched, or expired lifecycle access credentials."""

    if _config(request) is None:
        return None
    parsed = _parse_token(_bearer(request.headers) or "")
    if (
        parsed is None
        or parsed.kind != "access"
        or parsed.source != request.source
        or parsed.scope != request.scope
        or request.now >= parsed.expires_at
    ):
        return ProviderResponse.json(
            {
                "error": {
                    "code": "invalid_token",
                    "message": (
                        "Provider Lab lifecycle access credential is invalid "
                        "or expired"
                    ),
                }
            },
            status_code=401,
        )
    return None


def lifecycle_watch_expiration(request: ProviderRequest) -> str | None:
    """Return a virtual-time watch expiry in provider millisecond form."""

    config = _config(request)
    if config is None:
        return None
    expiry = request.now + timedelta(seconds=config.watch_ttl_seconds)
    return str(int(expiry.timestamp() * 1000))


def lifecycle_resource_id(
    request: ProviderRequest,
    *,
    resource_prefix: str,
) -> str | None:
    """Return an opaque resource identifier that changes by source/scope/time."""

    if _config(request) is None:
        return None
    return f"{resource_prefix}:{_opaque_id(request, label=resource_prefix)}"


def lifecycle_history_id(request: ProviderRequest) -> str | None:
    """Return a numeric Gmail-shaped history ID tied to virtual time/scope."""

    if _config(request) is None:
        return None
    # Gmail history ids are numeric strings. Keep this bounded well inside a
    # signed bigint while changing deterministically with every relevant input.
    return str(int(_opaque_id(request, label="history"), 16) % 9_000_000_000_000_000)


def _config(request: ProviderRequest) -> _LifecycleConfig | None:
    return _config_from_state(request.source_state)


def _config_from_state(state: Mapping[str, Any]) -> _LifecycleConfig | None:
    raw = state.get(LIFECYCLE_STATE_KEY)
    if not isinstance(raw, Mapping) or raw.get("enabled") is not True:
        return None
    initial_token = raw.get("initial_refresh_token")
    if initial_token is not None and (
        not isinstance(initial_token, str) or not initial_token
    ):
        raise ValueError("initial_refresh_token must be a non-empty string")
    initial_expiry_raw = raw.get("initial_refresh_expires_at")
    initial_expiry = (
        None
        if initial_expiry_raw is None
        else _parse_timestamp(initial_expiry_raw, "initial_refresh_expires_at")
    )
    if initial_token is not None and initial_expiry is None:
        raise ValueError(
            "initial_refresh_expires_at is required with initial_refresh_token",
        )
    return _LifecycleConfig(
        access_ttl_seconds=_positive_seconds(
            raw.get("access_ttl_seconds", 3_600),
            "access_ttl_seconds",
        ),
        refresh_ttl_seconds=_positive_seconds(
            raw.get("refresh_ttl_seconds", 86_400),
            "refresh_ttl_seconds",
        ),
        watch_ttl_seconds=_positive_seconds(
            raw.get("watch_ttl_seconds", 604_800),
            "watch_ttl_seconds",
        ),
        initial_refresh_token=initial_token,
        initial_refresh_expires_at=initial_expiry,
    )


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _positive_seconds(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    if value > 31_536_000:
        raise ValueError(f"{field} must not exceed one year")
    return value


def _parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _form(request: ProviderRequest) -> dict[str, str]:
    return {
        key: values[-1]
        for key, values in parse_qs(
            request.body.decode("utf-8", "replace"),
            keep_blank_values=True,
        ).items()
        if values
    }


def _bearer(headers: Mapping[str, str]) -> str | None:
    authorization = headers.get("authorization")
    if not authorization:
        return None
    scheme, separator, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not separator or not token.strip():
        return None
    return token.strip()


def _oauth_error(code: str, message: str) -> ProviderResponse:
    return ProviderResponse.json(
        {"error": {"code": code, "message": message}},
        status_code=400,
    )


def _issue_token(
    kind: Literal["access", "refresh"],
    *,
    source: str,
    scope: str,
    now: datetime,
    ttl_seconds: int,
) -> str:
    expires_at = now + timedelta(seconds=ttl_seconds)
    encoded_scope = base64.urlsafe_b64encode(scope.encode("utf-8")).decode("ascii")
    encoded_scope = encoded_scope.rstrip("=")
    expires_epoch = int(expires_at.timestamp())
    signature = _signature(kind, source, encoded_scope, expires_epoch)
    return ".".join(
        (
            _TOKEN_PREFIX,
            kind,
            source,
            encoded_scope,
            str(expires_epoch),
            signature,
        )
    )


def _parse_token(value: str) -> _LifecycleToken | None:
    parts = value.split(".")
    if len(parts) != 6 or parts[0] != _TOKEN_PREFIX:
        return None
    _prefix, kind, source, encoded_scope, raw_expiry, signature = parts
    if kind not in _TOKEN_KINDS or not source or not raw_expiry.isdigit():
        return None
    try:
        padded_scope = encoded_scope + ("=" * (-len(encoded_scope) % 4))
        scope = base64.urlsafe_b64decode(padded_scope.encode("ascii")).decode("utf-8")
        expires_at = datetime.fromtimestamp(int(raw_expiry), tz=timezone.utc)
    except (OverflowError, UnicodeDecodeError, ValueError):
        return None
    if not scope or signature != _signature(kind, source, encoded_scope, int(raw_expiry)):
        return None
    return _LifecycleToken(
        kind=kind,  # type: ignore[arg-type]
        source=source,
        scope=scope,
        expires_at=expires_at,
    )


def _signature(kind: str, source: str, encoded_scope: str, expires_epoch: int) -> str:
    payload = f"{_SIGNING_SALT}|{kind}|{source}|{encoded_scope}|{expires_epoch}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def _opaque_id(request: ProviderRequest, *, label: str) -> str:
    payload = "|".join(
        (
            request.source,
            request.scope,
            label,
            str(int(request.now.timestamp() * 1000)),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


__all__ = [
    "LIFECYCLE_STATE_KEY",
    "LifecycleWatchRegistry",
    "lifecycle_history_id",
    "lifecycle_resource_id",
    "lifecycle_token_response",
    "lifecycle_watch_expiration",
    "require_lifecycle_access_token",
    "validate_lifecycle_client_credentials",
    "validate_lifecycle_refresh_grant",
]
