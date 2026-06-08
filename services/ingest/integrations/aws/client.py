"""services/ingest/integrations/aws/client.py — outbound AWS API client (IN-AWS).

Single outbound surface for the CloudTrail management-events backfill +
poll-incremental + the reconciler's gap probe. AWS authenticates with IAM
credentials and SIGNS every request with SigV4 (botocore). The synthetic gate
drives the REAL fetcher against a MOCK client (`mock_clients/aws.py`), so the
production signing here is intentionally a thin TODO-stubbed seam — the method
SURFACE the pipeline depends on is real and stable; only the wire-level signing
is left to confirm against the vendor SDK.

============================================================
API shape (load-bearing for the fetcher's time-window walk)
============================================================
`list_events(account_id, region, from_ms, to_ms, cursor)` mirrors CloudTrail's
`LookupEvents`: it returns a page of management events in the
[from_ms, to_ms] window plus an opaque continuation token (CloudTrail's
`NextToken`). `from_ms` / `to_ms` are epoch MILLISECONDS (CloudTrail's API takes
`StartTime` / `EndTime` as datetimes; the client converts at the boundary). Each
event element carries (CloudTrail event shape): `eventId` (stable, immutable),
`eventTime` (RFC3339 / epoch), `eventName`, `eventSource`, `awsRegion`,
`userIdentity`, `cloudTrailEvent` (the full JSON), and — for an alarm-state
related event — the alarm transition fields the handler reads.

The backfill walks the window newest-first in pages, advancing `cursor` until the
page returns no continuation token (end-of-data).

============================================================
NOVEL — SigV4 (TODO seam)
============================================================
Real AWS auth is IAM SigV4 over the per-region CloudTrail endpoint. In production
`_signed_request` must sign with botocore's SigV4Auth (or call boto3's
`cloudtrail` client). Until that is wired, `_signed_request` raises so it can
never silently issue an UNSIGNED request; the mock client replaces the whole
surface in the synthetic gate.

Logging redaction: IAM credentials (access key / secret / session token) and the
Authorization header are NEVER logged. The account id is hashed before logging.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import CompanyOSError


log = structlog.get_logger("integrations.aws.client")


_DEFAULT_TIMEOUT_S = 30.0
# CloudTrail LookupEvents caps `MaxResults` at 50; keep parity.
_DEFAULT_PAGE_SIZE = 50

# TODO(human): confirm the CloudTrail regional endpoint host template against
# vendor docs (e.g. https://cloudtrail.{region}.amazonaws.com) and the
# LookupEvents action/version. Exposed as a constant so the real client can be
# wired without touching call sites.
CLOUDTRAIL_ENDPOINT_TEMPLATE = os.environ.get(
    "AWS_CLOUDTRAIL_ENDPOINT_TEMPLATE",
    "https://cloudtrail.{region}.amazonaws.com",
)


class AwsApiError(CompanyOSError):
    """Outbound AWS API call failure (IN-AWS).

    Defined locally (not in lib/shared/errors.py) so this Phase-1 source does not
    edit the shared errors registry; the wiring phase may promote it. The mock
    client raises with the SAME stable `code` values so the fetcher branches
    identically against mock and real clients.

    Stable `code` values:
      - aws_api_throttled:      Throttling / RequestLimitExceeded (HTTP 400/429)
      - aws_api_unauthorized:   AccessDenied / UnrecognizedClient / Signature
                                does not match (403)
      - aws_api_not_found:      account/region/trail not found or not visible
      - aws_api_error:          other terminal errors / transport failures

    `context` carries `{http_status?, retry_after?, region?, account_id?}`. IAM
    credentials and the Authorization header are NEVER placed on context.
    """

    default_code = "aws_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


def short_account_hash(account_id: str) -> str:
    """Non-reversible 16-hex digest of the account id for logs."""
    return hashlib.blake2b(account_id.encode("utf-8"), digest_size=8).hexdigest()


def _parse_retry_after(value: str | None) -> float:
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 1.0


class AwsClient:
    """Outbound AWS API client, one per backfill/poll shard open.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_aws_client`
    (production / spammer) and by the seed/onboarding probe. In production the
    request is SigV4-signed against the per-region CloudTrail endpoint; in the
    synthetic gate the whole surface is replaced by `MockAwsClient`.
    """

    def __init__(
        self,
        *,
        account_id: str,
        region: str,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        credential_kind: str | None = None,
        secret_ref: str | None = None,
        http_client: Any | None = None,
        endpoint_override: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._account_id = account_id
        self._region = region
        self._credential_kind = credential_kind
        self._secret_ref = secret_ref
        self._creds_lock = asyncio.Lock()
        self._creds: Any | None = None
        # Production endpoint is per-region; a spammer/test override wins so
        # backfill points at the mock.
        self._endpoint = (
            endpoint_override
            or CLOUDTRAIL_ENDPOINT_TEMPLATE.format(region=region)
        ).rstrip("/")
        self._http = http_client

    async def aclose(self) -> None:
        """No durable client held by default; present for surface parity."""
        return None

    async def _credentials(self) -> Any:
        """Resolve IAM credentials once (assume-role / static keys).

        TODO(human): resolve real IAM credentials via credentials.py
        (AssumeRole with the install's role ARN, or static keys from the secret
        store) and cache them with expiry-aware refresh.
        """
        if self._creds is not None:
            return self._creds
        async with self._creds_lock:
            if self._creds is not None:
                return self._creds
            from services.ingest.integrations.aws.credentials import (
                resolve_credentials,
            )

            self._creds = await resolve_credentials(
                secret_store=self._secret_store,
                tenant_id=self._tenant_id,
                credential_kind=self._credential_kind,
                secret_ref=self._secret_ref,
            )
            return self._creds

    async def _signed_request(
        self,
        action: str,
        body: dict[str, Any],
    ) -> Any:
        """One SigV4-signed CloudTrail API call with bounded throttle retry.

        TODO(human): sign with botocore SigV4Auth / use a boto3 `cloudtrail`
        client. This MUST NEVER issue an unsigned request — until signing is
        wired it raises so the gap is loud. The synthetic gate replaces the whole
        client with `MockAwsClient`, so this stub never executes there.
        """
        creds = await self._credentials()  # noqa: F841 — wired with signing
        max_attempts = int(os.environ.get("AWS_RL_MAX_ATTEMPTS", "4"))
        max_sleep = float(os.environ.get("AWS_RL_MAX_SLEEP_SEC", "30"))
        _ = (max_attempts, max_sleep, action, body, self._endpoint)
        raise AwsApiError(
            "aws client SigV4 signing is not wired "
            "(TODO: sign with botocore SigV4Auth / boto3 cloudtrail client)",
            code="aws_api_error",
            context={"region": self._region},
        )

    # -----------------------------------------------------------------
    # Public read surface
    # -----------------------------------------------------------------

    async def list_events(
        self,
        *,
        account_id: str,
        region: str,
        from_ms: int | None = None,
        to_ms: int | None = None,
        cursor: str | None = None,
        limit: int = _DEFAULT_PAGE_SIZE,
    ) -> dict[str, Any]:
        """`CloudTrail:LookupEvents` — management events in the [from_ms, to_ms]
        window for one account/region.

        `from_ms` / `to_ms` are epoch MILLISECONDS; `cursor` is the opaque
        continuation token from the prior page (CloudTrail `NextToken`).
        Returns `{"events": [<event dict>, ...], "next_cursor": str | None}`,
        newest-first. Mirrors the mock client's contract so the fetcher's
        window walk terminates correctly.
        """
        body: dict[str, Any] = {
            "MaxResults": min(int(limit or _DEFAULT_PAGE_SIZE), _DEFAULT_PAGE_SIZE),
        }
        if from_ms is not None:
            body["StartTime"] = int(from_ms)
        if to_ms is not None:
            body["EndTime"] = int(to_ms)
        if cursor:
            body["NextToken"] = cursor
        resp = await self._signed_request("LookupEvents", body)
        events = resp.get("Events") if isinstance(resp, dict) else None
        events = [e for e in events if isinstance(e, dict)] if isinstance(events, list) else []
        next_cursor = resp.get("NextToken") if isinstance(resp, dict) else None
        return {
            "events": events,
            "next_cursor": next_cursor if isinstance(next_cursor, str) and next_cursor else None,
        }

    async def has_events_since(
        self, *, account_id: str, region: str, from_ms: int,
    ) -> bool:
        """Cheap reconciler gap probe: is there >=1 event with `eventTime` at/after
        `from_ms` (epoch ms)? The caller passes an EXCLUSIVE floor (high-water +
        1 ms) so the high-water event itself does not re-match."""
        page = await self.list_events(
            account_id=account_id, region=region, from_ms=from_ms, limit=1,
        )
        return len(page.get("events") or []) > 0

    async def describe_account(self) -> dict[str, Any]:
        """A cheap connectivity + credential probe used by the seed script
        (e.g. STS GetCallerIdentity).

        TODO(human): implement against STS GetCallerIdentity to verify the
        resolved credentials reach the target account/region.
        """
        raise AwsApiError(
            "aws describe_account probe is not wired (TODO: STS GetCallerIdentity)",
            code="aws_api_error",
            context={"region": self._region},
        )


__all__ = ["AwsClient", "AwsApiError", "short_account_hash", "CLOUDTRAIL_ENDPOINT_TEMPLATE"]
