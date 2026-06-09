"""services/ingest/integrations/aws/client.py — outbound AWS API client (IN-AWS).

Single outbound surface for the CloudTrail management-events backfill +
poll-incremental + the reconciler's gap probe. AWS authenticates with IAM
credentials and SIGNS every request with SigV4. We use the `aioboto3`/botocore
service clients (`cloudtrail`, `sts`), which perform SigV4 signing, endpoint
resolution, and throttle-retry internally — rather than hand-rolling a SigV4Auth
signer. The synthetic gate drives the REAL fetcher against a MOCK client
(`mock_clients/aws.py`), so this production path is not exercised in CI;
integration-testing it against moto/localstack is the remaining operator step.

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
SigV4 signing
============================================================
Real AWS auth is IAM SigV4 over the per-region service endpoints. The aioboto3
service clients sign internally from the resolved `AwsCredentials` (static keys
or AssumeRole STS creds — see `credentials.py`); a botocore `Config` owns
throttle-retry (exponential backoff w/ jitter). `endpoint_override` points the
clients at moto/localstack for tests. CloudTrail LookupEvents is capped at 50
results/page and a 90-DAY lookback; STS GetCallerIdentity is the connectivity
probe.

Logging redaction: IAM credentials (access key / secret / session token) are
NEVER logged. The account id is hashed before logging.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import os
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import AwsApiError


log = structlog.get_logger("integrations.aws.client")


_DEFAULT_TIMEOUT_S = 30.0
# CloudTrail LookupEvents caps `MaxResults` at 50; keep parity.
_DEFAULT_PAGE_SIZE = 50

# CONFIRMED (docs.aws.amazon.com): CloudTrail's regional endpoint is
# https://cloudtrail.{region}.amazonaws.com. aioboto3 resolves it from
# region_name automatically; this constant is only used for logging + as the
# moto/localstack override seam (endpoint_url).
CLOUDTRAIL_ENDPOINT_TEMPLATE = os.environ.get(
    "AWS_CLOUDTRAIL_ENDPOINT_TEMPLATE",
    "https://cloudtrail.{region}.amazonaws.com",
)


# AwsApiError is the canonical class in lib/shared/errors.py (promoted from this
# module during the all-22 merge). Re-exported below for callers that import it
# from here. Stable `code` values: aws_api_throttled / aws_api_unauthorized /
# aws_api_not_found / aws_api_error.


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
        # Production endpoint is resolved per-region by the aioboto3 client; a
        # spammer/test override (moto/localstack) is passed as `endpoint_url`.
        self._endpoint_override = endpoint_override.rstrip("/") if endpoint_override else None
        self._endpoint = (
            self._endpoint_override
            or CLOUDTRAIL_ENDPOINT_TEMPLATE.format(region=region)
        )
        self._http = http_client

    async def aclose(self) -> None:
        """No durable client held by default; present for surface parity."""
        return None

    async def _credentials(self) -> Any:
        """Resolve IAM credentials (assume-role / static keys), refreshing
        AssumeRole creds before expiry. Static keys never expire → resolved once.
        """
        import time

        now_ms = int(time.time() * 1000)
        if self._creds is not None and not self._creds.expires_soon(now_ms):
            return self._creds
        async with self._creds_lock:
            now_ms = int(time.time() * 1000)
            if self._creds is not None and not self._creds.expires_soon(now_ms):
                return self._creds
            from services.ingest.integrations.aws.credentials import (
                resolve_credentials,
            )

            self._creds = await resolve_credentials(
                secret_store=self._secret_store,
                tenant_id=self._tenant_id,
                credential_kind=self._credential_kind,
                secret_ref=self._secret_ref,
                region=self._region,
            )
            return self._creds

    def _retry_config(self) -> Any:
        """botocore Config owning SigV4 throttle-retry (exp backoff + jitter)."""
        from botocore.config import Config

        return Config(
            retries={
                "max_attempts": int(os.environ.get("AWS_RL_MAX_ATTEMPTS", "4")),
                "mode": "standard",
            },
            read_timeout=_DEFAULT_TIMEOUT_S,
        )

    async def _service_client(self, service: str) -> Any:
        """An aioboto3 service client (`cloudtrail` / `sts`) signed from the
        resolved credentials. Returns the async-context-manager the caller enters.
        aioboto3 is an OPTIONAL dependency, imported lazily here so importing this
        module never requires it (the synthetic gate uses MockAwsClient)."""
        try:
            import aioboto3
        except ImportError as exc:  # pragma: no cover
            raise AwsApiError(
                "aioboto3 not installed for AWS API calls",
                code="aws_api_error",
                context={"region": self._region},
            ) from exc
        creds = await self._credentials()
        kwargs: dict[str, Any] = {
            "region_name": self._region,
            "aws_access_key_id": creds.access_key_id,
            "aws_secret_access_key": creds.secret_access_key,
            "aws_session_token": creds.session_token,
            "config": self._retry_config(),
        }
        if self._endpoint_override:
            kwargs["endpoint_url"] = self._endpoint_override
        return aioboto3.Session().client(service, **kwargs)

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
        # CloudTrail's LookupEvents takes tz-aware datetimes (NOT epoch ms) and
        # caps the lookback at 90 days; the fetcher clamps the window floor.
        kwargs: dict[str, Any] = {
            "MaxResults": min(int(limit or _DEFAULT_PAGE_SIZE), _DEFAULT_PAGE_SIZE),
        }
        if from_ms is not None:
            kwargs["StartTime"] = dt.datetime.fromtimestamp(
                from_ms / 1000, tz=dt.timezone.utc)
        if to_ms is not None:
            kwargs["EndTime"] = dt.datetime.fromtimestamp(
                to_ms / 1000, tz=dt.timezone.utc)
        if cursor:
            kwargs["NextToken"] = cursor
        try:
            client_cm = await self._service_client("cloudtrail")
            async with client_cm as ct:
                resp = await ct.lookup_events(**kwargs)
        except AwsApiError:
            raise
        except Exception as exc:  # noqa: BLE001 — map botocore ClientError.
            from services.ingest.integrations.aws.credentials import (
                _map_botocore_error,
            )
            raise _map_botocore_error(exc, region=region) from exc
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
        """A cheap connectivity + credential probe (STS GetCallerIdentity — a
        zero-permission call) used by the seed script to verify the resolved
        credentials reach the expected account."""
        try:
            client_cm = await self._service_client("sts")
            async with client_cm as sts:
                ident = await sts.get_caller_identity()
        except AwsApiError:
            raise
        except Exception as exc:  # noqa: BLE001 — map botocore ClientError.
            from services.ingest.integrations.aws.credentials import (
                _map_botocore_error,
            )
            raise _map_botocore_error(exc, region=self._region) from exc
        return {
            "account_id": ident.get("Account") if isinstance(ident, dict) else None,
            "arn": ident.get("Arn") if isinstance(ident, dict) else None,
            "user_id": ident.get("UserId") if isinstance(ident, dict) else None,
        }


__all__ = ["AwsClient", "AwsApiError", "short_account_hash", "CLOUDTRAIL_ENDPOINT_TEMPLATE"]
