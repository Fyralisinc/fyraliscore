"""services/ingest/integrations/aws/credentials.py — IAM credential resolution (IN-AWS).

Resolves the IAM credentials the `AwsClient` uses to SigV4-sign CloudTrail calls.
AWS installs come in two flavours (the `aws_installations.credential_kind`
column):

  - "assume_role"  — the install stores a cross-account ROLE ARN (in secret_ref /
                     the secret store); we AssumeRole into the customer account
                     to obtain short-lived STS credentials, refreshing before
                     expiry. This is the recommended posture (no long-lived keys).
  - "static_keys"  — the install stores a long-lived access-key/secret pair in
                     the secret store; resolved once and reused.

The synthetic gate drives the REAL fetcher against `MockAwsClient`, which never
resolves credentials, so this is not exercised there. The production path below
is wired per AWS docs (botocore/aioboto3); integration-testing it against
moto/localstack is the remaining operator step.

Secret-store payload (resolved via `secret_ref`), a JSON blob:
  - static_keys:  {"access_key_id": "...", "secret_access_key": "...",
                   "session_token"?: "..."}
  - assume_role:  {"role_arn": "arn:aws:iam::<acct>:role/<name>",
                   "external_id"?: "...", "duration_seconds"?: 3600}
                  (a bare ARN string is also accepted). The integration's OWN base
                  identity (botocore default chain: env/instance/SSO) must be
                  allowed to sts:AssumeRole that role.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import AwsApiError
from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    parse_retry_after,
)


log = structlog.get_logger("integrations.aws.credentials")


CREDENTIAL_KIND_ASSUME_ROLE = "assume_role"
CREDENTIAL_KIND_STATIC_KEYS = "static_keys"

# Refresh AssumeRole creds this many ms before their stated expiry.
_REFRESH_SKEW_MS = 5 * 60 * 1000


@dataclass
class AwsCredentials:
    """Resolved IAM credentials for SigV4 signing. `session_token` is set only
    for AssumeRole-derived (STS) credentials; `expires_at_ms` drives refresh."""

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    expires_at_ms: int | None = None

    def expires_soon(self, now_ms: int) -> bool:
        """True when these creds are within the refresh skew of expiry (static
        keys never expire → always False)."""
        return self.expires_at_ms is not None and (
            self.expires_at_ms - now_ms <= _REFRESH_SKEW_MS
        )


async def _load_material(
    secret_store: Any, tenant_id: UUID, secret_ref: str,
) -> dict[str, Any]:
    import json

    raw = await secret_store.get(secret_ref, tenant_id=tenant_id)
    blob = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
    try:
        parsed = json.loads(blob)
        return parsed if isinstance(parsed, dict) else {"role_arn": blob.strip()}
    except (ValueError, TypeError):
        # A bare value (e.g. a role ARN) rather than a JSON blob.
        return {"role_arn": blob.strip()}


async def resolve_credentials(
    *,
    secret_store: Any | None,
    tenant_id: UUID | None,
    credential_kind: str | None,
    secret_ref: str | None,
    region: str | None = None,
    request_binding: Any | None = None,
    endpoint_override: str | None = None,
    botocore_config: Any | None = None,
) -> AwsCredentials:
    """Resolve IAM credentials for one install.

      - credential_kind == "static_keys": load the access-key/secret pair from
        the secret store via secret_ref and return them (no session token).
      - credential_kind == "assume_role": load the role ARN from the secret store
        via secret_ref, then STS AssumeRole (aioboto3, signed by the integration's
        base identity) and return the short-lived credentials with `expires_at_ms`.

    Raises `AwsApiError(code="aws_api_unauthorized")` rather than returning empty
    credentials so a production caller can never issue an unsigned/anonymous call.
    """
    if not secret_ref or secret_store is None or tenant_id is None:
        raise AwsApiError(
            "aws credential resolution missing secret_ref/secret_store/tenant",
            code="aws_api_unauthorized",
            context={"credential_kind": credential_kind},
        )

    material = await _load_material(secret_store, tenant_id, secret_ref)
    kind = credential_kind or CREDENTIAL_KIND_STATIC_KEYS

    if kind == CREDENTIAL_KIND_STATIC_KEYS:
        access_key = material.get("access_key_id") or material.get("aws_access_key_id")
        secret_key = material.get("secret_access_key") or material.get("aws_secret_access_key")
        if not (access_key and secret_key):
            raise AwsApiError(
                "aws static_keys secret missing access_key_id/secret_access_key",
                code="aws_api_unauthorized",
                context={"credential_kind": kind},
            )
        return AwsCredentials(
            access_key_id=str(access_key),
            secret_access_key=str(secret_key),
            session_token=material.get("session_token") or material.get("aws_session_token"),
        )

    if kind == CREDENTIAL_KIND_ASSUME_ROLE:
        role_arn = material.get("role_arn") or material.get("arn")
        if not role_arn:
            raise AwsApiError(
                "aws assume_role secret missing role_arn",
                code="aws_api_unauthorized",
                context={"credential_kind": kind},
            )
        try:
            import aioboto3  # deferred — optional/heavy dependency.
        except ImportError as exc:  # pragma: no cover
            raise AwsApiError(
                "aioboto3 not installed for AssumeRole credential resolution",
                code="aws_api_error",
                context={"credential_kind": kind},
            ) from exc
        kwargs: dict[str, Any] = {
            "RoleArn": str(role_arn),
            "RoleSessionName": str(material.get("session_name") or "fyralis-ingest"),
        }
        if material.get("external_id"):
            kwargs["ExternalId"] = str(material["external_id"])
        if material.get("duration_seconds"):
            kwargs["DurationSeconds"] = int(material["duration_seconds"])
        async def _once() -> Any:
            client_kwargs: dict[str, Any] = {"region_name": region}
            if endpoint_override:
                client_kwargs["endpoint_url"] = endpoint_override
            if botocore_config is not None:
                client_kwargs["config"] = botocore_config
            try:
                session = aioboto3.Session()
                async with session.client("sts", **client_kwargs) as sts:
                    return await sts.assume_role(**kwargs)
            except (
                ProviderPermanentError,
                ProviderRateLimited,
                ProviderTransientError,
            ):
                raise
            except Exception as exc:  # noqa: BLE001 — botocore taxonomy.
                raise _map_botocore_provider_error(
                    exc,
                    region=region,
                    operation="sts.assume_role",
                ) from exc

        try:
            resp = (
                await request_binding.execute("sts.assume_role", _once)
                if request_binding is not None
                else await _once()
            )
        except ProviderPermanentError as exc:
            raise _provider_permanent_to_aws(
                exc,
                region=region,
                operation="sts.assume_role",
            ) from exc
        creds = resp.get("Credentials") or {}
        expiration = creds.get("Expiration")
        expires_at_ms = (
            int(expiration.timestamp() * 1000) if expiration is not None else None
        )
        return AwsCredentials(
            access_key_id=str(creds["AccessKeyId"]),
            secret_access_key=str(creds["SecretAccessKey"]),
            session_token=str(creds["SessionToken"]),
            expires_at_ms=expires_at_ms,
        )

    raise AwsApiError(
        f"aws unknown credential_kind={kind!r}",
        code="aws_api_unauthorized",
        context={"credential_kind": kind},
    )


def _map_botocore_error(exc: Exception, *, region: str | None) -> Exception:
    """Map a botocore ClientError to the shared AwsApiError taxonomy. Falls back
    to a generic AwsApiError for non-botocore failures (transport, etc.)."""
    code = "aws_api_error"
    http_status = None
    err_code = None
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        err_code = (response.get("Error") or {}).get("Code")
        http_status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
    if err_code in ("AccessDenied", "UnrecognizedClientException",
                    "InvalidClientTokenId", "SignatureDoesNotMatch",
                    "ExpiredToken", "ExpiredTokenException"):
        code = "aws_api_unauthorized"
    elif err_code in ("Throttling", "ThrottlingException", "ThrottledException",
                      "RequestLimitExceeded", "TooManyRequestsException",
                      "RequestThrottled", "RequestThrottledException", "SlowDown"):
        code = "aws_api_throttled"
    elif err_code in ("NoSuchEntity", "ResourceNotFoundException"):
        code = "aws_api_not_found"
    return AwsApiError(
        f"aws STS/credential call failed: {err_code or type(exc).__name__}",
        code=code,
        context={"region": region, "http_status": http_status, "aws_code": err_code},
    )


def _map_botocore_provider_error(
    exc: Exception,
    *,
    region: str | None,
    operation: str,
) -> Exception:
    """Translate one botocore attempt into ProviderTransport's taxonomy."""
    response = getattr(exc, "response", None)
    err_code = None
    http_status = None
    headers: dict[str, Any] = {}
    if isinstance(response, dict):
        err_code = (response.get("Error") or {}).get("Code")
        metadata = response.get("ResponseMetadata") or {}
        http_status = metadata.get("HTTPStatusCode")
        raw_headers = metadata.get("HTTPHeaders")
        if isinstance(raw_headers, dict):
            headers = raw_headers

    context: dict[str, Any] = {
        "source": "aws",
        "operation": operation,
        "region": region,
        **({"status_code": http_status} if http_status is not None else {}),
        **({"aws_code": err_code} if err_code is not None else {}),
    }
    class_name = type(exc).__name__
    if class_name in {
        "ConnectTimeoutError",
        "ReadTimeoutError",
        "ReadTimeout",
    }:
        return ProviderTimeoutError("AWS request timed out", **context)
    if err_code in {
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "RequestLimitExceeded",
        "TooManyRequestsException",
        "RequestThrottled",
        "RequestThrottledException",
        "SlowDown",
    } or http_status == 429:
        return ProviderRateLimited(
            "AWS request throttled",
            retry_after_seconds=parse_retry_after(
                headers.get("retry-after")
                or headers.get("x-amz-retry-after")
            ),
            status_code=http_status or 429,
            header_parser_id="aws.sdk_throttle_headers",
            **{
                key: value
                for key, value in context.items()
                if key != "status_code"
            },
        )
    if (
        isinstance(http_status, int)
        and http_status >= 500
    ) or class_name in {
        "EndpointConnectionError",
        "ConnectionClosedError",
        "HTTPClientError",
        "ProxyConnectionError",
    }:
        return ProviderTransientError(
            "AWS service or transport unavailable",
            error_type=class_name,
            **context,
        )
    return ProviderPermanentError(
        "AWS request rejected",
        error_type=class_name,
        **context,
    )


def _provider_permanent_to_aws(
    exc: ProviderPermanentError,
    *,
    region: str | None,
    operation: str,
) -> AwsApiError:
    status = exc.context.get("status_code")
    aws_code = exc.context.get("aws_code")
    code = "aws_api_error"
    if status in {401, 403} or aws_code in {
        "AccessDenied",
        "UnrecognizedClientException",
        "InvalidClientTokenId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "ExpiredTokenException",
    }:
        code = "aws_api_unauthorized"
    elif status == 404:
        code = "aws_api_not_found"
    return AwsApiError(
        exc.message,
        code=code,
        context={
            "region": region,
            "operation": operation,
            **({"http_status": status} if status is not None else {}),
            **({"aws_code": aws_code} if aws_code is not None else {}),
        },
    )


__all__ = [
    "AwsCredentials",
    "resolve_credentials",
    "CREDENTIAL_KIND_ASSUME_ROLE",
    "CREDENTIAL_KIND_STATIC_KEYS",
    "_map_botocore_error",
    "_map_botocore_provider_error",
]
