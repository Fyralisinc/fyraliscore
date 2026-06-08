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

The whole module is a TODO-stubbed seam: the synthetic gate drives the REAL
fetcher against `MockAwsClient`, which never resolves credentials, so this is not
exercised there. It exists so the production `AwsClient._credentials()` call site
is real and stable, with the vendor-specific resolution isolated here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import structlog


log = structlog.get_logger("integrations.aws.credentials")


CREDENTIAL_KIND_ASSUME_ROLE = "assume_role"
CREDENTIAL_KIND_STATIC_KEYS = "static_keys"


@dataclass
class AwsCredentials:
    """Resolved IAM credentials for SigV4 signing. `session_token` is set only
    for AssumeRole-derived (STS) credentials; `expires_at_ms` drives refresh."""

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    expires_at_ms: int | None = None


async def resolve_credentials(
    *,
    secret_store: Any | None,
    tenant_id: UUID | None,
    credential_kind: str | None,
    secret_ref: str | None,
) -> AwsCredentials:
    """Resolve IAM credentials for one install.

    TODO(human): confirm the real resolution against vendor docs and wire it:
      - credential_kind == "assume_role": load the role ARN from the secret
        store via secret_ref, then STS AssumeRole (botocore) into the customer
        account and return the short-lived credentials with `expires_at_ms`.
      - credential_kind == "static_keys": load the access-key/secret pair from
        the secret store via secret_ref and return them (no session token).

    Until wired this raises so a production caller can never proceed with empty
    credentials; the synthetic gate never reaches here (MockAwsClient replaces
    the client surface).
    """
    from services.ingest.integrations.aws.client import AwsApiError

    raise AwsApiError(
        "aws credential resolution is not wired "
        f"(TODO: resolve credential_kind={credential_kind!r} via secret_store)",
        code="aws_api_unauthorized",
        context={"credential_kind": credential_kind},
    )


__all__ = [
    "AwsCredentials",
    "resolve_credentials",
    "CREDENTIAL_KIND_ASSUME_ROLE",
    "CREDENTIAL_KIND_STATIC_KEYS",
]
