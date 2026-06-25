"""Secret provider contract for production key material.

Fyralis currently stores per-tenant source credentials in Postgres via
``FernetSecretStore``. This module defines how the wrapping key for that store
is resolved in production so deployments can move the key material from process
environment variables to managed secret providers without changing every
``secret_ref`` caller.
"""
from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Mapping

from lib.shared.errors import SecretStoreError
from lib.shared.env import is_prod


SECRET_STORE_BACKEND_ENV = "SECRET_STORE_BACKEND"
MASTER_KEK_PROVIDER_ENV = "MASTER_KEK_PROVIDER"
MASTER_KEK_SECRET_REF_ENV = "MASTER_KEK_SECRET_REF"
SECRET_PROVIDER_REGION_ENV = "SECRET_PROVIDER_REGION"
SECRET_PROVIDER_ENDPOINT_ENV = "SECRET_PROVIDER_ENDPOINT"
SECRET_PROVIDER_TIMEOUT_SECONDS_ENV = "SECRET_PROVIDER_TIMEOUT_SECONDS"
VAULT_TOKEN_ENV = "VAULT_TOKEN"

SUPPORTED_SECRET_STORE_BACKENDS = frozenset({"fernet"})
SUPPORTED_MASTER_KEK_PROVIDERS = frozenset(
    {"env", "aws-secrets-manager", "gcp-secret-manager", "hashicorp-vault"}
)


@dataclass(frozen=True)
class SecretProviderConfig:
    """Configuration for secret-store backend and wrapping-key provider."""

    secret_store_backend: str = "fernet"
    master_kek_provider: str = "env"
    master_kek_secret_ref: str | None = None
    region: str | None = None
    endpoint_url: str | None = None
    timeout_seconds: float = 5.0
    vault_token: str | None = field(default=None, repr=False)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
    ) -> "SecretProviderConfig":
        source = os.environ if env is None else env
        timeout_raw = source.get(SECRET_PROVIDER_TIMEOUT_SECONDS_ENV, "5")
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError as exc:
            raise SecretStoreError(
                "SECRET_PROVIDER_TIMEOUT_SECONDS must be numeric",
                reason="invalid_secret_provider_config",
            ) from exc
        return cls(
            secret_store_backend=source.get(SECRET_STORE_BACKEND_ENV, "fernet"),
            master_kek_provider=source.get(MASTER_KEK_PROVIDER_ENV, "env"),
            master_kek_secret_ref=source.get(MASTER_KEK_SECRET_REF_ENV) or None,
            region=source.get(SECRET_PROVIDER_REGION_ENV) or None,
            endpoint_url=source.get(SECRET_PROVIDER_ENDPOINT_ENV) or None,
            timeout_seconds=timeout_seconds,
            vault_token=source.get(VAULT_TOKEN_ENV) or None,
        ).validate()

    def validate(self) -> "SecretProviderConfig":
        if self.secret_store_backend not in SUPPORTED_SECRET_STORE_BACKENDS:
            raise SecretStoreError(
                (
                    "unsupported SECRET_STORE_BACKEND "
                    f"{self.secret_store_backend!r}"
                ),
                reason="unsupported_secret_store_backend",
            )
        if self.master_kek_provider not in SUPPORTED_MASTER_KEK_PROVIDERS:
            raise SecretStoreError(
                f"unsupported MASTER_KEK_PROVIDER {self.master_kek_provider!r}",
                reason="unsupported_master_kek_provider",
            )
        if is_prod() and self.master_kek_provider == "env":
            raise SecretStoreError(
                (
                    "MASTER_KEK_PROVIDER=env is not allowed in production; "
                    "use aws-secrets-manager, gcp-secret-manager, or "
                    "hashicorp-vault"
                ),
                reason="env_master_kek_forbidden_in_production",
            )
        if self.master_kek_provider != "env" and not self.master_kek_secret_ref:
            raise SecretStoreError(
                (
                    "MASTER_KEK_SECRET_REF is required when "
                    "MASTER_KEK_PROVIDER is managed"
                ),
                reason="missing_master_kek_secret_ref",
            )
        if self.master_kek_provider == "hashicorp-vault":
            if not self.endpoint_url:
                raise SecretStoreError(
                    "SECRET_PROVIDER_ENDPOINT is required for HashiCorp Vault",
                    reason="missing_secret_provider_endpoint",
                )
            if not self.vault_token:
                raise SecretStoreError(
                    "VAULT_TOKEN is required for HashiCorp Vault",
                    reason="missing_vault_token",
                )
        if self.timeout_seconds <= 0:
            raise SecretStoreError(
                "SECRET_PROVIDER_TIMEOUT_SECONDS must be positive",
                reason="invalid_secret_provider_config",
            )
        return self


def load_master_kek_from_config(
    config: SecretProviderConfig | None = None,
) -> bytes:
    """Load the Fernet wrapping key from the configured provider."""

    cfg = SecretProviderConfig.from_env() if config is None else config.validate()
    if cfg.master_kek_provider == "env":
        env_val = os.environ.get("MASTER_KEK", "")
        return env_val.encode("ascii") if env_val else b""
    if cfg.master_kek_provider == "aws-secrets-manager":
        return _load_from_aws_secrets_manager(cfg)
    if cfg.master_kek_provider == "gcp-secret-manager":
        return _load_from_gcp_secret_manager(cfg)
    if cfg.master_kek_provider == "hashicorp-vault":
        return _load_from_hashicorp_vault(cfg)
    raise SecretStoreError(
        f"unsupported MASTER_KEK_PROVIDER {cfg.master_kek_provider!r}",
        reason="unsupported_master_kek_provider",
    )


def _load_from_aws_secrets_manager(config: SecretProviderConfig) -> bytes:
    try:
        import boto3  # type: ignore[import-not-found]
    except ImportError as exc:
        raise SecretStoreError(
            "boto3 is required for MASTER_KEK_PROVIDER=aws-secrets-manager",
            reason="missing_secret_provider_sdk",
        ) from exc

    client = boto3.client(
        "secretsmanager",
        region_name=config.region,
        endpoint_url=config.endpoint_url,
    )
    response = client.get_secret_value(SecretId=config.master_kek_secret_ref)
    if "SecretString" in response and response["SecretString"]:
        return str(response["SecretString"]).encode("ascii")
    if "SecretBinary" in response and response["SecretBinary"]:
        secret_binary = response["SecretBinary"]
        if isinstance(secret_binary, str):
            return base64.b64decode(secret_binary)
        return bytes(secret_binary)
    raise SecretStoreError(
        "AWS Secrets Manager response did not include secret material",
        reason="secret_provider_empty_response",
    )


def _load_from_gcp_secret_manager(config: SecretProviderConfig) -> bytes:
    try:
        from google.cloud import secretmanager  # type: ignore[import-not-found]
    except ImportError as exc:
        raise SecretStoreError(
            "google-cloud-secret-manager is required for "
            "MASTER_KEK_PROVIDER=gcp-secret-manager",
            reason="missing_secret_provider_sdk",
        ) from exc

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(
        request={"name": config.master_kek_secret_ref}
    )
    return bytes(response.payload.data)


def _extract_vault_secret_value(payload: Mapping[str, object]) -> str:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise SecretStoreError(
            "Vault response did not include data",
            reason="secret_provider_empty_response",
        )
    nested = data.get("data")
    candidate = nested if isinstance(nested, Mapping) else data
    for key in ("MASTER_KEK", "master_kek", "value"):
        value = candidate.get(key)
        if isinstance(value, str) and value:
            return value
    raise SecretStoreError(
        "Vault response did not include a supported key field",
        reason="secret_provider_empty_response",
    )


def _load_from_hashicorp_vault(config: SecretProviderConfig) -> bytes:
    assert config.endpoint_url is not None
    assert config.master_kek_secret_ref is not None
    assert config.vault_token is not None
    url = (
        config.endpoint_url.rstrip("/")
        + "/v1/"
        + config.master_kek_secret_ref.lstrip("/")
    )
    request = urllib.request.Request(
        url,
        headers={"X-Vault-Token": config.vault_token},
        method="GET",
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=config.timeout_seconds,
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise SecretStoreError(
            "HashiCorp Vault secret read failed",
            reason="secret_provider_read_failed",
        ) from exc
    return _extract_vault_secret_value(payload).encode("ascii")


__all__ = [
    "MASTER_KEK_PROVIDER_ENV",
    "MASTER_KEK_SECRET_REF_ENV",
    "SECRET_PROVIDER_ENDPOINT_ENV",
    "SECRET_PROVIDER_REGION_ENV",
    "SECRET_PROVIDER_TIMEOUT_SECONDS_ENV",
    "SECRET_STORE_BACKEND_ENV",
    "SUPPORTED_MASTER_KEK_PROVIDERS",
    "SUPPORTED_SECRET_STORE_BACKENDS",
    "SecretProviderConfig",
    "VAULT_TOKEN_ENV",
    "load_master_kek_from_config",
]
