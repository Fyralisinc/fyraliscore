from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

import lib.shared.secrets.provider_contract as provider_contract
from lib.shared.errors import SecretStoreError
from lib.shared.secrets import (
    FernetSecretStore,
    SecretProviderConfig,
    build_secret_store,
)
from lib.shared.secrets.provider_contract import (
    _extract_vault_secret_value,
    load_app_secret_text_from_env,
    load_master_kek_from_config,
    load_secret_bytes_from_config,
)


def test_provider_config_defaults_to_fernet_env() -> None:
    config = SecretProviderConfig.from_env({})

    assert config.secret_store_backend == "fernet"
    assert config.master_kek_provider == "env"
    assert config.master_kek_secret_ref is None


def test_provider_config_rejects_unknown_store_backend() -> None:
    with pytest.raises(SecretStoreError, match="SECRET_STORE_BACKEND"):
        SecretProviderConfig(secret_store_backend="plaintext").validate()


def test_provider_config_rejects_unknown_kek_provider() -> None:
    with pytest.raises(SecretStoreError, match="MASTER_KEK_PROVIDER"):
        SecretProviderConfig(master_kek_provider="local-file").validate()


def test_provider_config_rejects_env_kek_provider_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "prod")

    with pytest.raises(SecretStoreError, match="MASTER_KEK_PROVIDER=env"):
        SecretProviderConfig(master_kek_provider="env").validate()


def test_provider_config_requires_secret_ref_for_managed_provider() -> None:
    with pytest.raises(SecretStoreError, match="MASTER_KEK_SECRET_REF"):
        SecretProviderConfig(master_kek_provider="aws-secrets-manager").validate()


def test_provider_config_requires_vault_endpoint_and_token() -> None:
    with pytest.raises(SecretStoreError, match="SECRET_PROVIDER_ENDPOINT"):
        SecretProviderConfig(
            master_kek_provider="hashicorp-vault",
            master_kek_secret_ref="secret/data/fyralis/master-kek",
        ).validate()
    with pytest.raises(SecretStoreError, match="VAULT_TOKEN"):
        SecretProviderConfig(
            master_kek_provider="hashicorp-vault",
            master_kek_secret_ref="secret/data/fyralis/master-kek",
            endpoint_url="https://vault.example",
        ).validate()


def test_provider_config_from_env_accepts_managed_ref() -> None:
    config = SecretProviderConfig.from_env(
        {
            "SECRET_STORE_BACKEND": "fernet",
            "MASTER_KEK_PROVIDER": "aws-secrets-manager",
            "MASTER_KEK_SECRET_REF": "prod/fyralis/master-kek",
            "SECRET_PROVIDER_REGION": "us-east-1",
        }
    )

    assert config.secret_store_backend == "fernet"
    assert config.master_kek_provider == "aws-secrets-manager"
    assert config.master_kek_secret_ref == "prod/fyralis/master-kek"
    assert config.region == "us-east-1"


def test_load_master_kek_from_env_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = Fernet.generate_key().decode("ascii")
    monkeypatch.setenv("MASTER_KEK", key)

    assert load_master_kek_from_config(SecretProviderConfig()) == key.encode("ascii")


def test_load_secret_bytes_uses_requested_app_secret_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, str | None] = {}

    def _fake_aws_loader(config: SecretProviderConfig) -> bytes:
        observed["ref"] = config.master_kek_secret_ref
        return b"managed-secret"

    monkeypatch.setattr(
        provider_contract,
        "_load_from_aws_secrets_manager",
        _fake_aws_loader,
    )

    value = load_secret_bytes_from_config(
        "prod/fyralis/slack-client-secret",
        SecretProviderConfig(
            master_kek_provider="aws-secrets-manager",
            master_kek_secret_ref="prod/fyralis/master-kek",
        ),
    )

    assert value == b"managed-secret"
    assert observed["ref"] == "prod/fyralis/slack-client-secret"


def test_app_secret_ref_wins_over_plaintext_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_aws_loader(config: SecretProviderConfig) -> bytes:
        assert config.master_kek_secret_ref == "prod/fyralis/slack-client-secret"
        return b"managed-client-secret"

    monkeypatch.setattr(
        provider_contract,
        "_load_from_aws_secrets_manager",
        _fake_aws_loader,
    )

    value = load_app_secret_text_from_env(
        "SLACK_CLIENT_SECRET",
        env={
            "SECRET_STORE_BACKEND": "fernet",
            "MASTER_KEK_PROVIDER": "aws-secrets-manager",
            "MASTER_KEK_SECRET_REF": "prod/fyralis/master-kek",
            "SLACK_CLIENT_SECRET": "plaintext-should-not-win",
            "SLACK_CLIENT_SECRET_SECRET_REF": "prod/fyralis/slack-client-secret",
        },
        production=True,
    )

    assert value == "managed-client-secret"


def test_app_secret_plaintext_rejected_in_production() -> None:
    with pytest.raises(SecretStoreError, match="SLACK_CLIENT_SECRET"):
        load_app_secret_text_from_env(
            "SLACK_CLIENT_SECRET",
            env={"SLACK_CLIENT_SECRET": "raw-secret"},
            production=True,
        )


def test_app_secret_plaintext_allowed_outside_production() -> None:
    assert (
        load_app_secret_text_from_env(
            "SLACK_CLIENT_SECRET",
            env={"SLACK_CLIENT_SECRET": "local-secret"},
            production=False,
        )
        == "local-secret"
    )


def test_build_secret_store_uses_provider_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MASTER_KEK", Fernet.generate_key().decode("ascii"))

    store = build_secret_store(  # type: ignore[arg-type]
        pool=None,
        provider_config=SecretProviderConfig(),
    )

    assert isinstance(store, FernetSecretStore)


def test_extract_vault_secret_value_accepts_kv_v2_shape() -> None:
    assert (
        _extract_vault_secret_value(
            {"data": {"data": {"MASTER_KEK": "vault-kek"}}}
        )
        == "vault-kek"
    )


def test_extract_vault_secret_value_accepts_flat_value_shape() -> None:
    assert _extract_vault_secret_value({"data": {"value": "vault-kek"}}) == "vault-kek"


def test_extract_vault_secret_value_rejects_missing_secret() -> None:
    with pytest.raises(SecretStoreError):
        _extract_vault_secret_value({"data": {"data": {}}})
