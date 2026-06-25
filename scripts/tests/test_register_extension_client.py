from __future__ import annotations

import argparse
import json
import stat

import pytest

from scripts.register_extension_client import (
    ExtensionClientCliError,
    _secret_output_path,
    _write_secret_file,
)


def test_write_secret_file_uses_owner_only_permissions(tmp_path) -> None:
    out = tmp_path / "extension-client.json"

    _write_secret_file(
        out,
        {
            "client_id": "ext_test",
            "client_secret": "secret-value",
            "webhook_secret": "whsec_secret",
        },
    )

    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    assert json.loads(out.read_text(encoding="utf-8")) == {
        "client_id": "ext_test",
        "client_secret": "secret-value",
        "webhook_secret": "whsec_secret",
    }


def test_write_secret_file_refuses_existing_file_without_overwrite(tmp_path) -> None:
    out = tmp_path / "extension-client.json"
    out.write_text("existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        _write_secret_file(out, {"client_secret": "new-secret"})

    assert out.read_text(encoding="utf-8") == "existing\n"


def test_secret_output_path_is_required_for_secret_generating_commands() -> None:
    args = argparse.Namespace(secret_output_file=None)

    with pytest.raises(ExtensionClientCliError, match="secret-output-file"):
        _secret_output_path(args)


def test_secret_output_path_rejects_existing_file_without_overwrite(tmp_path) -> None:
    out = tmp_path / "extension-client.json"
    out.write_text("existing\n", encoding="utf-8")
    args = argparse.Namespace(
        secret_output_file=str(out),
        overwrite_secret_file=False,
    )

    with pytest.raises(ExtensionClientCliError, match="already exists"):
        _secret_output_path(args)
