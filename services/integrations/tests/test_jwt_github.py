"""IN-13 unit tests for `services/integrations/github/jwt.py`.

Covers tasks T009–T012 from specs/IN-13-github-integration/tasks.md.
No DB required.
"""
from __future__ import annotations

import os

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt as pyjwt

from lib.shared.errors import GithubJWTError
from services.integrations.github.jwt import mint_app_jwt


def _make_keypair() -> tuple[bytes, bytes]:
    """Generate a throwaway RSA-2048 keypair. Returns (private_pem, public_pem)."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return priv, pub


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear GitHub App env vars between tests."""
    monkeypatch.delenv("GITHUB_APP_ID", raising=False)
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY_PATH", raising=False)


def test_mint_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    """T009: mint a JWT with a known key; verify with the matching public key."""
    priv, pub = _make_keypair()
    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", priv.decode())

    token = mint_app_jwt()

    decoded = pyjwt.decode(
        token, pub, algorithms=["RS256"], options={"verify_iat": False},
    )
    assert decoded["iss"] == "999999"
    assert "iat" in decoded
    assert "exp" in decoded
    # Exp should be ~10 minutes ahead of iat.
    assert decoded["exp"] - decoded["iat"] >= 600


def test_missing_app_id_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """T010: GITHUB_APP_ID missing → GithubJWTError(reason='no_app_id')."""
    priv, _ = _make_keypair()
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", priv.decode())

    with pytest.raises(GithubJWTError) as exc:
        mint_app_jwt()
    assert exc.value.reason == "no_app_id"


def test_missing_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """T010: neither private-key env var set → reason='no_private_key'."""
    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    with pytest.raises(GithubJWTError) as exc:
        mint_app_jwt()
    assert exc.value.reason == "no_private_key"


def test_conflicting_keys_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """T010: both env vars set → reason='conflicting_keys'."""
    priv, _ = _make_keypair()
    pem_path = tmp_path / "key.pem"
    pem_path.write_bytes(priv)

    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", priv.decode())
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY_PATH", str(pem_path))

    with pytest.raises(GithubJWTError) as exc:
        mint_app_jwt()
    assert exc.value.reason == "conflicting_keys"


def test_io_error_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """T010 extension: PATH points to a nonexistent file → reason='io_error'."""
    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    monkeypatch.setenv(
        "GITHUB_APP_PRIVATE_KEY_PATH", "/tmp/IN-13-nonexistent-key.pem",
    )

    with pytest.raises(GithubJWTError) as exc:
        mint_app_jwt()
    assert exc.value.reason == "io_error"


def test_malformed_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """T010 extension: invalid PEM material → reason='malformed_key'."""
    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "not-a-pem")

    with pytest.raises(GithubJWTError) as exc:
        mint_app_jwt()
    assert exc.value.reason == "malformed_key"


def test_rotation_transparent(monkeypatch: pytest.MonkeyPatch) -> None:
    """T011: changing the private key between mints produces a token
    that verifies under the new public key only — no in-process cache
    binding to the prior key."""
    priv1, pub1 = _make_keypair()
    priv2, pub2 = _make_keypair()

    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", priv1.decode())

    tok1 = mint_app_jwt()
    pyjwt.decode(tok1, pub1, algorithms=["RS256"], options={"verify_iat": False})

    # Rotate.
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", priv2.decode())
    tok2 = mint_app_jwt()
    pyjwt.decode(tok2, pub2, algorithms=["RS256"], options={"verify_iat": False})

    with pytest.raises(pyjwt.InvalidSignatureError):
        pyjwt.decode(
            tok2, pub1, algorithms=["RS256"],
            options={"verify_iat": False},
        )


def test_private_key_never_logged(
    monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """T012: minting must not emit PEM material or signature bytes via
    any log channel. The JWT module itself does not log; this test
    verifies no print/log accidentally leaks the key.
    """
    priv, _ = _make_keypair()
    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", priv.decode())

    token = mint_app_jwt()

    captured = capsys.readouterr()
    pem_marker = "-----BEGIN PRIVATE KEY-----"
    assert pem_marker not in captured.out
    assert pem_marker not in captured.err
    # The encoded token's signature portion is also sensitive; it MUST
    # NOT appear in any output of the mint call itself.
    sig_b64 = token.rsplit(".", 1)[-1]
    assert sig_b64 not in captured.out
    assert sig_b64 not in captured.err
