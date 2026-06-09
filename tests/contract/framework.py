"""Lightweight, strict loader for real-provider contract fixtures.

A fixture is a physical JSON file:

    tests/contract/fixtures/<provider>/<kind>/<name>.json

with `kind` in {webhook, api_response, oauth_token}. Every file MUST carry a
`_meta` block (provenance + a `sanitized: true` attestation) and the
kind-appropriate payload envelope:

  webhook       -> {"_meta": {...}, "request":  {"headers": {...}, "body": ...}}
  api_response  -> {"_meta": {...}, "response": {"status": int, "body": ...}}
  oauth_token   -> {"_meta": {...}, "request": {...}, "response": {...}}

The loader is intentionally strict: a malformed or un-sanitized fixture fails
the build rather than silently producing a misleading "contract verified" pass.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

FIXTURES_DIR = Path(__file__).parent / "fixtures"

KINDS = frozenset({"webhook", "api_response", "oauth_token"})
_REQUIRED_META = frozenset(
    {"provider", "kind", "description", "source", "captured_at", "sanitized"}
)


class ContractFixtureError(AssertionError):
    """A fixture file violates the contract-fixture schema."""


@dataclass(frozen=True)
class Fixture:
    path: Path
    data: dict[str, Any]  # full parsed file, including `_meta`

    @property
    def meta(self) -> dict[str, Any]:
        return self.data["_meta"]

    @property
    def provider(self) -> str:
        return self.meta["provider"]

    @property
    def kind(self) -> str:
        return self.meta["kind"]

    # --- webhook accessors -------------------------------------------------
    @property
    def request(self) -> dict[str, Any]:
        return self.data.get("request", {})

    @property
    def headers(self) -> dict[str, str]:
        return self.request.get("headers", {})

    @property
    def body(self) -> Any:
        return self.request.get("body")

    # --- api_response / oauth_token accessors ------------------------------
    @property
    def response(self) -> dict[str, Any]:
        return self.data.get("response", {})

    @property
    def response_body(self) -> Any:
        return self.response.get("body")


def validate_fixture(path: Path, data: dict[str, Any]) -> None:
    """Raise ContractFixtureError if `data` is not a well-formed fixture."""
    meta = data.get("_meta")
    if not isinstance(meta, dict):
        raise ContractFixtureError(f"{path}: missing `_meta` object")
    missing = _REQUIRED_META - meta.keys()
    if missing:
        raise ContractFixtureError(
            f"{path}: `_meta` missing required keys {sorted(missing)}"
        )
    kind = meta["kind"]
    if kind not in KINDS:
        raise ContractFixtureError(
            f"{path}: `_meta.kind`={kind!r} not in {sorted(KINDS)}"
        )
    if meta.get("sanitized") is not True:
        raise ContractFixtureError(
            f"{path}: `_meta.sanitized` must be true — scrub secrets/PII "
            f"(tokens, signing keys, real names/emails) before committing."
        )
    if kind == "webhook" and not isinstance(data.get("request"), dict):
        raise ContractFixtureError(f"{path}: webhook fixture needs a `request` object")
    if kind in ("api_response", "oauth_token") and not isinstance(
        data.get("response"), dict
    ):
        raise ContractFixtureError(
            f"{path}: {kind} fixture needs a `response` object"
        )


def _load_path(path: Path) -> Fixture:
    data = json.loads(path.read_text())
    validate_fixture(path, data)
    return Fixture(path=path, data=data)


def fixture_path(provider: str, kind: str, name: str) -> Path:
    return FIXTURES_DIR / provider / kind / f"{name}.json"


def has_fixture(provider: str, kind: str, name: str) -> bool:
    return fixture_path(provider, kind, name).is_file()


def load_fixture(provider: str, kind: str, name: str) -> Fixture:
    p = fixture_path(provider, kind, name)
    if not p.is_file():
        raise FileNotFoundError(f"contract fixture not found: {p}")
    return _load_path(p)


def iter_fixtures() -> Iterator[Fixture]:
    """Yield every fixture on disk (validating each)."""
    for p in sorted(FIXTURES_DIR.rglob("*.json")):
        yield _load_path(p)
