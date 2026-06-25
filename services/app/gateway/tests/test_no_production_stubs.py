from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
GATEWAY = ROOT / "app/gateway"


def test_gateway_production_code_does_not_return_stub_true() -> None:
    offenders: list[str] = []
    for path in GATEWAY.rglob("*.py"):
        if "/tests/" in path.as_posix():
            continue
        text = path.read_text(encoding="utf-8")
        if '"stub": True' in text or "'stub': True" in text:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []


def test_spec_seed_routes_are_isolated_to_demo_router() -> None:
    offenders: list[str] = []
    for path in GATEWAY.rglob("*.py"):
        if "/tests/" in path.as_posix() or path.name == "spec_routes.py":
            continue
        text = path.read_text(encoding="utf-8")
        if "/v1/spec/" in text or "/v1/spec" in text:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
