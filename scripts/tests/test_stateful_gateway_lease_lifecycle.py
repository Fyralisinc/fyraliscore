from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

STATEFUL_GATEWAY_LAUNCHERS = (
    "scripts/run_signal_gateway_worker.py",
    "scripts/run_telegram_gateway_worker.py",
)


def test_stateful_gateway_launchers_handle_lease_loss_and_release() -> None:
    missing: list[str] = []
    for relative in STATEFUL_GATEWAY_LAUNCHERS:
        source = (REPO_ROOT / relative).read_text(encoding="utf-8")
        required_fragments = {
            "lease_lost flag": "lease_lost = False",
            "lease_lost set": "lease_lost = True",
            "transient exit": "return 3 if lease_lost else 0",
            "lease release": "await lock.release()",
        }
        for label, fragment in required_fragments.items():
            if fragment not in source:
                missing.append(f"{relative}: missing {label}")

    assert missing == []
