from __future__ import annotations

from pathlib import Path

from lib.evaluation.epistemic_repair.hook_blindness import (
    DEFAULT_REGISTRY,
    REGISTRY_VERSION,
    Surface,
    assert_registry_is_versioned,
    scan_production_reachability,
    scan_text_surfaces,
    scan_trace_payloads,
)


def _write(root: Path, relative: str, source: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_registry_is_explicit_versioned_and_complete() -> None:
    assert REGISTRY_VERSION == "fyralis-hook-blindness-registry-v1"
    assert_registry_is_versioned()
    assert {item.fingerprint_id for item in DEFAULT_REGISTRY} == {
        "BH-001",
        "BH-002",
        "BH-003",
        "BH-004",
    }


def test_reachable_hook_call_is_reported(tmp_path: Path) -> None:
    _write(tmp_path, "app.py", "from internal.hooks import run\nrun()\n")
    _write(
        tmp_path,
        "internal/hooks.py",
        "def run():\n    return maybe_inject_capability_probe_ops()\n",
    )
    report = scan_production_reachability(tmp_path, ["app"])
    assert not report.is_hook_blind
    assert [(item.fingerprint_id, item.surface) for item in report.findings] == [
        ("BH-002", Surface.CALL_SITE)
    ]


def test_unreachable_quarantined_definition_is_not_reported(tmp_path: Path) -> None:
    _write(tmp_path, "app.py", "from internal.safe import run\nrun()\n")
    _write(tmp_path, "internal/safe.py", "def run():\n    return 'safe'\n")
    _write(
        tmp_path,
        "quarantine/hooks.py",
        "def run():\n    return maybe_inject_capability_probe_ops()\n",
    )
    report = scan_production_reachability(tmp_path, ["app"])
    assert report.is_hook_blind
    assert "quarantine.hooks" not in report.reachable_modules


def test_imported_but_uncalled_helper_is_not_reported(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "app.py",
        "from internal.hooks import maybe_inject_capability_probe_ops\n\n"
        "def run():\n    return 'safe'\n",
    )
    _write(
        tmp_path,
        "internal/hooks.py",
        "def maybe_inject_capability_probe_ops():\n    return None\n",
    )
    report = scan_production_reachability(tmp_path, ["app"])
    assert report.is_hook_blind


def test_generic_company_concepts_do_not_trigger_findings(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "app.py",
        "def explain():\n"
        "    return 'Pricing capability and bridge noise are company concepts.'\n",
    )
    report = scan_production_reachability(tmp_path, ["app"])
    assert report.is_hook_blind
    assert scan_text_surfaces(
        {"prompt": "Assess pricing transitions, capability gaps, and noisy data."}
    ) == ()


def test_prompt_or_output_requires_complete_distinctive_signature() -> None:
    partial = "General operational chatter may contain useful commitments."
    contaminated = (
        "General operational chatter with duplicated dashboard links was "
        "classified using discard_as_noise."
    )
    assert scan_text_surfaces({"partial": partial}) == ()
    findings = scan_text_surfaces({"output": contaminated})
    assert [(item.fingerprint_id, item.location) for item in findings] == [
        ("BH-004", "output")
    ]


def test_runtime_trace_payload_is_scanned_recursively_and_deterministically() -> None:
    events = [
        {"event": "think.completed", "payload": {"pricing": "normal"}},
        {
            "event": "think.noise_noop_fast_path",
            "payload": {
                "input": "general operational chatter; duplicated dashboard links",
                "decision": "discard_as_noise",
            },
        },
    ]
    findings = scan_trace_payloads(events)
    assert len(findings) == 1
    assert findings[0].fingerprint_id == "BH-004"
    assert findings[0].location == "trace[1]"


def test_every_registered_fingerprint_is_scannable_in_text_and_trace() -> None:
    for fingerprint in DEFAULT_REGISTRY:
        signature = " | ".join(fingerprint.anchors)
        text_ids = [
            item.fingerprint_id
            for item in scan_text_surfaces({"x": signature})
        ]
        assert text_ids == [fingerprint.fingerprint_id]
        trace_ids = [
            item.fingerprint_id
            for item in scan_trace_payloads([{"payload": signature}])
        ]
        assert trace_ids == [fingerprint.fingerprint_id]


def test_current_production_think_entrypoint_is_hook_blind() -> None:
    root = Path(__file__).resolve().parents[3]
    report = scan_production_reachability(
        root,
        ["services.reasoning.think.reason", "services.reasoning.think.worker"],
    )
    assert report.is_hook_blind, report.findings
