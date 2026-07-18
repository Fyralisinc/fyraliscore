"""Provider-free P0 regeneration from reopenable inventory members."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from typing import Any

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p9_contributions import (
    attach_p9_member_evidence,
    git_run_provenance,
)


P0_DIRECTORY = Path("docs/plans/epistemic-repair/p0")
P0_INVENTORIES = (
    "authority-writer-reader-inventory.json",
    "benchmark-hook-inventory.json",
    "truth-state-inventory.json",
    "telemetry-inventory.json",
    "evidence-inventory.json",
    "hard-gate-ownership-matrix.json",
)


def _read_json(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"P0 source must contain a JSON object: {path}")
    return value, {
        "path": str(path), "byte_count": len(raw),
        "sha256": canonical_sha256(value), "root_keys": sorted(value),
    }


def run_p0_regeneration(repository_root: Path) -> dict[str, Any]:
    root = repository_root.resolve()
    provenance = git_run_provenance(root)
    p0 = root / P0_DIRECTORY
    baseline, baseline_receipt = _read_json(
        p0 / "epistemic-repair-p0-baseline-v1.json"
    )
    inventory_receipts = []
    for name in P0_INVENTORIES:
        _, receipt = _read_json(p0 / name)
        inventory_receipts.append(receipt)
    preregistration_sources = (
        root / "lib/evaluation/epistemic_repair/preregistration.py",
        root / "tests/epistemic_repair/p0/test_preregistration_contract.py",
    )
    preregistration_receipts = [{
        "path": str(path), "byte_count": path.stat().st_size,
        "sha256": canonical_sha256(path.read_text(encoding="utf-8")),
    } for path in preregistration_sources]
    validation_run = subprocess.run(
        [
            sys.executable, "-m", "pytest", "tests/epistemic_repair/p0",
            "--ignore=tests/epistemic_repair/p0/test_p0_regeneration_runner.py",
            "-q",
        ],
        cwd=root, text=True, capture_output=True, check=False,
    )
    suite_receipt = {
        "validation_id": "p0_characterization_suite",
        "conforms": validation_run.returncode == 0,
        "returncode": validation_run.returncode,
        "raw_source_digest": canonical_sha256({
            "stdout": validation_run.stdout,
            "stderr": validation_run.stderr,
        }),
    }
    validation_receipts = [{
        "validation_id": "baseline_schema",
        "conforms": baseline.get("schema_version")
        == "epistemic-repair-p0-baseline-v1",
        "raw_source_digest": baseline_receipt["sha256"],
    }, *[{
        "validation_id": f"inventory:{Path(item['path']).name}",
        "conforms": bool(item["byte_count"] and item["root_keys"]),
        "raw_source_digest": item["sha256"],
    } for item in inventory_receipts], *[{
        "validation_id": f"preregistration:{Path(item['path']).name}",
        "conforms": item["byte_count"] > 0,
        "raw_source_digest": item["sha256"],
    } for item in preregistration_receipts], suite_receipt]
    artifact = {
        "schema_version": "epistemic-repair-p0-regeneration-v1",
        "execution_mode": "provider_free_static_reopen",
        "baseline_source": baseline_receipt,
        "inventory_receipts": inventory_receipts,
        "validation_receipts": validation_receipts,
        "preregistration_receipts": preregistration_receipts,
        "hard_gates": {
            "P0-baseline-integrity": validation_receipts[0]["conforms"],
            "P0-preregistration-integrity": all(
                item["conforms"] for item in validation_receipts
                if item["validation_id"].startswith("preregistration:")
            ) and suite_receipt["conforms"],
            "P0-inventory-completeness": len(inventory_receipts)
            == len(P0_INVENTORIES) and suite_receipt["conforms"],
        },
        "phase_exit_ready": all(item["conforms"] for item in validation_receipts),
        "proof_boundary": [
            "P0 reopens static characterization evidence only.",
            "No database or provider workload is executed.",
        ],
    }
    gate_members = {
        "P0-baseline-integrity": [{
            "member_id": "baseline-schema",
            "conforms": validation_receipts[0]["conforms"],
            "raw_source_digest": baseline_receipt["sha256"],
        }],
        "P0-preregistration-integrity": [{
            "member_id": item["validation_id"],
            "conforms": item["conforms"],
            "raw_source_digest": item["raw_source_digest"],
        } for item in validation_receipts
        if item["validation_id"].startswith("preregistration:")] + [{
            "member_id": "p0-characterization-suite",
            "conforms": suite_receipt["conforms"],
            "raw_source_digest": suite_receipt["raw_source_digest"],
        }],
        "P0-inventory-completeness": [{
            "member_id": f"inventory:{Path(item['path']).name}",
            "conforms": bool(item["byte_count"] and item["root_keys"]),
            "raw_source_digest": item["sha256"],
        } for item in inventory_receipts] + [{
            "member_id": "p0-characterization-suite",
            "conforms": suite_receipt["conforms"],
            "raw_source_digest": suite_receipt["raw_source_digest"],
        }],
    }
    return attach_p9_member_evidence(
        artifact, phase="p0", gate_members=gate_members, metric_members={},
        run_provenance=provenance,
    )


__all__ = ["P0_DIRECTORY", "P0_INVENTORIES", "run_p0_regeneration"]
