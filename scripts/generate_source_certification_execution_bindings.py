#!/usr/bin/env python3
"""Generate one hash-pinned execution binding per canonical source."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.ingest.source_certification.catalog import (  # noqa: E402
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.execution_driver import (  # noqa: E402
    declared_execution_plan_sha256,
)
from services.ingest.source_certification.producer import (  # noqa: E402
    BINDING_SCHEMA_VERSION,
)
from services.ingest.source_contract.catalog import (  # noqa: E402
    CANONICAL_SOURCE_IDS,
)


DEFAULT_OUTPUT_DIRECTORY = (
    REPO_ROOT
    / "services"
    / "ingest"
    / "source_certification"
    / "execution_bindings"
)
_DRIVER_MODULE = "services.ingest.source_certification.execution_driver"


def _stage(
    source_id: str,
    stage: str,
) -> dict[str, object]:
    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    required_env: list[str] = []
    credential_env: list[str] = []
    if stage == "canary":
        required_env = [spec.canary.credential_env_prefix]
        credential_env = list(required_env)
    timeout_seconds = {
        "local_correctness": 600,
        "load": 1_800,
        "fault_recovery": 600,
        "canary": 900,
    }[stage]
    return {
        "argv": [
            "{python}",
            "-m",
            _DRIVER_MODULE,
            "--source",
            source_id,
            "--stage",
            stage,
            "--plan-sha256",
            declared_execution_plan_sha256(source_id),
        ],
        "timeout_seconds": timeout_seconds,
        "required_env": required_env,
        "credential_env": credential_env,
    }


def build_execution_bindings() -> dict[str, dict[str, object]]:
    """Build all binding files in canonical catalog order."""

    if tuple(SOURCE_CERTIFICATION_CATALOG) != CANONICAL_SOURCE_IDS:
        raise RuntimeError(
            "certification and source catalog order/coverage differ",
        )
    return {
        source_id: {
            "schema_version": BINDING_SCHEMA_VERSION,
            "source_id": source_id,
            "spec_hash": spec.declaration_hash(),
            "stages": {
                stage: _stage(source_id, stage)
                for stage in (
                    "local_correctness",
                    "load",
                    "fault_recovery",
                    "canary",
                )
            },
        }
        for source_id, spec in SOURCE_CERTIFICATION_CATALOG.items()
    }


def render_binding(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def write_execution_bindings(
    output_dir: Path,
    *,
    check: bool,
) -> tuple[str, ...]:
    """Write or verify the exact 27-file execution-binding directory."""

    expected = build_execution_bindings()
    expected_names = {f"{source_id}.json" for source_id in expected}
    actual_names = (
        {path.name for path in output_dir.iterdir()}
        if output_dir.is_dir()
        else set()
    )
    unexpected = sorted(actual_names - expected_names)
    stale: list[str] = []
    if unexpected:
        stale.extend(f"unexpected:{name}" for name in unexpected)

    for source_id, value in expected.items():
        path = output_dir / f"{source_id}.json"
        rendered = render_binding(value)
        if path.is_file() and path.read_text(encoding="utf-8") == rendered:
            continue
        stale.append(source_id)
        if not check:
            output_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(rendered, encoding="utf-8")

    if stale and check:
        raise RuntimeError(
            "source certification execution bindings are stale: "
            + ", ".join(stale),
        )
    return tuple(stale)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIRECTORY,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail rather than write when generated bindings differ",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    write_execution_bindings(
        args.output_dir.resolve(),
        check=args.check,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_execution_bindings",
    "render_binding",
    "write_execution_bindings",
]
