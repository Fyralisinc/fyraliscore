#!/usr/bin/env python3
"""Generate or check BYOC product-health collector automation artifacts."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_product_health_automation import (
    ByocProductHealthAutomation,
    ByocProductHealthAutomationViolation,
    generate_product_health_automation,
    load_product_health_automation,
    product_health_automation_json_schema,
    render_product_health_automation_artifacts,
    render_validation_errors,
    validate_product_health_automation_contract,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest to consume.",
    )
    parser.add_argument(
        "--collector-script",
        type=Path,
        default=Path("scripts/run_byoc_product_health_collector.py"),
        help="Product-health collector script referenced by generated artifacts.",
    )
    parser.add_argument(
        "--check-automation",
        type=Path,
        help="Existing product-health automation manifest to validate and compare.",
    )
    parser.add_argument(
        "--automation-output",
        type=Path,
        default=Path("deploy/byoc/product-health-automation.example.yaml"),
        help="Automation manifest path to write when --write is supplied.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used to inspect or write automation artifacts.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the automation manifest and rendered artifacts.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the automation contract and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))

    if args.schema:
        print(
            json.dumps(
                product_health_automation_json_schema(),
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    try:
        repo_root = args.repo_root.resolve()
        dataplane = load_byoc_manifest(args.dataplane_manifest)
        generated = generate_product_health_automation(
            dataplane_manifest=dataplane,
            dataplane_manifest_path=_display_path(args.dataplane_manifest, repo_root),
            collector_script_path=_display_path(args.collector_script, repo_root),
        )
        rendered_artifacts = render_product_health_automation_artifacts(generated)
    except ValidationError as exc:
        _print_errors(
            "BYOC product-health automation schema violations",
            render_validation_errors(exc),
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "Failed to build BYOC product-health automation",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    if args.check_automation is not None:
        return _check_existing_automation(
            existing_path=args.check_automation,
            generated=generated,
            dataplane=dataplane,
            repo_root=repo_root,
        )

    if args.write:
        _write_automation_and_artifacts(
            automation_path=args.automation_output,
            automation=generated,
            rendered_artifacts=rendered_artifacts,
            repo_root=repo_root,
        )
        print("BYOC product-health automation written.")
        return 0

    payload = generated.model_dump(mode="json", exclude_none=True)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
            _print_errors("Failed to render YAML", [str(exc)])
            return 1
        sys.stdout.write(yaml.safe_dump(payload, sort_keys=False, width=1_000_000))
    return 0


def _check_existing_automation(
    *,
    existing_path: Path,
    generated: ByocProductHealthAutomation,
    dataplane,
    repo_root: Path,
) -> int:
    existing = load_product_health_automation(existing_path)
    violations = validate_product_health_automation_contract(
        existing,
        dataplane_manifest=dataplane,
        repo_root=repo_root,
    )
    if existing.model_dump(mode="json") != generated.model_dump(mode="json"):
        violations.append(
            ByocProductHealthAutomationViolation(
                path="<root>",
                code="generated_product_health_automation_drift",
                message=(
                    "checked-in product-health automation does not match "
                    "generated output"
                ),
            )
        )
    if violations:
        _print_errors(
            "BYOC product-health automation contract violations",
            [violation.render() for violation in violations],
        )
        return 1
    print("BYOC product-health automation passed.")
    return 0


def _write_automation_and_artifacts(
    *,
    automation_path: Path,
    automation: ByocProductHealthAutomation,
    rendered_artifacts: dict[str, str],
    repo_root: Path,
) -> None:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
        raise RuntimeError("YAML output requires PyYAML") from exc
    automation_path.parent.mkdir(parents=True, exist_ok=True)
    automation_path.write_text(
        yaml.safe_dump(
            automation.model_dump(mode="json", exclude_none=True),
            sort_keys=False,
            width=1_000_000,
        ),
        encoding="utf-8",
    )
    for rel_path, content in rendered_artifacts.items():
        path = repo_root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _display_path(path: Path, repo_root: Path) -> Path:
    resolved = path.resolve()
    try:
        return resolved.relative_to(repo_root)
    except ValueError:
        return path


def _print_errors(title: str, errors: list[str]) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
