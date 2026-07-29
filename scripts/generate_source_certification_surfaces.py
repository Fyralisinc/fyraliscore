#!/usr/bin/env python3
"""Generate the pinned, sanitized API surface bundle for every source.

The bundle is deliberately local evidence. It captures the exact source and
provider contracts, Provider Lab routes, and deterministic golden fixture used
by Fyralis. It does not claim that provider documentation or a live account
has been verified; those remain separate fail-closed evidence items.
"""

from __future__ import annotations

import argparse
import json
import os
import hashlib
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Provider Lab imports are production-guarded. This is a build/test generator,
# so select test mode only when the caller did not choose an environment.
os.environ.setdefault("COMPANY_OS_ENV", "test")

from services.ingest.source_certification.evidence import (  # noqa: E402
    EVIDENCE_PACK_DIRECTORY,
)
from services.ingest.source_certification.runtime import (  # noqa: E402
    resolve_fixture_factory,
    resolve_live_fixture_factory,
)
from services.ingest.source_contract.catalog import (  # noqa: E402
    CANONICAL_SOURCE_IDS,
    provider_definition,
    source_definition,
)
from services.ingest.synthetic.provider_lab.adapters import (  # noqa: E402
    build_lab_adapter_registry,
)


SURFACE_SCHEMA_VERSION = "fyralis.source-certification-surface.v2"
SURFACE_EVIDENCE_NOTE = (
    "Exact local contract, implementation modules, Provider Lab routes, and "
    "sanitized golden fixture are pinned by "
    "scripts/generate_source_certification_surfaces.py; "
    "provider-document and live-account verification remain pending."
)
DEFAULT_OUTPUT_DIRECTORY = Path(
    "services/ingest/source_certification/surfaces",
)

# Checked-in surface artifacts must be byte-stable.  The runtime certification
# fixtures for these providers deliberately use a recent clock anchor so their
# backfills stay inside real provider lookback windows.  That behavior is
# correct for a run, but it must not leak into the static golden fixture bundle.
# Keep the pinned values aligned with the deterministic generator defaults.
_PINNED_GOLDEN_FIXTURE_PARAMS: Mapping[str, Mapping[str, Any]] = {
    "aws": {"base_ms": 1767571200000},
    "grafana": {"base_ms": 1767571200000},
    "hibob": {"base_iso": "2026-01-05T00:00:00Z"},
}


def _golden_fixture(source_id: str) -> Mapping[str, Any]:
    source = source_definition(source_id)
    factory = (
        resolve_fixture_factory(source_id)
        if source.history is not None
        else resolve_live_fixture_factory(source_id)
    )
    fixture = factory(
        fixture_params=_PINNED_GOLDEN_FIXTURE_PARAMS.get(source_id, {}),
        installation_id=f"certification-surface-{source_id}",
    )
    if not isinstance(fixture, Mapping):
        raise TypeError(
            f"{source_id} fixture factory returned "
            f"{type(fixture).__name__}, expected a mapping",
        )
    return fixture


def _module_path(module_name: str) -> Path | None:
    module = REPO_ROOT.joinpath(*module_name.split("."))
    file_path = module.with_suffix(".py")
    if file_path.is_file():
        return file_path
    package_path = module / "__init__.py"
    return package_path if package_path.is_file() else None


def _binding_modules(value: Any) -> set[str]:
    """Extract modules only from contract fields that explicitly own bindings."""

    modules: set[str] = set()

    def visit(item: Any, *, binding_field: bool = False) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                visit(
                    child,
                    binding_field=isinstance(key, str) and "binding" in key,
                )
            return
        if isinstance(item, (list, tuple)):
            for child in item:
                visit(child, binding_field=binding_field)
            return
        if not binding_field or not isinstance(item, str) or ":" not in item:
            return
        module_name, _separator, _callable_name = item.partition(":")
        if _module_path(module_name) is None:
            raise ValueError(
                f"contract binding references missing module {module_name!r}",
            )
        modules.add(module_name)

    visit(value)
    return modules


def _implementation_digests(
    *,
    source_id: str,
    source_contract: Mapping[str, Any],
    provider_contract: Mapping[str, Any],
    adapter_module: str,
    fixture_module: str | None,
) -> dict[str, str]:
    modules = _binding_modules(source_contract) | _binding_modules(
        provider_contract,
    )
    modules.add(adapter_module)
    if fixture_module is not None:
        modules.add(fixture_module)

    integration_root = (
        REPO_ROOT / "services" / "ingest" / "integrations" / source_id
    )
    implementation_files: dict[str, Path] = {}
    for module_name in modules:
        path = _module_path(module_name)
        if path is None:
            raise ValueError(
                f"cannot resolve implementation module {module_name!r}",
            )
        implementation_files[module_name] = path
    if integration_root.is_dir():
        for path in integration_root.rglob("*.py"):
            if "__pycache__" in path.parts or "tests" in path.parts:
                continue
            module_name = ".".join(
                path.relative_to(REPO_ROOT).with_suffix("").parts,
            )
            if module_name.endswith(".__init__"):
                module_name = module_name.removesuffix(".__init__")
            implementation_files[module_name] = path
    return {
        module_name: hashlib.sha256(path.read_bytes()).hexdigest()
        for module_name, path in sorted(implementation_files.items())
    }


def _load_evidence_pack_json(
    source_id: str,
    *,
    evidence_directory: Path,
) -> dict[str, Any]:
    path = evidence_directory / f"{source_id}.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("source_id") != source_id:
        raise ValueError(f"invalid evidence pack identity in {path}")
    return value


def _used_surface_item(pack: Mapping[str, Any]) -> dict[str, Any]:
    evidence = pack.get("evidence")
    if not isinstance(evidence, list):
        raise ValueError("evidence pack has no evidence array")
    matches = [
        item
        for item in evidence
        if isinstance(item, dict)
        and item.get("behavior_id") == "used_api_surface"
    ]
    if len(matches) != 1:
        raise ValueError(
            "evidence pack must have exactly one used_api_surface item",
        )
    return matches[0]


def build_surface_artifacts(
    *,
    evidence_directory: Path = EVIDENCE_PACK_DIRECTORY,
) -> dict[str, dict[str, Any]]:
    """Build all canonical surface artifacts in catalog order."""

    registry = build_lab_adapter_registry()
    registry.validate_expected_sources(CANONICAL_SOURCE_IDS)

    artifacts: dict[str, dict[str, Any]] = {}
    for source_id in CANONICAL_SOURCE_IDS:
        source = source_definition(source_id)
        provider = provider_definition(source.provider_id)
        adapter = registry.require(source_id)
        source_payload = asdict(source)
        provider_payload = asdict(provider)
        fixture_factory = (
            resolve_fixture_factory(source_id)
            if source.history is not None
            else resolve_live_fixture_factory(source_id)
        )
        evidence_pack = _load_evidence_pack_json(
            source_id,
            evidence_directory=evidence_directory,
        )
        artifacts[source_id] = {
            "schema_version": SURFACE_SCHEMA_VERSION,
            "source_id": source_id,
            "provider_api_version": evidence_pack["provider_api_version"],
            "source_contract": source_payload,
            "provider_contract": provider_payload,
            "implementation_sha256": _implementation_digests(
                source_id=source_id,
                source_contract=source_payload,
                provider_contract=provider_payload,
                adapter_module=adapter.__class__.__module__,
                fixture_module=fixture_factory.__module__,
            ),
            "provider_lab": {
                "routes": [
                    {
                        "route_id": route.route_id,
                        "path_template": route.path_template,
                        "methods": list(route.methods),
                        "operation_ids": list(route.operation_ids),
                        "operation_bindings": [
                            {
                                "operation_id": binding.operation_id,
                                "method": binding.method,
                                "path_values": [
                                    list(item)
                                    for item in binding.path_values
                                ],
                                "query_items": [
                                    list(item)
                                    for item in binding.query_items
                                ],
                                "headers": [
                                    list(item) for item in binding.headers
                                ],
                                "body_sha256": (
                                    hashlib.sha256(binding.body).hexdigest()
                                    if binding.body is not None
                                    else None
                                ),
                            }
                            for operation_id in route.operation_ids
                            for binding in (
                                route.binding_for(operation_id),
                            )
                        ],
                        "transport": route.transport,
                        "quota_bucket": route.quota_bucket,
                        "quota_cost": route.quota_cost,
                    }
                    for route in adapter.routes
                ],
                "protocol_surfaces": [
                    {
                        "surface_id": surface.surface_id,
                        "transport": surface.transport,
                        "operation_ids": list(surface.operation_ids),
                    }
                    for surface in adapter.protocol_surfaces
                ],
                "golden_fixture": _golden_fixture(source_id),
            },
        }
    return artifacts


def render_surface_artifact(value: Mapping[str, Any]) -> str:
    """Render one bundle with byte-stable JSON formatting."""

    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIRECTORY,
        help="repository-relative or absolute surface directory",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail instead of writing when any checked-in bundle is stale",
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=EVIDENCE_PACK_DIRECTORY,
        help="directory containing the evidence packs whose digests are pinned",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = (
        args.output_dir
        if args.output_dir.is_absolute()
        else REPO_ROOT / args.output_dir
    )
    evidence_dir = (
        args.evidence_dir
        if args.evidence_dir.is_absolute()
        else REPO_ROOT / args.evidence_dir
    )
    artifacts = build_surface_artifacts(evidence_directory=evidence_dir)
    expected_names = {f"{source_id}.json" for source_id in CANONICAL_SOURCE_IDS}

    if args.check:
        actual_names = (
            {path.name for path in output_dir.glob("*.json")}
            if output_dir.is_dir()
            else set()
        )
        stale = sorted(expected_names ^ actual_names)
        for source_id, artifact in artifacts.items():
            path = output_dir / f"{source_id}.json"
            expected = render_surface_artifact(artifact)
            expected_sha256 = hashlib.sha256(
                expected.encode("utf-8"),
            ).hexdigest()
            try:
                actual = path.read_text(encoding="utf-8")
            except FileNotFoundError:
                continue
            if actual != expected:
                stale.append(path.name)
            pack = _load_evidence_pack_json(
                source_id,
                evidence_directory=evidence_dir,
            )
            used_surface = _used_surface_item(pack)
            if used_surface.get("schema_sha256") != expected_sha256:
                stale.append(f"evidence/{source_id}.json")
            if used_surface.get("notes") != SURFACE_EVIDENCE_NOTE:
                stale.append(f"evidence/{source_id}.json")
        stale = sorted(set(stale))
        if not stale:
            print(
                "Source certification surface artifacts are current: "
                f"{output_dir}",
            )
            return 0
        print(
            "Source certification surface artifacts are missing or stale: "
            + ", ".join(stale),
            file=sys.stderr,
        )
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    for path in output_dir.glob("*.json"):
        if path.name not in expected_names:
            path.unlink()
    for source_id, artifact in artifacts.items():
        rendered = render_surface_artifact(artifact)
        (output_dir / f"{source_id}.json").write_text(
            rendered,
            encoding="utf-8",
        )
        pack_path = evidence_dir / f"{source_id}.json"
        pack = _load_evidence_pack_json(
            source_id,
            evidence_directory=evidence_dir,
        )
        used_surface = _used_surface_item(pack)
        used_surface["schema_sha256"] = hashlib.sha256(
            rendered.encode("utf-8"),
        ).hexdigest()
        used_surface["notes"] = SURFACE_EVIDENCE_NOTE
        pack_path.write_text(
            json.dumps(pack, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(
        f"Wrote {len(artifacts)} source certification surface artifacts: "
        f"{output_dir}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
