#!/usr/bin/env python3
"""Run the sealed P6 stream through production T1 Think in 12 intact batches."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from services.domain.company_identity_bootstrap import FounderIdentityBootstrapEntry
from services.evaluation.epistemic_repair.founder_bootstrap import (
    FounderBootstrapBatchPreparer,
    build_founder_bootstrap_batch_preparer,
)
from services.evaluation.epistemic_repair.p6_think_runner import (
    _write_checkpoint,
    p6_runtime_start,
    run_p6_production_think,
)


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"founder manifest {field} must be a non-empty string")
    return value.strip()


def _load_founder_manifest(
    path: Path, *, raw_bytes: bytes | None = None,
) -> FounderBootstrapBatchPreparer:
    """Load an explicit public identity manifest without consulting P6 gold."""

    try:
        manifest_bytes = path.read_bytes() if raw_bytes is None else raw_bytes
        payload = json.loads(manifest_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read founder manifest {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("founder manifest must be a JSON object")

    provenance = payload.get("provenance_refs")
    if not isinstance(provenance, list) or not provenance:
        raise ValueError("founder manifest provenance_refs must be a non-empty array")
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ValueError("founder manifest entries must be a non-empty array")

    entries: list[FounderIdentityBootstrapEntry] = []
    for index, raw_entry in enumerate(raw_entries):
        if not isinstance(raw_entry, dict):
            raise ValueError(f"founder manifest entries[{index}] must be an object")
        canonical_ref = raw_entry.get("canonical_ref")
        if not isinstance(canonical_ref, dict):
            raise ValueError(
                f"founder manifest entries[{index}].canonical_ref must be an object"
            )
        aliases = raw_entry.get("aliases", [])
        if not isinstance(aliases, list) or not all(
            isinstance(alias, str) for alias in aliases
        ):
            raise ValueError(
                f"founder manifest entries[{index}].aliases must be an array of strings"
            )
        entries.append(FounderIdentityBootstrapEntry(
            canonical_ref=canonical_ref,
            canonical_name=_required_text(
                raw_entry.get("canonical_name"),
                field=f"entries[{index}].canonical_name",
            ),
            aliases=tuple(aliases),
        ))

    effective_text = _required_text(payload.get("effective_at"), field="effective_at")
    try:
        effective_at = datetime.fromisoformat(effective_text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("founder manifest effective_at must be ISO-8601") from exc
    if effective_at.tzinfo is None:
        raise ValueError("founder manifest effective_at must include a timezone")
    if effective_at > p6_runtime_start():
        raise ValueError(
            "founder manifest effective_at must not be later than the first "
            "selected P6 observation"
        )

    return build_founder_bootstrap_batch_preparer(
        manifest_ref=_required_text(payload.get("manifest_ref"), field="manifest_ref"),
        authority_ref=_required_text(
            payload.get("authority_ref"), field="authority_ref"
        ),
        asserted_by_ref=_required_text(
            payload.get("asserted_by_ref"), field="asserted_by_ref"
        ),
        provenance_refs=tuple(
            _required_text(value, field="provenance_refs") for value in provenance
        ),
        entries=tuple(entries),
        effective_at=effective_at,
    )


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    founder_manifest_bytes = None
    founder_preparer = None
    if args.founder_manifest is not None:
        try:
            founder_manifest_bytes = args.founder_manifest.read_bytes()
        except OSError as exc:
            raise ValueError(
                f"cannot read founder manifest {args.founder_manifest}: {exc}"
            ) from exc
        founder_preparer = _load_founder_manifest(
            args.founder_manifest, raw_bytes=founder_manifest_bytes,
        )
    artifact = await run_p6_production_think(
        database_url=args.database_url, population=build_p6_population(),
        checkpoint_path=args.output,
        per_batch_timeout_s=args.batch_timeout,
        attempt_timeout_s=args.attempt_timeout,
        total_timeout_s=args.total_timeout,
        max_batches=args.max_batches,
        prepare_persisted_batch=founder_preparer,
    )
    if founder_preparer is not None:
        bootstrap_receipt = founder_preparer.receipt or {
            "manifest_ref": founder_preparer.manifest_ref,
            "alias_count": None,
            "applied_before_enqueue": False,
            "semantic_truth_unchanged": None,
        }
        artifact["founder_identity_bootstrap"] = {
            **bootstrap_receipt,
            "manifest_file_sha256": hashlib.sha256(
                founder_manifest_bytes
            ).hexdigest(),
            "canonical_entry_count": len(founder_preparer.entries),
            "no_behavioral_models_seeded": True,
        }
        _write_checkpoint(args.output, artifact)
    print(f"complete={str(artifact['complete']).lower()} batches={artifact['completed_batches']} terminal_reason={artifact['terminal_reason']} output={args.output}")
    return 0 if artifact["complete"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("/tmp/p6-think.json"))
    parser.add_argument("--attempt-timeout", type=float, default=300.0)
    parser.add_argument("--batch-timeout", type=float, default=650.0)
    parser.add_argument("--total-timeout", type=float, default=1800.0)
    parser.add_argument("--max-batches", type=int, default=12)
    parser.add_argument(
        "--founder-manifest",
        type=Path,
        help="JSON identity manifest applied once before the first batch is enqueued",
    )
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
