#!/usr/bin/env python3
"""Generate the onboarding source catalog from the Python source contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.ingest.source_contract import (  # noqa: E402
    PROVIDER_DEFINITIONS,
    SOURCE_DEFINITIONS,
    ProviderDefinition,
    SourceDefinition,
)


SCHEMA_VERSION = "fyralis.source-catalog.ui.v1"
DEFAULT_OUTPUT_PATH = Path("ui/features/onboarding/data/source-catalog.generated.json")


def build_source_catalog_artifact(
    *,
    sources: Sequence[SourceDefinition] = SOURCE_DEFINITIONS,
    providers: Sequence[ProviderDefinition] = PROVIDER_DEFINITIONS,
) -> dict[str, object]:
    """Return one JSON-compatible catalog with deterministic ordering."""

    display_order = tuple(
        source.display.order
        for source in sorted(sources, key=lambda item: item.display.order)
    )
    if display_order != tuple(range(len(sources))):
        raise ValueError(
            "source display order must be contiguous from zero; "
            f"got {display_order!r}"
        )

    provider_ids = tuple(provider.provider_id for provider in providers)
    known_providers = frozenset(provider_ids)
    unknown_providers = sorted(
        {
            source.provider_id
            for source in sources
            if source.provider_id not in known_providers
        }
    )
    if unknown_providers:
        raise ValueError(
            "source catalog references unknown providers: " f"{unknown_providers!r}"
        )

    return {
        "schemaVersion": SCHEMA_VERSION,
        "canonicalSourceIds": [source.source_id for source in sources],
        "canonicalProviderIds": list(provider_ids),
        "sources": [
            source.as_ui_catalog_entry()
            for source in sorted(
                sources,
                key=lambda item: item.display.order,
            )
        ],
    }


def render_source_catalog_artifact(
    *,
    sources: Sequence[SourceDefinition] = SOURCE_DEFINITIONS,
    providers: Sequence[ProviderDefinition] = PROVIDER_DEFINITIONS,
) -> str:
    """Render the canonical artifact with a stable JSON encoding."""

    return (
        json.dumps(
            build_source_catalog_artifact(
                sources=sources,
                providers=providers,
            ),
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    )


def check_artifact(path: Path, expected: str) -> bool:
    """Return whether ``path`` exactly matches the generated content."""

    try:
        actual = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return False
    return actual == expected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="repository-relative or absolute generated artifact path",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail instead of writing when the checked-in artifact is stale",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_path = args.output if args.output.is_absolute() else REPO_ROOT / args.output
    expected = render_source_catalog_artifact()

    if args.check:
        if check_artifact(output_path, expected):
            print(f"Source catalog artifact is current: {output_path}")
            return 0
        print(
            "Source catalog artifact is missing or stale. Regenerate with: "
            "python scripts/generate_source_catalog_artifacts.py",
            file=sys.stderr,
        )
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(expected, encoding="utf-8")
    print(f"Wrote source catalog artifact: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
