"""Independent scanner for benchmark-aware production behavior.

The scanner deliberately works from a small, versioned registry of structural
fingerprints.  It does not treat generic domain vocabulary (for example
``pricing``, ``noise`` or ``bridge``) as evidence of benchmark contamination.
"""

from __future__ import annotations

import ast
import json
import re
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REGISTRY_VERSION = "fyralis-hook-blindness-registry-v1"


class Surface(StrEnum):
    CALL_SITE = "call_site"
    SOURCE_TEXT = "source_text"
    PROMPT_OR_OUTPUT = "prompt_or_output"
    TRACE_PAYLOAD = "trace_payload"


@dataclass(frozen=True)
class Fingerprint:
    """A benchmark signature whose anchors must all occur on one surface."""

    fingerprint_id: str
    surfaces: frozenset[Surface]
    anchors: tuple[str, ...]
    call_symbols: tuple[str, ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        if not self.fingerprint_id or not self.anchors:
            raise ValueError("fingerprints require an ID and at least one anchor")
        generic = {"benchmark", "pricing", "bridge", "noise", "capability"}
        if len(self.anchors) == 1 and self.anchors[0].casefold() in generic:
            raise ValueError("a generic concept cannot be a standalone fingerprint")


@dataclass(frozen=True)
class Finding:
    fingerprint_id: str
    surface: Surface
    location: str
    matched_anchors: tuple[str, ...]


@dataclass(frozen=True)
class ScanReport:
    registry_version: str
    reachable_modules: tuple[str, ...]
    findings: tuple[Finding, ...]

    @property
    def is_hook_blind(self) -> bool:
        return not self.findings


# These are signatures of the four P0 hook families, not a domain-word banlist.
DEFAULT_REGISTRY: tuple[Fingerprint, ...] = (
    Fingerprint(
        "BH-001",
        frozenset(
            {
                Surface.CALL_SITE,
                Surface.SOURCE_TEXT,
                Surface.PROMPT_OR_OUTPUT,
                Surface.TRACE_PAYLOAD,
            }
        ),
        ("company_os.reasoning_augmentors", "augment_context"),
        call_symbols=("augment_context",),
        description="dynamic reasoning augmentor reachable from production",
    ),
    Fingerprint(
        "BH-002",
        frozenset(Surface),
        ("capability_probe_kinds", "maybe_inject_capability_probe_ops"),
        call_symbols=("maybe_inject_capability_probe_ops",),
        description="benchmark-requested capability output injector",
    ),
    Fingerprint(
        "BH-003",
        frozenset(Surface),
        ("bounded inferred bridge", "confidence", "0.58"),
        call_symbols=("maybe_inject_latent_bridge",),
        description="fixture-shaped pricing-transition bridge injector",
    ),
    Fingerprint(
        "BH-004",
        frozenset(
            {
                Surface.CALL_SITE,
                Surface.SOURCE_TEXT,
                Surface.PROMPT_OR_OUTPUT,
                Surface.TRACE_PAYLOAD,
            }
        ),
        (
            "general operational chatter",
            "duplicated dashboard links",
            "discard_as_noise",
        ),
        call_symbols=("build_noise_only_raw_diff", "_build_noise_noop_fast_path"),
        description="fixture-phrase noise short circuit",
    ),
)


def _module_name(root: Path, path: Path) -> str:
    relative = path.relative_to(root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _module_index(root: Path) -> dict[str, Path]:
    return {
        _module_name(root, path): path
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts
    }


def _imports(module: str, tree: ast.AST) -> set[str]:
    found: set[str] = set()
    package = module.split(".")[:-1]
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                prefix = package[: len(package) - node.level + 1]
                base = ".".join((*prefix, *(node.module or "").split(".")))
            else:
                base = node.module or ""
            if base:
                found.add(base)
            for alias in node.names:
                if alias.name != "*" and base:
                    found.add(f"{base}.{alias.name}")
    return found


def reachable_python_modules(
    root: Path,
    entrypoints: Sequence[str],
) -> dict[str, tuple[Path, ast.AST]]:
    """Return local modules statically reachable through Python imports."""

    root = root.resolve()
    index = _module_index(root)
    queue = deque(entrypoints)
    result: dict[str, tuple[Path, ast.AST]] = {}
    while queue:
        module = queue.popleft()
        if module in result or module not in index:
            continue
        path = index[module]
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        result[module] = (path, tree)
        queue.extend(sorted(_imports(module, tree) - result.keys()))
    return result


def _called_symbols(tree: ast.AST) -> set[str]:
    symbols: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            symbols.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            symbols.add(node.func.attr)
    return symbols


def _matches(text: str, anchors: Sequence[str]) -> bool:
    folded = text.casefold()
    return all(anchor.casefold() in folded for anchor in anchors)


def scan_production_reachability(
    root: Path,
    entrypoints: Sequence[str],
    *,
    registry: Sequence[Fingerprint] = DEFAULT_REGISTRY,
) -> ScanReport:
    modules = reachable_python_modules(root, entrypoints)
    findings: list[Finding] = []
    for module, (path, tree) in sorted(modules.items()):
        source = path.read_text(encoding="utf-8")
        calls = _called_symbols(tree)
        for fingerprint in registry:
            if Surface.CALL_SITE in fingerprint.surfaces and set(
                fingerprint.call_symbols
            ).intersection(calls):
                findings.append(
                    Finding(
                        fingerprint.fingerprint_id,
                        Surface.CALL_SITE,
                        f"{path}:{module}",
                        tuple(
                            sorted(set(fingerprint.call_symbols).intersection(calls))
                        ),
                    )
                )
            elif Surface.SOURCE_TEXT in fingerprint.surfaces and _matches(
                source, fingerprint.anchors
            ):
                findings.append(
                    Finding(
                        fingerprint.fingerprint_id,
                        Surface.SOURCE_TEXT,
                        f"{path}:{module}",
                        fingerprint.anchors,
                    )
                )
    return ScanReport(
        REGISTRY_VERSION,
        tuple(sorted(modules)),
        tuple(findings),
    )


def scan_text_surfaces(
    surfaces: Mapping[str, str],
    *,
    registry: Sequence[Fingerprint] = DEFAULT_REGISTRY,
) -> tuple[Finding, ...]:
    findings: list[Finding] = []
    for location, text in sorted(surfaces.items()):
        for fingerprint in registry:
            if Surface.PROMPT_OR_OUTPUT in fingerprint.surfaces and _matches(
                text, fingerprint.anchors
            ):
                findings.append(
                    Finding(
                        fingerprint.fingerprint_id,
                        Surface.PROMPT_OR_OUTPUT,
                        location,
                        fingerprint.anchors,
                    )
                )
    return tuple(findings)


def scan_trace_payloads(
    events: Iterable[Mapping[str, Any]],
    *,
    registry: Sequence[Fingerprint] = DEFAULT_REGISTRY,
) -> tuple[Finding, ...]:
    findings: list[Finding] = []
    for index, event in enumerate(events):
        text = json.dumps(event, sort_keys=True, default=str)
        for fingerprint in registry:
            if Surface.TRACE_PAYLOAD in fingerprint.surfaces and _matches(
                text, fingerprint.anchors
            ):
                findings.append(
                    Finding(
                        fingerprint.fingerprint_id,
                        Surface.TRACE_PAYLOAD,
                        f"trace[{index}]",
                        fingerprint.anchors,
                    )
                )
    return tuple(findings)


def assert_registry_is_versioned(
    registry: Sequence[Fingerprint] = DEFAULT_REGISTRY,
) -> None:
    if not re.fullmatch(
        r"fyralis-hook-blindness-registry-v[1-9][0-9]*", REGISTRY_VERSION
    ):
        raise ValueError("hook-blindness registry must have an explicit major version")
    ids = [item.fingerprint_id for item in registry]
    if len(ids) != len(set(ids)):
        raise ValueError("hook-blindness fingerprint IDs must be unique")
