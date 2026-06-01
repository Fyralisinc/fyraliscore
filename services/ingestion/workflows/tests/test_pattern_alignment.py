"""M6.0 Phase 3 — pattern-alignment static analyzer.

The five-rule analyzer enforced over `services/ingestion/workflows/`.
Per [docs/ingestion/pattern-alignment-rules.md](../../../../docs/ingestion/pattern-alignment-rules.md)
and [04-implementation-plan.md §M6](../../../../docs/ingestion/04-implementation-plan.md).

============================================================
LOAD-BEARING (M6.0 Phase 3) — Temporal portability gate
============================================================
The asyncio substrate (M6.0) is the framework every subsequent M6
sub-block builds on. The pattern-alignment requirements documented
in the plan are what make a future Temporal port mechanical rather
than a rewrite when [A11's trigger conditions](../../../../docs/ingestion/05-lld-amendments.md)
fire. This file makes those requirements load-bearing PROPERTIES of
the codebase, checked on every test run.

============================================================
RETROACTIVE CALIBRATION (smoke check)
============================================================
Per the M6.0 Phase 3 prompt: the analyzer is calibrated such that
M3.3 (`embedding_backlog.py`) and M5.1 (`circuit_breaker.py`) — both
proven-correct asyncio services predating M6 — pass by construction.
If they fail, the analyzer is wrong (false positive). Phase 2's
`feels_onboarded_monitor.py` is the third pass-by-construction case.

============================================================
SCOPE
============================================================
The analyzer runs over `services/ingestion/workflows/*.py` (excluding
`tests/`, `__init__.py`, `__pycache__/`). Substrate files —
`state.py`, `signals.py`, `runtime.py`, `retry.py`, `__main__.py` —
are exempt from the rules they would inappropriately fail (Rule 1
exempts substrate; Rule 2 exempts substrate + __main__; Rule 3
exempts retry.py per the naming-convention fallback).

Rules ARE ordering-agnostic per the user's reminder (a):
neither N1 cursor-advance (publish-then-persist) nor
CLAIM-VIA-UPDATE (persist-then-publish) is flagged. The analyzer
enforces STRUCTURAL properties — named retry helpers, no inline
retry loops, no cross-service queues — not orderings.
"""
from __future__ import annotations

import ast
import pathlib
from dataclasses import dataclass

import pytest


# ---------------------------------------------------------------------
# Scope.
# ---------------------------------------------------------------------
REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
WORKFLOWS_DIR = REPO_ROOT / "services" / "ingestion" / "workflows"

# Substrate files — exempt from various rules per the pattern-alignment
# docs. Lower-cased filenames only.
SUBSTRATE_FILES = {"state.py", "signals.py", "runtime.py", "retry.py"}
# `__main__.py` is the CLI bootstrap; it's allowed to wire pools +
# producers (the only orchestration role assigned to it).
BOOTSTRAP_FILES = {"__main__.py"}

# Retroactive precedents — proven-correct services that pre-date M6.
# Same analyzer runs over them; they MUST pass by construction.
RETROACTIVE_PRECEDENTS = [
    REPO_ROOT / "services" / "ingestion" / "recovery"
                  / "embedding_backlog" / "embedding_backlog.py",
    REPO_ROOT / "services" / "ingestion" / "feature_flags"
                  / "circuit_breaker.py",
]


def _workflow_files_to_analyze() -> list[pathlib.Path]:
    """Every `.py` file directly under workflows/ except tests + init."""
    return sorted(
        p for p in WORKFLOWS_DIR.glob("*.py")
        if p.name != "__init__.py"
    )


def _parse(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


# =====================================================================
# Violation record.
# =====================================================================
@dataclass(frozen=True)
class Violation:
    rule: str
    file: str
    lineno: int
    detail: str

    def __str__(self) -> str:
        return f"  [{self.rule}] {self.file}:{self.lineno} — {self.detail}"


# =====================================================================
# Rule 1 — Orchestration separated from side effects.
#
# Class methods MUST NOT make direct pool/producer API calls. Calls
# go through named module-level functions.
# =====================================================================
_DB_VERBS = frozenset({
    "execute", "fetch", "fetchrow", "fetchval", "fetchmany",
    "executemany", "copy_from_query", "copy_to_table",
})
_KAFKA_VERBS = frozenset({"produce", "flush", "send", "send_and_wait"})
_FORBIDDEN_RECEIVERS = frozenset({
    "pool", "_pool", "producer", "_producer",
    "kafka_producer", "_kafka_producer", "db", "_db",
})


def _direct_io_call_violations(
    func_name: str, body: list[ast.stmt], file: str,
) -> list[Violation]:
    """Walk `body` for direct pool/producer attribute calls.

    Catches both `pool.execute(...)` (parameter receiver) and
    `self._pool.execute(...)` (attribute receiver). Returns the list
    of violations found.
    """
    found: list[Violation] = []
    for node in ast.walk(ast.Module(body=body, type_ignores=[])):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        verb = func.attr
        if verb not in _DB_VERBS and verb not in _KAFKA_VERBS:
            continue
        # The receiver: either Name("pool") or Attribute(Name("self"), "_pool").
        receiver = func.value
        receiver_name: str | None = None
        if isinstance(receiver, ast.Name):
            receiver_name = receiver.id
        elif isinstance(receiver, ast.Attribute) and isinstance(receiver.value, ast.Name):
            if receiver.value.id == "self":
                receiver_name = receiver.attr
        if receiver_name in _FORBIDDEN_RECEIVERS:
            found.append(Violation(
                rule="R1",
                file=file,
                lineno=node.lineno,
                detail=(
                    f"method {func_name!r}: direct {receiver_name!r}.{verb}(...) "
                    f"call. Move the DB/Kafka I/O into a module-level "
                    f"named function; the method should call THAT function."
                ),
            ))
    return found


def _rule_1_orchestration_separated(
    path: pathlib.Path, tree: ast.Module,
) -> list[Violation]:
    """Class methods MUST NOT make direct pool/producer API calls.

    Substrate files are exempt (they ARE the encapsulation). Function-
    style services (no ClassDef) are vacuously compliant.
    """
    if path.name in SUBSTRATE_FILES:
        return []
    violations: list[Violation] = []
    for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
        for member in cls.body:
            if isinstance(member, (ast.AsyncFunctionDef, ast.FunctionDef)):
                violations.extend(_direct_io_call_violations(
                    f"{cls.name}.{member.name}", member.body, str(path),
                ))
    return violations


# =====================================================================
# Rule 2 — State in Postgres, not memory.
#
# Concrete workflow files MUST import from state.py.
# =====================================================================
def _rule_2_state_in_postgres(
    path: pathlib.Path, tree: ast.Module,
) -> list[Violation]:
    if path.name in SUBSTRATE_FILES or path.name in BOOTSTRAP_FILES:
        return []
    found_state_import = False
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "services.ingestion.workflows.state":
                found_state_import = True
                break
    if not found_state_import:
        return [Violation(
            rule="R2",
            file=str(path),
            lineno=1,
            detail=(
                "Concrete workflow file does not import from "
                "services.ingestion.workflows.state. The substrate "
                "import is the structural proof that state lives in "
                "Postgres; without it, the file has no apparent "
                "connection to the state substrate."
            ),
        )]
    return []


# =====================================================================
# Rule 3 — Retry logic in named functions.
#
# A 'retry-shaped try' has an except handler with `await asyncio.sleep`
# or `continue`. Such functions MUST live in retry.py OR be named
# retry_*.
# =====================================================================
def _try_is_retry_shaped(node: ast.Try) -> bool:
    """A retry-shaped try has an `await asyncio.sleep(...)` in at
    least one except handler. The sleep is what distinguishes a retry
    (wait, then the enclosing loop re-attempts) from a skip-and-
    continue error-recovery pattern (M5.1's flag-flip handler:
    `log.exception(...); continue` advances to the NEXT tenant in
    the outer loop, doesn't retry the failed flip).

    `continue` alone is NOT sufficient evidence of a retry — a list
    traversal's per-element error handler often uses `continue` to
    skip the bad item without sleep. Requiring sleep tightens the
    heuristic to actual back-off patterns.
    """
    for handler in node.handlers:
        for sub in ast.walk(ast.Module(body=handler.body, type_ignores=[])):
            if isinstance(sub, ast.Await):
                call = sub.value
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute):
                    if (
                        isinstance(call.func.value, ast.Name)
                        and call.func.value.id == "asyncio"
                        and call.func.attr == "sleep"
                    ):
                        return True
    return False


def _rule_3_retry_in_named_functions(
    path: pathlib.Path, tree: ast.Module,
) -> list[Violation]:
    """Retry-shaped try blocks must live in retry.py or in a function
    named `retry_*`. Naming convention is the deliberate relaxation
    documented in pattern-alignment-rules.md.
    """
    if path.name == "retry.py":
        return []
    violations: list[Violation] = []

    def _walk(parent_func: str | None, body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                _walk(node.name, node.body)
            elif isinstance(node, ast.ClassDef):
                for member in node.body:
                    if isinstance(member, (ast.AsyncFunctionDef, ast.FunctionDef)):
                        _walk(member.name, member.body)
            elif isinstance(node, ast.Try):
                if _try_is_retry_shaped(node):
                    func_name = parent_func or "<module-level>"
                    if not func_name.startswith("retry_"):
                        violations.append(Violation(
                            rule="R3",
                            file=str(path),
                            lineno=node.lineno,
                            detail=(
                                f"retry-shaped try block in {func_name!r}. "
                                f"Move the retry policy into a named "
                                f"function: either live in retry.py or "
                                f"name the function `retry_<what>(...)`."
                            ),
                        ))
                # Walk the body + handlers for nested defs.
                _walk(parent_func, node.body)
                for handler in node.handlers:
                    _walk(parent_func, handler.body)
                _walk(parent_func, node.finalbody)
                _walk(parent_func, node.orelse)
            elif hasattr(node, "body") and isinstance(getattr(node, "body"), list):
                _walk(parent_func, node.body)  # type: ignore[arg-type]
                if hasattr(node, "orelse"):
                    _walk(parent_func, getattr(node, "orelse"))  # type: ignore[arg-type]

    _walk(None, tree.body)
    return violations


# =====================================================================
# Rule 4 — Signals via Postgres polling.
#
# No asyncio.Queue, multiprocessing.Queue/Manager, threading.Lock/
# RLock/Event in workflow files.
# =====================================================================
_FORBIDDEN_QUEUE_PRIMITIVES = {
    ("asyncio", "Queue"),
    ("asyncio", "PriorityQueue"),
    ("asyncio", "LifoQueue"),
    ("multiprocessing", "Queue"),
    ("multiprocessing", "Manager"),
    ("multiprocessing", "JoinableQueue"),
    ("threading", "Lock"),
    ("threading", "RLock"),
    ("threading", "Event"),
}


def _rule_4_signals_via_postgres(
    path: pathlib.Path, tree: ast.Module,
) -> list[Violation]:
    if path.name in SUBSTRATE_FILES:
        # state.py / signals.py / runtime.py may use asyncio.Event
        # internally; runtime.py uses asyncio.Event for SIGTERM
        # signalling which is the *allowed* shape.
        return []
    violations: list[Violation] = []
    # ImportFrom violations.
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module in {"multiprocessing", "threading"}:
                for alias in node.names:
                    if (node.module, alias.name) in _FORBIDDEN_QUEUE_PRIMITIVES:
                        violations.append(Violation(
                            rule="R4",
                            file=str(path),
                            lineno=node.lineno,
                            detail=(
                                f"import {node.module}.{alias.name} — "
                                f"use services.ingestion.workflows.signals "
                                f"for cross-service signaling."
                            ),
                        ))
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            recv = node.func.value
            if isinstance(recv, ast.Name):
                if (recv.id, node.func.attr) in _FORBIDDEN_QUEUE_PRIMITIVES:
                    violations.append(Violation(
                        rule="R4",
                        file=str(path),
                        lineno=node.lineno,
                        detail=(
                            f"{recv.id}.{node.func.attr}(...) construction. "
                            f"Use services.ingestion.workflows.signals for "
                            f"cross-service signaling."
                        ),
                    ))
    return violations


# =====================================================================
# Rule 5 — No cross-workflow shared in-process state.
#
# Module-level Assign nodes with mutable-container values MUST be
# named `*_metrics` (the A4 allowlist) or be ALL_CAPS constants.
# =====================================================================
def _is_mutable_container(value: ast.expr) -> bool:
    if isinstance(value, (ast.Dict, ast.List, ast.Set)):
        return True
    if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
        if value.func.id in {"dict", "list", "set"}:
            return True
    return False


def _rule_5_no_cross_workflow_state(
    path: pathlib.Path, tree: ast.Module,
) -> list[Violation]:
    violations: list[Violation] = []
    for node in tree.body:
        targets: list[ast.expr]
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
            value = node.value
        else:
            continue
        if not _is_mutable_container(value):
            continue
        for tgt in targets:
            if not isinstance(tgt, ast.Name):
                continue
            name = tgt.id
            # Dunders are Python conventions (`__all__`, `__slots__`),
            # not orchestration state.
            if name.startswith("__") and name.endswith("__"):
                continue
            if name.endswith("_metrics"):
                continue
            if name.isupper() or all(c.isupper() or c == "_" or c.isdigit() for c in name):
                continue
            violations.append(Violation(
                rule="R5",
                file=str(path),
                lineno=node.lineno,
                detail=(
                    f"module-level mutable container {name!r}. Rename "
                    f"to *_metrics if it's per-process observability, "
                    f"use ALL_CAPS if it's a constant, or move to "
                    f"Postgres if it's progress-bearing state."
                ),
            ))
    return violations


# =====================================================================
# Analyzer entry.
# =====================================================================
def _all_rules(path: pathlib.Path) -> list[Violation]:
    tree = _parse(path)
    return [
        *_rule_1_orchestration_separated(path, tree),
        *_rule_2_state_in_postgres(path, tree),
        *_rule_3_retry_in_named_functions(path, tree),
        *_rule_4_signals_via_postgres(path, tree),
        *_rule_5_no_cross_workflow_state(path, tree),
    ]


def _retroactive_rules(path: pathlib.Path) -> list[Violation]:
    """Subset of rules applicable to files outside services/ingestion/workflows/.

    M3.3 and M5.1 predate M6 and live in their own packages; the
    substrate-architectural rules (R1, R2, R4) are scoped to
    workflows/, and they don't apply outside that boundary. The
    structural rules (R3 inline-retry; R5 module-level mutable state)
    are universal and DO apply.
    """
    tree = _parse(path)
    return [
        *_rule_3_retry_in_named_functions(path, tree),
        *_rule_5_no_cross_workflow_state(path, tree),
    ]


# =====================================================================
# Primary tests: every workflow file passes by construction.
# =====================================================================

def test_pattern_alignment_passes_for_workflows_dir() -> None:
    """Every concrete file under services/ingestion/workflows/ MUST
    satisfy all five rules. This is the gate test that keeps
    M6.1–M6.6 sub-blocks within the pattern-alignment contract."""
    files = _workflow_files_to_analyze()
    assert files, (
        f"No files discovered under {WORKFLOWS_DIR} — analyzer "
        f"glob is broken or the directory is empty."
    )
    all_violations: list[Violation] = []
    for path in files:
        all_violations.extend(_all_rules(path))
    if all_violations:
        formatted = "\n".join(str(v) for v in all_violations)
        raise AssertionError(
            f"M6 pattern-alignment violations in "
            f"services/ingestion/workflows/:\n{formatted}\n\n"
            f"See docs/ingestion/pattern-alignment-rules.md for what "
            f"each rule means and what the analyzer checks."
        )


def test_pattern_alignment_smoke_passes_against_m3_3_and_m5_1() -> None:
    """LOAD-BEARING calibration check: M3.3's embedding_backlog and
    M5.1's circuit_breaker are proven-correct asyncio services that
    predate M6. The universal structural rules (R3 inline-retry,
    R5 module-level mutable state) MUST pass against them.

    If either fails, the analyzer is OVER-STRICT — the rule needs a
    relaxation, not the proven-correct code a refactor. Per the M6.0
    Phase 3 prompt's reminder (b)."""
    all_violations: list[Violation] = []
    for path in RETROACTIVE_PRECEDENTS:
        assert path.exists(), (
            f"Retroactive precedent file {path} is missing. The "
            f"analyzer's calibration anchor is broken; either the "
            f"file was moved or the constant in this test is stale."
        )
        all_violations.extend(_retroactive_rules(path))
    if all_violations:
        formatted = "\n".join(str(v) for v in all_violations)
        raise AssertionError(
            f"Retroactive smoke check FAILED — the analyzer flagged "
            f"M3.3 or M5.1, both of which are proven-correct asyncio "
            f"services. The rule is over-strict; relax it. Do NOT "
            f"refactor M3.3 / M5.1 to satisfy a wrong analyzer. \n"
            f"Violations:\n{formatted}"
        )


# =====================================================================
# Per-rule sanity tests: each rule must DETECT violations on synthetic
# bad code. Without these, a rule could silently regress to no-op.
# =====================================================================

def _parse_snippet(src: str) -> ast.Module:
    return ast.parse(src)


def test_rule_1_detects_direct_pool_call_in_method() -> None:
    src = '''
class BadService:
    def __init__(self, pool):
        self._pool = pool
    async def tick(self):
        await self._pool.execute("INSERT INTO x VALUES (1)")
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"  # any non-substrate
    v = _rule_1_orchestration_separated(path, tree)
    assert v, "Rule 1 failed to detect direct pool.execute in a method"
    assert v[0].rule == "R1"


def test_rule_2_detects_missing_state_import() -> None:
    src = "x = 1\n"  # no imports
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_2_state_in_postgres(path, tree)
    assert v and v[0].rule == "R2"


def test_rule_3_detects_inline_retry_loop() -> None:
    src = '''
import asyncio
async def fetch_page():
    for attempt in range(3):
        try:
            return await api_call()
        except Exception:
            await asyncio.sleep(1.0)
            continue
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_3_retry_in_named_functions(path, tree)
    assert v and v[0].rule == "R3", (
        f"Rule 3 did not detect inline retry loop. Violations: {v}"
    )


def test_rule_3_accepts_named_retry_function() -> None:
    """A function named `retry_*` containing the same shape MUST NOT
    be flagged."""
    src = '''
import asyncio
async def retry_with_my_backoff(fn):
    try:
        return await fn()
    except Exception:
        await asyncio.sleep(1.0)
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_3_retry_in_named_functions(path, tree)
    assert v == [], (
        f"Rule 3 false-positive — named retry function flagged: {v}"
    )


def test_rule_3_accepts_asyncio_wait_for_timeout_pass() -> None:
    """LOAD-BEARING false-positive guard: the standard
    `await asyncio.wait_for(stop_event.wait(), timeout=N) +
    except asyncio.TimeoutError: pass` pattern (used in M3.3, M5.1,
    runtime.py) is NOT a retry loop. Must not be flagged."""
    src = '''
import asyncio
async def run_loop():
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_3_retry_in_named_functions(path, tree)
    assert v == [], (
        f"Rule 3 falsely flagged the standard wait_for/Timeout pass "
        f"pattern; this would break M3.3, M5.1, and runtime.py. "
        f"Violations: {v}"
    )


def test_rule_4_detects_asyncio_queue() -> None:
    src = '''
import asyncio
q = asyncio.Queue()
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_4_signals_via_postgres(path, tree)
    assert v and v[0].rule == "R4"


def test_rule_4_accepts_asyncio_event() -> None:
    """asyncio.Event() is ALLOWED — it's the SIGTERM signalling
    primitive runtime.py uses. Must not be flagged."""
    src = '''
import asyncio
e = asyncio.Event()
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_4_signals_via_postgres(path, tree)
    assert v == [], (
        f"Rule 4 falsely flagged asyncio.Event(); this would break "
        f"runtime.py and every CLI entry's SIGTERM handler. "
        f"Violations: {v}"
    )


def test_rule_5_detects_module_level_mutable_state() -> None:
    src = '''
_seen = set()  # bad: hidden cross-tick state
_buffer = []   # bad: hidden cross-tick state
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_5_no_cross_workflow_state(path, tree)
    assert len(v) == 2, (
        f"Rule 5 should have flagged both module-level mutable "
        f"assignments; got {len(v)}: {v}"
    )


def test_rule_5_accepts_metrics_dict_by_name() -> None:
    """The A4 / M3.3 / M5.1 precedent: module-level `_metrics` dict
    for per-process observability is allowed. Must not be flagged."""
    src = '''
_metrics: dict[str, float] = {
    "ticks": 0.0,
}
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_5_no_cross_workflow_state(path, tree)
    assert v == [], (
        f"Rule 5 falsely flagged _metrics; this would break M3.3 / "
        f"M5.1 retroactive checks. Violations: {v}"
    )


def test_rule_5_accepts_all_caps_constants() -> None:
    """ALL_CAPS module-level dicts/lists are constants by convention."""
    src = '''
WORKFLOW_KINDS = ["fetch", "monitor"]
TOPIC_CONFIG = {"partitions": 16}
'''
    tree = _parse_snippet(src)
    path = WORKFLOWS_DIR / "feels_onboarded_monitor.py"
    v = _rule_5_no_cross_workflow_state(path, tree)
    assert v == [], (
        f"Rule 5 falsely flagged ALL_CAPS constants. Violations: {v}"
    )


# =====================================================================
# Coverage probe: ensure the analyzer ACTUALLY scanned the expected
# files. A bug in the glob could silently produce a "passes against
# empty set" green test.
# =====================================================================

def test_analyzer_actually_scans_expected_files() -> None:
    """Sanity: the workflows/ glob discovers the M6.0 Phase 2 files
    AND each retroactive precedent is reachable."""
    files = {p.name for p in _workflow_files_to_analyze()}
    must_include = {
        "state.py", "signals.py", "runtime.py", "retry.py",
        "__main__.py", "feels_onboarded_monitor.py",
    }
    missing = must_include - files
    assert not missing, (
        f"Analyzer glob did not pick up: {missing}. "
        f"Found: {sorted(files)}"
    )
    for path in RETROACTIVE_PRECEDENTS:
        assert path.exists(), f"Retroactive precedent missing: {path}"
