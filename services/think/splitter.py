"""services/think/splitter.py — atomic-model splitter.

Decomposes compound LLM-proposed `ClaimOp` entries into a list of
atomic primitives plus (when warranted) a synthesized composing
`situation` ClaimOp. Pure text heuristics — NO LLM call — so the
splitter is cheap, deterministic, and trivially testable.

Why this exists
---------------
The model-layer probe found single-row claims that pack 3-4 distinct
beliefs into one entry, e.g.:

  "HarborRail procurement evidence is delayed, sponsor confidence is
   dropping, ARR is at risk, and security review needs SOC2/data
   residency evidence"

Such compound rows collapse under embedding-based dedupe, prevent
meaningful adjudication, and ruin reconciliation. Splitting them
BEFORE reconciliation runs lets each atomic claim land on its own
Model row and be reconciled / adjudicated independently. The
synthesized `situation` carries the compositional meaning so the
"these things are jointly true" signal isn't lost.

Integration pattern (the integrator wires this; do NOT change applier
in this PR):

    In services/think/applier.py:_apply_diff(), BEFORE the per-claim_op loop:
        all_ops = []
        for op in validated.claim_ops:
            all_ops.extend(split_compound_claim_op(op))
        for op in all_ops:
            reconcile + quality_gate + apply
        AFTER inserts: resolve member_model_pending situations using the
        newly inserted atomic model IDs in the same diff.

Heuristics (all text-based, no LLM):

  * Top-level conjunction splits: " and ", ", and ", "; ", ", while ",
    ", with ", ", which means " — each conjunct must contain a
    distinct subject-verb-object pattern (rough check: each piece has
    at least one verb token).
  * Multi-kind signals: text simultaneously expresses STATE (factual
    present) AND CONCERN (worry / risk / blocker) AND/OR PREDICTION
    (future / will / by-date).
  * Compound entity scope: same entry naming >=3 distinct named
    actors / customers / workstreams in scope.

When NONE of these trigger, the splitter returns `[op]` unchanged.
False-negative is safer than over-splitting; under-confident heuristics
yield `[op]`.

Public API:
    split_compound_claim_op(op)         -> list[ClaimOp]
    is_compound(entry)                  -> tuple[bool, list[str]]

The synthesized `situation` ClaimOp uses `member_model_pending=True`
in its `entry` so the integrator can patch member_model_ids after the
atomic inserts complete in the same diff.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from .diff_schema import ClaimOp


# ---------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------

# Conjunction separators evaluated as splits at the top level of the
# claim text. Order matters: longer / more specific separators MUST
# come first so a stray " and " inside ", and " doesn't double-split.
_CONJ_SEPARATORS: tuple[str, ...] = (
    ", which means ",
    ", with ",
    ", while ",
    ", and ",
    "; ",
    " and ",
)

# Rough English verb-ish tokens used as a "this conjunct contains a
# distinct claim" sanity check. We deliberately keep this list short
# and broad — false negative (no split) is better than over-splitting.
_VERB_HINTS: frozenset[str] = frozenset({
    "is", "are", "was", "were", "be", "been", "being",
    "has", "have", "had",
    "needs", "need", "needed",
    "wants", "want",
    "delayed", "dropping", "dropped", "rising", "rose", "falling", "fell",
    "at risk", "at-risk",
    "missing", "missed",
    "blocked", "blocking",
    "approved", "approves", "approve",
    "rejected", "rejects", "reject",
    "shipped", "ships", "ship", "shipping",
    "delivered", "delivers", "deliver", "delivering",
    "will", "would", "should", "must", "may", "might",
    "expected", "expects", "expect",
    "completed", "completes", "complete",
    "review", "reviewed", "reviewing",
    "requires", "require", "required",
    "lacks", "lacking", "lacks",
    "happened", "happens", "happen", "happening",
    "occurred", "occurs", "occur",
    "increased", "decreased", "improved", "degraded",
    "churning", "churned", "churn",
    "concerned", "worried",
    "started", "starts", "start", "starting",
    "stopped", "stops", "stop", "stopping",
    "fails", "fail", "failed", "failing",
    "passes", "pass", "passed", "passing",
})

# Keyword maps for proposition-kind detection. Each entry is a regex
# pattern matched case-insensitively against the full claim text.
_STATE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"\bis\b", r"\bare\b", r"\bhas been\b", r"\bhave been\b",
        r"\bshipped\b", r"\bdelivered\b", r"\bcompleted\b",
        r"\bapproved\b", r"\bmerged\b", r"\bdeployed\b",
    )
)

_CONCERN_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"\bat risk\b", r"\bat-risk\b", r"\brisk\b", r"\bworried\b",
        r"\bconcern(ed|ing)?\b", r"\bblock(ed|er|ing)\b",
        r"\bchurn(ing|ed)?\b", r"\bdrop(ping|ped)?\b",
        r"\bmissing\b", r"\bdelayed\b", r"\bslipping\b",
        r"\bdeclin(e|ing|ed)\b", r"\bfailing\b", r"\bbroken\b",
        r"\bovers(told|sold)\b",
    )
)

_PREDICTION_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"\bwill\b", r"\bwould\b", r"\bshould\b", r"\bmust\b",
        r"\bby (next|the|tomorrow|monday|tuesday|wednesday|thursday|"
        r"friday|saturday|sunday|q[1-4]|january|february|march|april|"
        r"may|june|july|august|september|october|november|december)\b",
        r"\beta\b", r"\bdue\b", r"\bexpect(ed|s|ing)?\b",
        r"\bplan(ned|s|ning)?\b", r"\btarget(ed|s|ing)?\b",
        r"\bforecast(ed|s|ing)?\b",
    )
)

# Pressure-type keyword heuristics (best-effort). The ordering of
# checks below decides ties; revenue / compliance are checked first
# because they have the most distinctive vocabulary.
_PRESSURE_HEURISTICS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("revenue", (
        "arr", "mrr", "revenue", "churn", "renewal", "expansion",
        "deal", "pipeline", "quota", "bookings", "upsell",
    )),
    ("compliance", (
        "soc2", "soc 2", "iso27001", "iso 27001", "gdpr", "hipaa",
        "pci", "audit", "data residency", "compliance", "regulatory",
        "policy", "legal review",
    )),
    ("trust", (
        "sponsor", "confidence", "trust", "credibility", "reputation",
        "satisfaction", "nps", "csat", "frustrat", "complain",
    )),
    ("capacity", (
        "overloaded", "overload", "burnout", "burnt out", "stretched",
        "capacity", "bandwidth", "headcount", "understaffed", "backlog",
        "throughput", "swamped", "saturated",
    )),
    ("compliance", (  # second look — narrower forms
        "evidence", "attestation", "questionnaire",
    )),
    ("execution", (
        "delayed", "slipping", "blocked", "blocker", "stalled",
        "behind schedule", "ship", "delivery", "deadline",
        "missed", "rework",
    )),
    ("decision", (
        "decision", "decide", "approval", "approve", "tradeoff",
        "trade-off", "go/no-go", "go no go",
    )),
    ("market", (
        "competitor", "competitive", "market", "incumbent",
        "alternative", "displaced", "rfp",
    )),
    ("resource", (
        "budget", "spend", "cost", "investment", "hire", "hiring",
        "vendor", "license", "quota",
    )),
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _claim_text(entry: dict[str, Any]) -> str:
    """Pull the most representative human-text from an entry."""
    text_bits: list[str] = []
    natural = entry.get("natural")
    if isinstance(natural, str) and natural.strip():
        text_bits.append(natural.strip())
    prop = entry.get("proposition")
    if isinstance(prop, dict):
        for key in (
            "assertion", "nature", "summary", "situation",
            "hypothesis_text", "assessment", "observed_tendency",
            "expected",
        ):
            value = prop.get(key)
            if isinstance(value, str) and value.strip():
                text_bits.append(value.strip())
    # Dedupe but keep order.
    seen: set[str] = set()
    out: list[str] = []
    for bit in text_bits:
        if bit not in seen:
            seen.add(bit)
            out.append(bit)
    return " | ".join(out)


def _conjunct_has_verb(piece: str) -> bool:
    """True iff `piece` contains a verb-ish token suggesting a clause."""
    lowered = " " + piece.lower().strip() + " "
    for hint in _VERB_HINTS:
        # Multi-word hints like "at risk" must match with word boundaries.
        token = f" {hint} "
        if token in lowered:
            return True
    return False


def _split_top_level(text: str) -> list[str]:
    """
    Greedy top-level split on conjunction separators. Returns a list of
    >= 1 conjuncts. Each conjunct is stripped; empty conjuncts are
    dropped. Separator order matters (see _CONJ_SEPARATORS).

    Multi-text payloads (the helper joins natural + proposition fields
    with " | ") are first split on the joiner so duplicate restatements
    don't double-count.
    """
    if not text or not text.strip():
        return []
    pieces: list[str] = [p for p in text.split(" | ") if p.strip()]
    for sep in _CONJ_SEPARATORS:
        next_pieces: list[str] = []
        for piece in pieces:
            if sep in piece:
                next_pieces.extend(piece.split(sep))
            else:
                next_pieces.append(piece)
        pieces = next_pieces

    # Last-resort: split on bare commas IF each resulting sub-conjunct
    # still contains a verb-hint token. This catches "X is Y, Z is W,
    # ..., and V" style enumerations where the LLM only used "and"
    # before the final element. We deliberately gate by verb-presence
    # to avoid breaking apart noun-phrase lists like "foo, bar, baz".
    expanded: list[str] = []
    for piece in pieces:
        if ", " in piece:
            sub_pieces = [s.strip() for s in piece.split(", ") if s.strip()]
            if (
                len(sub_pieces) >= 2
                and all(_conjunct_has_verb(s) for s in sub_pieces)
            ):
                expanded.extend(sub_pieces)
                continue
        expanded.append(piece)
    return [p.strip() for p in expanded if p.strip()]


def _kind_signals(text: str) -> set[str]:
    """Return the proposition kinds the text appears to express."""
    lowered = text
    found: set[str] = set()
    for pat in _STATE_PATTERNS:
        if pat.search(lowered):
            found.add("state")
            break
    for pat in _CONCERN_PATTERNS:
        if pat.search(lowered):
            found.add("concern")
            break
    for pat in _PREDICTION_PATTERNS:
        if pat.search(lowered):
            found.add("prediction")
            break
    return found


def _distinct_entities(entry: dict[str, Any]) -> set[str]:
    """Approximate set of distinct named entities in scope."""
    out: set[str] = set()
    for actor in entry.get("scope_actors") or []:
        if actor:
            out.add(f"actor:{actor}")
    for ent in entry.get("scope_entities") or []:
        if isinstance(ent, dict):
            eid = ent.get("id")
            if eid:
                out.add(f"entity:{eid}")
    # Also pick up proper-noun-ish words in the claim text, capped to
    # avoid false positives (acronyms / sentence starts).
    text = _claim_text(entry)
    for token in re.findall(r"\b[A-Z][a-zA-Z0-9]{2,}\b", text):
        # Filter common sentence-starter words that look proper-nouny.
        if token.lower() in {
            "the", "and", "but", "however", "additionally",
            "meanwhile", "also", "further",
        }:
            continue
        out.add(f"name:{token}")
    return out


def _infer_pressure_type(text: str) -> str | None:
    """Best-effort pressure_type mapping from claim text."""
    lowered = text.lower()
    for pressure, keywords in _PRESSURE_HEURISTICS:
        for kw in keywords:
            if kw in lowered:
                return pressure
    return None


def _trim(s: str, limit: int) -> str:
    s = s.strip()
    if len(s) <= limit:
        return s
    return s[: limit - 3].rstrip() + "..."


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


def is_compound(entry: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Return (is_compound, reasons).

    `entry` is the insert-payload dict (the same shape as
    `ClaimOp.entry` for `op='insert'`). The function never touches the
    network; it only reads text fields and scope.

    Reasons returned (any combination):
      * "multi_conjunction:<N>" — N >= 2 verb-bearing conjuncts.
      * "multi_kind:<comma-joined-kinds>" — >= 2 distinct
        proposition-kind signals in the text.
      * "multi_entity:<N>" — >= 3 distinct entities in scope.
    """
    if not isinstance(entry, dict):
        return False, []
    text = _claim_text(entry)
    if not text:
        return False, []

    reasons: list[str] = []

    # Conjunction heuristic.
    conjuncts = _split_top_level(text)
    verb_conjuncts = [c for c in conjuncts if _conjunct_has_verb(c)]
    if len(verb_conjuncts) >= 2:
        reasons.append(f"multi_conjunction:{len(verb_conjuncts)}")

    # Multi-kind heuristic.
    kinds = _kind_signals(text)
    if len(kinds) >= 2:
        reasons.append("multi_kind:" + ",".join(sorted(kinds)))

    # Compound-entity heuristic.
    entities = _distinct_entities(entry)
    if len(entities) >= 3:
        reasons.append(f"multi_entity:{len(entities)}")

    return (bool(reasons), reasons)


def split_compound_claim_op(op: ClaimOp) -> list[ClaimOp]:
    """
    Decompose a compound `ClaimOp` into atomic primitives + optional
    composing `situation`.

    Behavior contract:
      * Returns `[op]` unchanged when:
        - op.op != "insert"
        - entry is missing / malformed
        - heuristics say the op is already atomic
      * Returns N >= 2 atomic ops + 1 synthesized situation op when
        compound.

    Atomic split preserves on EACH split op:
      * `op.tenant_id`-equivalent fields (this module doesn't own
        tenant; tenant lives on the enclosing ValidatedDiff).
      * `born_from_event_id`, `evidence_event_ids`
      * `confidence` / `confidence_at_assertion` (copied verbatim;
        calibration handles per-row tuning later)
      * `scope_actors`, `scope_entities`, `scope_temporal`
      * `falsifier` if present
      * `embedding` is dropped (set to None) so downstream backfills.

    The synthesized situation carries:
      * `proposition.kind = "situation"`
      * `summary` and `shared_mechanism` = trimmed compound text
      * `pressure_type` inferred via keyword heuristic
      * `status = "forming"`
      * `member_model_ids = []` (filled post-insert by the integrator)
      * `member_model_pending = True` flag on the entry itself so the
        integrator can identify which situations need patching.
    """
    if op.op != "insert" or not isinstance(op.entry, dict):
        return [op]

    entry = op.entry
    compound, reasons = is_compound(entry)
    if not compound:
        return [op]

    text = _claim_text(entry)
    conjuncts = _split_top_level(text)
    verb_conjuncts = [c for c in conjuncts if _conjunct_has_verb(c)]

    # If we can't get >=2 useful conjuncts, do NOT split — under-confident
    # heuristics yield the original (false negative is safer).
    if len(verb_conjuncts) < 2:
        return [op]

    base_entry = deepcopy(entry)
    # Strip embedding from base; each split will recompute downstream.
    base_entry.pop("embedding", None)

    split_ops: list[ClaimOp] = []
    for piece in verb_conjuncts:
        atomic_entry = deepcopy(base_entry)
        atomic_kind = _atomic_kind_for(piece)
        atomic_entry["proposition"] = _atomic_proposition(
            piece, atomic_kind, base_entry.get("proposition"),
        )
        atomic_entry["natural"] = piece.rstrip(".") + "."
        # Embedding stays None — applier / backfill will compute.
        atomic_entry.pop("embedding", None)
        split_ops.append(ClaimOp(op="insert", entry=atomic_entry))

    # Synthesize the composing situation.
    situation_entry = deepcopy(base_entry)
    situation_entry.pop("embedding", None)
    pressure_type = _infer_pressure_type(text)
    trimmed = _trim(text, 200)
    sit_prop: dict[str, Any] = {
        "kind": "situation",
        "situation": trimmed,
        "summary": trimmed,
        "member_model_ids": [],
        "relationship_summary": (
            "Atomic claims split from one compound LLM model entry; "
            "they are jointly true and share an operational context."
        ),
        "status": "forming",
        "shared_mechanism": trimmed,
    }
    if pressure_type is not None:
        sit_prop["pressure_type"] = pressure_type
    situation_entry["proposition"] = sit_prop
    situation_entry["natural"] = f"Composite situation: {trimmed}"
    # Flag for the integrator: patch member_model_ids after atomic
    # inserts in the same diff.
    situation_entry["member_model_pending"] = True
    # Echo reasons so the integrator / observability can audit why
    # this situation was synthesized.
    situation_entry["split_reasons"] = reasons

    split_ops.append(ClaimOp(op="insert", entry=situation_entry))
    return split_ops


# ---------------------------------------------------------------------
# Internal helpers for atomic proposition synthesis
# ---------------------------------------------------------------------


def _atomic_kind_for(piece: str) -> str:
    """Pick a single proposition kind for an atomic conjunct."""
    kinds = _kind_signals(piece)
    # Priority: concern > prediction > state. Concern is the most
    # information-dense and easiest to misclassify as state.
    if "concern" in kinds:
        return "concern"
    if "prediction" in kinds:
        return "prediction"
    if "state" in kinds:
        return "state"
    # Default to state — safe fallback.
    return "state"


def _atomic_proposition(
    piece: str,
    kind: str,
    original_prop: Any,
) -> dict[str, Any]:
    """Build a minimal valid proposition dict for the atomic conjunct."""
    piece_clean = piece.rstrip(".").strip()
    base_subject: Any = "compound-claim-split"
    base_raised_by: Any = "system:splitter"
    if isinstance(original_prop, dict):
        for src_key in ("subject", "about", "subject_external", "signature"):
            sval = original_prop.get(src_key)
            if isinstance(sval, (str, dict)) and sval:
                base_subject = sval
                break
        rb = original_prop.get("raised_by")
        if isinstance(rb, (str, dict)) and rb:
            base_raised_by = rb

    if kind == "concern":
        return {
            "kind": "concern",
            "about": base_subject,
            "nature": piece_clean,
            "raised_by": base_raised_by,
        }
    if kind == "prediction":
        return {
            "kind": "prediction",
            "expected": piece_clean,
            "resolution": "atomic_split_pending_resolution",
        }
    # state default
    return {
        "kind": "state",
        "subject": base_subject,
        "assertion": piece_clean,
    }


__all__ = [
    "split_compound_claim_op",
    "is_compound",
]
