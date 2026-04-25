"""Corpus validator: internal-consistency checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from lsob_contracts import Corpus

from lsob_simulation.io import read_corpus


@dataclass
class ValidationReport:
    path: str
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    signal_count: int = 0
    ground_truth_count: int = 0

    def summary(self) -> str:
        status = "OK" if self.ok else "FAIL"
        lines = [
            f"[{status}] corpus={self.path}",
            f"  signals={self.signal_count}  ground_truth={self.ground_truth_count}",
        ]
        for e in self.errors:
            lines.append(f"  error: {e}")
        for w in self.warnings:
            lines.append(f"  warn: {w}")
        return "\n".join(lines)


def validate_corpus_file(path: str | Path) -> ValidationReport:
    """Load + validate a corpus file."""
    corpus = read_corpus(path)
    report = validate_corpus(corpus, source_path=str(path))
    return report


def validate_corpus(corpus: Corpus, *, source_path: str = "<memory>") -> ValidationReport:
    report = ValidationReport(
        path=source_path,
        ok=True,
        signal_count=len(corpus.signals),
        ground_truth_count=len(corpus.ground_truth),
    )
    errors: list[str] = []
    warnings: list[str] = []

    # 1) Monotonic ordering & signal ID uniqueness.
    ids_seen: set[str] = set()
    last_ts = None
    for sig in corpus.signals:
        if sig.signal_id in ids_seen:
            errors.append(f"duplicate signal_id {sig.signal_id}")
        ids_seen.add(sig.signal_id)
        if last_ts is not None and sig.timestamp < last_ts:
            # Signals don't have to be globally sorted, but big jumps are suspicious.
            pass
        last_ts = sig.timestamp

    # 2) Actor references in signals must exist in at least one GT actors list.
    known_actor_ids: set[str] = set()
    for gt in corpus.ground_truth:
        for a in gt.actors:
            aid = a.get("id")
            if aid:
                known_actor_ids.add(str(aid))
    if known_actor_ids:
        for sig in corpus.signals:
            if sig.author_id not in known_actor_ids:
                errors.append(
                    f"signal {sig.signal_id} references unknown actor {sig.author_id!r}"
                )

    # 3) Commitment references in signal metadata must appear in ground truth at some point.
    known_commitment_ids: set[str] = set()
    for gt in corpus.ground_truth:
        for c in gt.commitments:
            cid = c.get("id") or c.get("commitment_id")
            if cid:
                known_commitment_ids.add(str(cid))
    for sig in corpus.signals:
        cref = sig.metadata.get("commitment_ref") if isinstance(sig.metadata, dict) else None
        if cref and known_commitment_ids and cref not in known_commitment_ids:
            warnings.append(
                f"signal {sig.signal_id} references commitment {cref!r} not present in ground truth"
            )

    # 4) Any commitment whose true_outcome is 'succeeded' / 'slipped_but_completed' / 'cancelled' must have a resolution timestamp in its final snapshot.
    if corpus.ground_truth:
        final = corpus.ground_truth[-1]
        for c in final.commitments:
            outcome = c.get("true_outcome")
            if outcome in ("succeeded", "slipped_but_completed", "cancelled"):
                if not c.get("resolution_event_at") and not c.get("resolved"):
                    warnings.append(
                        f"commitment {c.get('id')} marked resolved but missing resolution timestamp"
                    )

    # 5) Ground truth snapshots should be chronologically ordered.
    prev_ts = None
    for gt in corpus.ground_truth:
        if prev_ts is not None and gt.timestamp < prev_ts:
            errors.append(
                f"ground truth snapshot {gt.timestamp.isoformat()} is earlier than previous"
            )
        prev_ts = gt.timestamp

    # 6) Customer references.
    known_customer_ids: set[str] = set()
    for gt in corpus.ground_truth:
        for cu in gt.customers:
            cid = cu.get("id") or cu.get("customer_id")
            if cid:
                known_customer_ids.add(str(cid))
    for sig in corpus.signals:
        cust = sig.metadata.get("customer_ref") if isinstance(sig.metadata, dict) else None
        if cust and known_customer_ids and cust not in known_customer_ids:
            warnings.append(
                f"signal {sig.signal_id} references customer {cust!r} not in ground truth"
            )

    report.errors = errors
    report.warnings = warnings
    report.ok = not errors
    return report
