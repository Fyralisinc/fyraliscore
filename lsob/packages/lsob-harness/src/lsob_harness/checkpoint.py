"""Checkpoint / resume support for :func:`lsob_harness.runner.run_once`.

A checkpoint file captures enough state to resume a run without
re-ingesting signals the SUT already saw:

- ``run_id``
- ``corpus_hash`` — SHA256 of the corpus file bytes; used to detect
  corpus drift on resume.
- ``last_signal_id`` — the last successfully-ingested signal.
- ``last_timestamp`` — ISO-8601 of the last signal's timestamp.
- ``ingested_count`` — monotonic counter across the full run.
- ``evaluators_run`` — list of ``"L{layer}:{metric_name}"`` strings for
  evaluators whose results have been flushed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


CHECKPOINT_FILENAME = "checkpoint.json"


class CorpusHashMismatch(RuntimeError):
    """Raised when a resume is attempted against a modified corpus."""


@dataclass
class CheckpointState:
    run_id: str
    corpus_hash: str
    last_signal_id: str | None = None
    last_timestamp: str | None = None
    ingested_count: int = 0
    evaluators_run: list[str] = field(default_factory=list)
    updated_at: str | None = None

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "corpus_hash": self.corpus_hash,
            "last_signal_id": self.last_signal_id,
            "last_timestamp": self.last_timestamp,
            "ingested_count": self.ingested_count,
            "evaluators_run": list(self.evaluators_run),
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "CheckpointState":
        return cls(
            run_id=str(raw["run_id"]),
            corpus_hash=str(raw["corpus_hash"]),
            last_signal_id=raw.get("last_signal_id"),
            last_timestamp=raw.get("last_timestamp"),
            ingested_count=int(raw.get("ingested_count", 0)),
            evaluators_run=list(raw.get("evaluators_run", [])),
            updated_at=raw.get("updated_at"),
        )


def corpus_file_hash(path: str | Path) -> str:
    """Return SHA256 hex digest of the corpus file's raw bytes."""
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def write_checkpoint_file(run_dir: Path, state: CheckpointState) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    state.updated_at = datetime.now(timezone.utc).isoformat()
    target = run_dir / CHECKPOINT_FILENAME
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state.to_dict(), indent=2))
    tmp.replace(target)
    return target


def read_checkpoint_file(run_dir: Path) -> CheckpointState | None:
    target = run_dir / CHECKPOINT_FILENAME
    if not target.exists():
        return None
    raw = json.loads(target.read_text())
    return CheckpointState.from_dict(raw)
