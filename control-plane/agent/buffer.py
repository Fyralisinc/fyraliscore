"""buffer — a durable, bounded local queue for un-sent heartbeats (I3).

When the console is unreachable the agent must **never block local ops and never
crash** — it parks the heartbeat here and retries later. The queue is an
append-only JSONL file so it survives an agent restart (a deployment that was
offline for an hour still flushes its backlog when it comes back).

Design
------
* **Append-only JSONL**: one ``DeploymentRecord`` JSON dict per line. Appends are
  O(1); a flush rewrites the file with whatever could not be delivered.
* **Bounded**: at most ``max_records`` lines. When full, the *oldest* record is
  dropped (a stale heartbeat is worthless; the freshest fleet state matters most).
  Dropping is logged by the caller.
* **Corruption-tolerant**: a malformed line is skipped on read, never fatal.
* **No locking primitives needed**: a single agent process owns its buffer file;
  operations are simple read-modify-write under the daemon's own loop.

This module is pure local file I/O — no network, no imports from the network
stack — so it is trivially testable.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

__all__ = ["HeartbeatBuffer"]


class HeartbeatBuffer:
    def __init__(self, path: "str | Path", *, max_records: int = 10_000) -> None:
        self.path = Path(path)
        self.max_records = max(1, int(max_records))

    # -- introspection ------------------------------------------------------

    def __len__(self) -> int:
        return self.count()

    def count(self) -> int:
        if not self.path.is_file():
            return 0
        n = 0
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    n += 1
        return n

    def is_empty(self) -> bool:
        return self.count() == 0

    # -- write --------------------------------------------------------------

    def append(self, record: dict) -> bool:
        """Append one record (a C4 deployment-record dict).

        Returns ``True`` if an oldest record was evicted to stay within the cap.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, separators=(",", ":"), sort_keys=True)
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        # Enforce the cap (drop oldest) if we just exceeded it.
        if self.count() > self.max_records:
            self._trim_to_cap()
            return True
        return False

    def _trim_to_cap(self) -> None:
        records = self.read_all()
        if len(records) <= self.max_records:
            return
        keep = records[-self.max_records :]  # newest max_records
        self._rewrite(keep)

    # -- read ---------------------------------------------------------------

    def read_all(self) -> list[dict]:
        """All buffered records, oldest first. Skips/ignores corrupt lines."""
        if not self.path.is_file():
            return []
        out: list[dict] = []
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue  # corruption-tolerant: skip the bad line
        return out

    # -- flush --------------------------------------------------------------

    def flush(self, sender) -> tuple[int, int]:
        """Try to deliver every buffered record via ``sender(record) -> bool``.

        Delivers oldest-first. A record for which ``sender`` returns ``True`` (or
        does not raise) is considered delivered and removed; the first failure
        stops the flush and the remaining records (including the failed one) are
        retained for the next attempt — preserving order and not dropping data.

        Returns ``(delivered, remaining)``.
        """
        records = self.read_all()
        if not records:
            return 0, 0

        delivered = 0
        for i, rec in enumerate(records):
            try:
                ok = sender(rec)
            except Exception:
                ok = False
            if not ok:
                # Stop on first failure; keep this record and all after it.
                self._rewrite(records[i:])
                return delivered, len(records) - delivered
            delivered += 1

        # Everything went out.
        self._rewrite([])
        return delivered, 0

    # -- internal -----------------------------------------------------------

    def _rewrite(self, records: list[dict]) -> None:
        """Atomically replace the buffer file with ``records`` (oldest first)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not records:
            # Empty queue: truncate the file (keep it present for clarity).
            with open(self.path, "w", encoding="utf-8"):
                pass
            return
        fd, tmp = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=self.path.name + ".", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                for rec in records:
                    fh.write(json.dumps(rec, separators=(",", ":"), sort_keys=True) + "\n")
            os.replace(tmp, self.path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
