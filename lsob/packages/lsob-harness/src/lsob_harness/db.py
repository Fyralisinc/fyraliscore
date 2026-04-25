"""SQLite persistence for run manifests and evaluation results.

Two shapes share the same schema:

- ``<runs_root>/<run_id>/results.db`` — per-run local DB.
- ``<runs_root>/index.db`` — global index mirroring the ``runs`` rows only.

Both use the same schema so we can cross-query trivially.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from lsob_contracts import EvalResult, RunManifest

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    manifest_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    sut TEXT NOT NULL,
    corpus TEXT NOT NULL,
    ablation TEXT NOT NULL,
    layer_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS eval_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    idx INTEGER NOT NULL,
    layer_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    value REAL NOT NULL,
    ci_low REAL,
    ci_high REAL,
    breakdown_by TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_eval_results_run ON eval_results(run_id);
CREATE INDEX IF NOT EXISTS idx_eval_results_layer ON eval_results(run_id, layer_id);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    checkpoint_at TEXT NOT NULL,
    ingested_count INTEGER NOT NULL,
    signal_id TEXT,
    last_timestamp TEXT,
    evaluators_run TEXT NOT NULL DEFAULT '[]',
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_run ON checkpoints(run_id);

CREATE TABLE IF NOT EXISTS timings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    detail TEXT,
    started_at TEXT,
    finished_at TEXT,
    duration_ms REAL NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_timings_run ON timings(run_id);
CREATE INDEX IF NOT EXISTS idx_timings_phase ON timings(run_id, phase);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open (and bootstrap) a SQLite DB at ``db_path``."""
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


@contextmanager
def open_db(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def write_manifest(conn: sqlite3.Connection, manifest: RunManifest) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO runs
            (run_id, manifest_json, started_at, finished_at, sut, corpus, ablation, layer_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            manifest.run_id,
            manifest.model_dump_json(),
            _iso(manifest.started_at),
            _iso(manifest.finished_at),
            manifest.baseline,
            manifest.corpus_uri,
            manifest.ablation.name,
            len(manifest.layers),
        ),
    )
    conn.commit()


def delete_eval_results(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute("DELETE FROM eval_results WHERE run_id = ?", (run_id,))
    conn.commit()


def delete_timings(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute("DELETE FROM timings WHERE run_id = ?", (run_id,))
    conn.commit()


def write_eval_results(
    conn: sqlite3.Connection, run_id: str, results: list[EvalResult]
) -> None:
    rows = []
    for idx, r in enumerate(results):
        ci_low, ci_high = (None, None)
        if r.confidence_interval is not None:
            ci_low, ci_high = r.confidence_interval
        rows.append(
            (
                run_id,
                idx,
                r.layer_id,
                r.metric_name,
                float(r.value),
                ci_low,
                ci_high,
                json.dumps(r.breakdown_by, default=str),
            )
        )
    conn.executemany(
        """
        INSERT INTO eval_results
            (run_id, idx, layer_id, metric_name, value, ci_low, ci_high, breakdown_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def read_eval_results(conn: sqlite3.Connection, run_id: str) -> list[EvalResult]:
    cur = conn.execute(
        """
        SELECT layer_id, metric_name, value, ci_low, ci_high, breakdown_by
        FROM eval_results WHERE run_id = ? ORDER BY idx
        """,
        (run_id,),
    )
    out: list[EvalResult] = []
    for row in cur.fetchall():
        ci = None
        if row["ci_low"] is not None and row["ci_high"] is not None:
            ci = (float(row["ci_low"]), float(row["ci_high"]))
        out.append(
            EvalResult(
                layer_id=int(row["layer_id"]),
                metric_name=row["metric_name"],
                value=float(row["value"]),
                confidence_interval=ci,
                breakdown_by=json.loads(row["breakdown_by"] or "{}"),
                run_id=run_id,
            )
        )
    return out


def read_manifest(conn: sqlite3.Connection, run_id: str) -> RunManifest | None:
    cur = conn.execute("SELECT manifest_json FROM runs WHERE run_id = ?", (run_id,))
    row = cur.fetchone()
    if not row:
        return None
    return RunManifest.model_validate_json(row["manifest_json"])


def list_runs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT run_id, started_at, finished_at, sut, corpus, ablation, layer_count
        FROM runs ORDER BY started_at DESC
        """
    )
    return [dict(r) for r in cur.fetchall()]


def update_finished(
    conn: sqlite3.Connection, run_id: str, finished_at: datetime, manifest: RunManifest
) -> None:
    conn.execute(
        "UPDATE runs SET finished_at = ?, manifest_json = ? WHERE run_id = ?",
        (_iso(finished_at), manifest.model_dump_json(), run_id),
    )
    conn.commit()


def write_checkpoint(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    checkpoint_at: datetime,
    ingested_count: int,
    signal_id: str | None,
    last_timestamp: datetime | None,
    evaluators_run: list[str] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO checkpoints
            (run_id, checkpoint_at, ingested_count, signal_id, last_timestamp, evaluators_run)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            _iso(checkpoint_at),
            int(ingested_count),
            signal_id,
            _iso(last_timestamp),
            json.dumps(evaluators_run or []),
        ),
    )
    conn.commit()


def read_latest_checkpoint(
    conn: sqlite3.Connection, run_id: str
) -> dict[str, Any] | None:
    cur = conn.execute(
        """
        SELECT checkpoint_at, ingested_count, signal_id, last_timestamp, evaluators_run
        FROM checkpoints
        WHERE run_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "checkpoint_at": row["checkpoint_at"],
        "ingested_count": int(row["ingested_count"]),
        "signal_id": row["signal_id"],
        "last_timestamp": row["last_timestamp"],
        "evaluators_run": json.loads(row["evaluators_run"] or "[]"),
    }


def write_timing(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    phase: str,
    duration_ms: float,
    detail: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO timings (run_id, phase, detail, started_at, finished_at, duration_ms)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            phase,
            detail,
            _iso(started_at),
            _iso(finished_at),
            float(duration_ms),
        ),
    )
    conn.commit()


def write_timings_batch(
    conn: sqlite3.Connection,
    run_id: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    payload = [
        (
            run_id,
            r.get("phase"),
            r.get("detail"),
            _iso(r.get("started_at")) if isinstance(r.get("started_at"), datetime) else r.get("started_at"),
            _iso(r.get("finished_at")) if isinstance(r.get("finished_at"), datetime) else r.get("finished_at"),
            float(r.get("duration_ms", 0.0)),
        )
        for r in rows
    ]
    conn.executemany(
        """
        INSERT INTO timings (run_id, phase, detail, started_at, finished_at, duration_ms)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()


def read_timings(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT phase, detail, started_at, finished_at, duration_ms
        FROM timings
        WHERE run_id = ?
        ORDER BY id
        """,
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]
