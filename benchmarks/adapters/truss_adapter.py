"""Adapter for the Truss authored scenario fixture.

The Truss directories are benchmark source data, not measured Fyralis output.
This adapter replays only the authored company signals and scores against a
frozen signal-derivable fact checklist.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from benchmarks.adapters.base import (
    BenchmarkAdapter,
    BenchmarkObservation,
    BenchmarkQuery,
    GoldLabels,
)


class TrussAdapter(BenchmarkAdapter):
    benchmark_name = "truss"

    def __init__(
        self,
        data_path: Path | str,
        *,
        include_run1: bool = True,
        include_run2: bool = True,
        fact_filter_path: Path | str | None = None,
        max_cases: int | None = None,
        tenant_id: str = "truss_company_replay",
    ) -> None:
        self.data_path = Path(data_path)
        self.include_run1 = include_run1
        self.include_run2 = include_run2
        self.fact_filter_path = (
            Path(fact_filter_path)
            if fact_filter_path is not None
            else Path("benchmarks/truss_signal_derivable_facts.json")
        )
        self.max_cases = max_cases
        self.tenant_id = tenant_id
        self._signals: list[dict[str, Any]] = []
        self._observations: list[BenchmarkObservation] = []
        self._queries: list[BenchmarkQuery] = []
        self._gold: dict[str, GoldLabels] = {}

    def load_raw(self) -> None:
        roots = _resolve_roots(self.data_path)
        signals: list[dict[str, Any]] = []
        if self.include_run1:
            signals.extend(_load_signals(roots["run1"]))
        if self.include_run2:
            signals.extend(_load_signals(roots["run2"]))
        signals.sort(key=lambda item: (int(item.get("sim_day") or 0), str(item.get("occurred_at") or ""), str(item.get("external_id") or "")))
        self._signals = signals

    def preprocess(self) -> None:
        if not self._signals:
            self.load_raw()
        observations: list[BenchmarkObservation] = []
        seen_observation_ids: set[str] = set()
        for signal in self._signals:
            external_id = str(signal.get("external_id") or "").strip()
            if not external_id:
                continue
            occurred_at = _parse_dt(signal.get("occurred_at"))
            observation = BenchmarkObservation(
                observation_id=external_id,
                source=str(signal.get("source_channel") or "truss"),
                tenant_id=self.tenant_id,
                occurred_at=occurred_at,
                content=str(signal.get("content_text") or _content_text(signal)),
                entities=_normalise_entities(signal.get("entities_hint")),
                metadata={
                    "benchmark": "truss",
                    "sim_day": signal.get("sim_day"),
                    "kind": signal.get("kind"),
                    "source_actor_ref": signal.get("source_actor_ref"),
                    "fixture_run": "run2" if int(signal.get("sim_day") or 0) >= 61 else "run1",
                },
                trust_tier=_trust_tier(signal.get("trust_tier")),
            )
            observations.append(observation)
            seen_observation_ids.add(external_id)

        queries: list[BenchmarkQuery] = []
        gold: dict[str, GoldLabels] = {}
        facts = _load_facts(self.fact_filter_path)
        for fact in facts:
            evidence_ids = [str(eid) for eid in fact.get("evidence_ids") or []]
            if evidence_ids and not set(evidence_ids).issubset(seen_observation_ids):
                continue
            query_id = str(fact["id"])
            query = BenchmarkQuery(
                query_id=query_id,
                tenant_id=self.tenant_id,
                query_text=str(fact["question"]),
                query_type="company_memory_fact",
                gold_answer=str(fact.get("answer") or ""),
                gold_evidence_ids=evidence_ids,
                metadata={
                    "benchmark": "truss",
                    "requires_run1_memory": bool(fact.get("requires_run1_memory")),
                },
            )
            queries.append(query)
            gold[query_id] = GoldLabels(
                answer=query.gold_answer,
                evidence_ids=evidence_ids,
                metadata={
                    "benchmark": "truss",
                    "requires_run1_memory": bool(fact.get("requires_run1_memory")),
                    "source_filter": str(self.fact_filter_path),
                },
            )
            if self.max_cases is not None and len(queries) >= self.max_cases:
                break

        self._observations = observations
        self._queries = queries
        self._gold = gold

    def iter_observations(self) -> Iterable[BenchmarkObservation]:
        if not self._observations:
            self.preprocess()
        yield from self._observations

    def iter_queries(self) -> Iterable[BenchmarkQuery]:
        if not self._queries:
            self.preprocess()
        yield from self._queries

    def gold(self, query_id: str) -> GoldLabels:
        if not self._gold:
            self.preprocess()
        return self._gold[query_id]


def _resolve_roots(path: Path) -> dict[str, Path]:
    if (path / "signals").exists():
        if path.name == "truss_run_2":
            root = path.parent
        else:
            root = path.parent
    else:
        root = path
    run1 = root / "truss_run"
    run2 = root / "truss_run_2"
    for expected in (run1, run2):
        if not expected.exists():
            raise FileNotFoundError(f"Truss fixture directory not found: {expected}")
    return {"run1": run1, "run2": run2}


def _load_signals(root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((root / "signals").glob("day_*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                item = json.loads(line)
                if isinstance(item, dict):
                    out.append(item)
    return out


def _load_facts(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    facts = payload.get("facts") if isinstance(payload, dict) else None
    if not isinstance(facts, list):
        raise ValueError(f"Expected facts list in {path}")
    return [fact for fact in facts if isinstance(fact, dict)]


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _content_text(signal: dict[str, Any]) -> str:
    content = signal.get("content")
    if isinstance(content, dict):
        text = content.get("text")
        if isinstance(text, str):
            return text
    return json.dumps(signal, sort_keys=True, default=str)


def _normalise_entities(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            entity_type = item.get("type") or "entity"
            ref = item.get("ref") or item.get("id")
            if ref is not None:
                out.append({"type": str(entity_type), "ref": str(ref)})
        elif isinstance(item, str):
            out.append({"type": "entity", "ref": item})
    return out


def _trust_tier(value: Any) -> str:
    value_s = str(value or "").casefold()
    if value_s in {"direct", "authoritative", "benchmark_gold"}:
        return "authoritative"
    if value_s in {"inferential", "reputable", "unverified"}:
        return "unvetted" if value_s == "unverified" else value_s
    return "authoritative"


__all__ = ["TrussAdapter"]
