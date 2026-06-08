"""Adapter for LongMemEval cleaned JSON files.

The public dataset format is documented in the official LongMemEval
repository. Each instance has a question, answer, timestamped history
sessions, and answer-session ids for retrieval evaluation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.adapters.base import (
    BenchmarkAdapter,
    BenchmarkObservation,
    BenchmarkQuery,
    GoldLabels,
)


class LongMemEvalAdapter(BenchmarkAdapter):
    benchmark_name = "longmemeval"

    def __init__(
        self,
        data_path: Path | str,
        *,
        max_cases: int | None = None,
        include_abstention: bool = True,
    ) -> None:
        self.data_path = Path(data_path)
        self.max_cases = max_cases
        self.include_abstention = include_abstention
        self._records: list[dict[str, Any]] | None = None
        self._observations: list[BenchmarkObservation] = []
        self._queries: list[BenchmarkQuery] = []
        self._gold: dict[str, GoldLabels] = {}

    def load_raw(self) -> None:
        raw = json.loads(self.data_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            for key in ("data", "records", "examples"):
                if isinstance(raw.get(key), list):
                    raw = raw[key]
                    break
        if not isinstance(raw, list):
            raise ValueError(f"Expected LongMemEval JSON list in {self.data_path}")

        records: list[dict[str, Any]] = []
        for record in raw:
            if not isinstance(record, dict):
                continue
            if not self.include_abstention and _is_abstention(record):
                continue
            records.append(record)
            if self.max_cases is not None and len(records) >= self.max_cases:
                break
        self._records = records

    def preprocess(self) -> None:
        if self._records is None:
            self.load_raw()
        assert self._records is not None

        observations: list[BenchmarkObservation] = []
        queries: list[BenchmarkQuery] = []
        gold: dict[str, GoldLabels] = {}

        for record in self._records:
            query_id = str(record["question_id"])
            tenant_id = f"bench_longmemeval_{query_id}"
            session_ids = [str(item) for item in record.get("haystack_session_ids", [])]
            session_dates = list(record.get("haystack_dates", []))
            sessions = list(record.get("haystack_sessions", []))
            answer_session_ids = [
                _observation_id(query_id, str(session_id))
                for session_id in record.get("answer_session_ids", [])
            ]

            for index, session in enumerate(sessions):
                session_id = session_ids[index] if index < len(session_ids) else str(index)
                occurred_at = _parse_datetime(
                    session_dates[index] if index < len(session_dates) else None
                )
                observations.append(
                    BenchmarkObservation(
                        observation_id=_observation_id(query_id, session_id),
                        source="benchmark_longmemeval",
                        tenant_id=tenant_id,
                        occurred_at=occurred_at,
                        content=_render_session(session),
                        metadata={
                            "question_id": query_id,
                            "question_type": record.get("question_type"),
                            "session_id": session_id,
                            "session_index": index,
                            "has_answer_turn": _session_has_answer(session),
                            "turn_count": len(session) if isinstance(session, list) else None,
                        },
                    )
                )

            query = BenchmarkQuery(
                query_id=query_id,
                tenant_id=tenant_id,
                query_text=str(record.get("question", "")),
                query_type=str(record.get("question_type", "memory_qa")),
                constraints={"question_date": record.get("question_date")},
                gold_answer=record.get("answer"),
                gold_evidence_ids=answer_session_ids,
                metadata={
                    "expected_abstain": _is_abstention(record),
                    "answer_session_ids": list(record.get("answer_session_ids", [])),
                    "question_date": record.get("question_date"),
                },
            )
            queries.append(query)
            gold[query_id] = GoldLabels(
                answer=record.get("answer"),
                evidence_ids=answer_session_ids,
                expected_abstain=_is_abstention(record),
                metadata={
                    "question_type": record.get("question_type"),
                    "question_date": record.get("question_date"),
                    "source_path": str(self.data_path),
                },
            )

        self._observations = observations
        self._queries = queries
        self._gold = gold

    def iter_observations(self):
        if not self._observations:
            self.preprocess()
        yield from self._observations

    def iter_queries(self):
        if not self._queries:
            self.preprocess()
        yield from self._queries

    def gold(self, query_id: str):
        if not self._gold:
            self.preprocess()
        return self._gold[query_id]


def _observation_id(query_id: str, session_id: str) -> str:
    return f"longmemeval:{query_id}:session:{session_id}"


def _is_abstention(record: dict[str, Any]) -> bool:
    return str(record.get("question_id", "")).endswith("_abs")


def _session_has_answer(session: Any) -> bool:
    if not isinstance(session, list):
        return False
    return any(bool(turn.get("has_answer")) for turn in session if isinstance(turn, dict))


def _render_session(session: Any) -> str:
    if isinstance(session, str):
        return session
    if not isinstance(session, list):
        return json.dumps(session, ensure_ascii=False, sort_keys=True)
    lines: list[str] = []
    for turn in session:
        if not isinstance(turn, dict):
            lines.append(str(turn))
            continue
        role = str(turn.get("role", "unknown")).strip() or "unknown"
        content = str(turn.get("content", "")).strip()
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _parse_datetime(value: Any) -> datetime:
    if value is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    raw = str(value).strip()
    if not raw:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    raw = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime(1970, 1, 1, tzinfo=timezone.utc)
