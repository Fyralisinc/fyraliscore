"""Adapter for the HaluMem JSONL memory QA/retrieval slice."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.adapters.base import (
    BenchmarkAdapter,
    BenchmarkObservation,
    BenchmarkQuery,
    GoldLabels,
)


class HaluMemAdapter(BenchmarkAdapter):
    benchmark_name = "halumem"

    def __init__(
        self,
        data_path: Path | str,
        *,
        max_users: int | None = None,
        max_questions: int | None = None,
    ) -> None:
        self.data_path = Path(data_path)
        self.max_users = max_users
        self.max_questions = max_questions
        self._records: list[dict[str, Any]] | None = None
        self._observations: list[BenchmarkObservation] = []
        self._queries: list[BenchmarkQuery] = []
        self._gold: dict[str, GoldLabels] = {}

    def load_raw(self) -> None:
        records: list[dict[str, Any]] = []
        with self.data_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                records.append(json.loads(line))
                if self.max_users is not None and len(records) >= self.max_users:
                    break
        self._records = records

    def preprocess(self) -> None:
        if self._records is None:
            self.load_raw()
        assert self._records is not None

        observations: list[BenchmarkObservation] = []
        queries: list[BenchmarkQuery] = []
        gold: dict[str, GoldLabels] = {}
        question_count = 0

        for user in self._records:
            user_id = str(user.get("uuid", "unknown"))
            tenant_id = f"bench_halumem_{_slug(user_id)}"
            memory_by_content: dict[str, list[str]] = {}
            sessions = list(user.get("sessions") or [])

            for session_index, session in enumerate(sessions):
                occurred_at = _parse_datetime(session.get("end_time") or session.get("start_time"))
                for memory in session.get("memory_points") or []:
                    content = str(memory.get("memory_content", "")).strip()
                    if not content:
                        continue
                    observation_id = (
                        f"halumem:{user_id}:session:{session_index}:"
                        f"memory:{memory.get('index', len(observations))}"
                    )
                    memory_by_content.setdefault(_normalize(content), []).append(observation_id)
                    observations.append(
                        BenchmarkObservation(
                            observation_id=observation_id,
                            source="benchmark_halumem",
                            tenant_id=tenant_id,
                            occurred_at=occurred_at,
                            content=content,
                            metadata={
                                "user_id": user_id,
                                "session_index": session_index,
                                "memory_index": memory.get("index"),
                                "memory_type": memory.get("memory_type"),
                                "memory_source": memory.get("memory_source"),
                                "is_update": memory.get("is_update"),
                                "importance": memory.get("importance"),
                            },
                        )
                    )

            for session_index, session in enumerate(sessions):
                for q_index, question in enumerate(session.get("questions") or []):
                    if self.max_questions is not None and question_count >= self.max_questions:
                        break
                    query_id = f"halumem:{user_id}:session:{session_index}:q:{q_index}"
                    evidence_ids = _evidence_ids(question.get("evidence"), memory_by_content)
                    answer = question.get("answer")
                    expected_abstain = not evidence_ids or _looks_unknown(answer)
                    queries.append(
                        BenchmarkQuery(
                            query_id=query_id,
                            tenant_id=tenant_id,
                            query_text=str(question.get("question", "")),
                            query_type=str(question.get("question_type", "memory_qa")),
                            gold_answer=answer,
                            gold_evidence_ids=evidence_ids,
                            metadata={
                                "user_id": user_id,
                                "session_index": session_index,
                                "difficulty": question.get("difficulty"),
                                "question_type": question.get("question_type"),
                                "expected_abstain": expected_abstain,
                            },
                        )
                    )
                    gold[query_id] = GoldLabels(
                        answer=answer,
                        evidence_ids=evidence_ids,
                        expected_abstain=expected_abstain,
                        metadata={
                            "difficulty": question.get("difficulty"),
                            "question_type": question.get("question_type"),
                            "source_path": str(self.data_path),
                        },
                    )
                    question_count += 1
                if self.max_questions is not None and question_count >= self.max_questions:
                    break
            if self.max_questions is not None and question_count >= self.max_questions:
                break

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


def _evidence_ids(
    evidence: Any,
    memory_by_content: dict[str, list[str]],
) -> list[str]:
    out: list[str] = []
    if not isinstance(evidence, list):
        return out
    for item in evidence:
        if not isinstance(item, dict):
            continue
        content = str(item.get("memory_content", "")).strip()
        if not content:
            continue
        out.extend(memory_by_content.get(_normalize(content), []))
    return list(dict.fromkeys(out))


def _normalize(value: str) -> str:
    return " ".join(value.casefold().split())


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower() or "unknown"


def _looks_unknown(value: Any) -> bool:
    text = str(value or "").strip().casefold()
    return text.startswith("unknown") or "not provided" in text or text == "i don't know"


def _parse_datetime(value: Any) -> datetime:
    if value is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    raw = str(value).strip()
    for fmt in ("%b %d, %Y, %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return datetime(1970, 1, 1, tzinfo=timezone.utc)
