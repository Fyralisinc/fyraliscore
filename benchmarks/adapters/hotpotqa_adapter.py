"""Adapter for HotpotQA distractor/fullwiki JSON files."""

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


class HotpotQAAdapter(BenchmarkAdapter):
    benchmark_name = "hotpotqa"

    def __init__(
        self,
        data_path: Path | str,
        *,
        max_cases: int | None = None,
    ) -> None:
        self.data_path = Path(data_path)
        self.max_cases = max_cases
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
            raise ValueError(f"Expected HotpotQA JSON list in {self.data_path}")
        records = [record for record in raw if isinstance(record, dict)]
        if self.max_cases is not None:
            records = records[: self.max_cases]
        self._records = records

    def preprocess(self) -> None:
        if self._records is None:
            self.load_raw()
        assert self._records is not None

        observations: list[BenchmarkObservation] = []
        queries: list[BenchmarkQuery] = []
        gold: dict[str, GoldLabels] = {}

        for record in self._records:
            query_id = str(record.get("_id") or record.get("id"))
            if not query_id or query_id == "None":
                continue
            tenant_id = f"bench_hotpotqa_{query_id}"
            supporting_facts = _supporting_fact_pairs(record.get("supporting_facts"))
            supporting_titles = {title for title, _sent_id in supporting_facts}
            gold_evidence_ids: list[str] = []

            for index, paragraph in enumerate(_context_paragraphs(record.get("context"))):
                title = paragraph["title"]
                observation_id = _observation_id(query_id, title, index)
                if title in supporting_titles:
                    gold_evidence_ids.append(observation_id)
                observations.append(
                    BenchmarkObservation(
                        observation_id=observation_id,
                        source="benchmark_hotpotqa",
                        tenant_id=tenant_id,
                        occurred_at=datetime(1970, 1, 1, tzinfo=timezone.utc),
                        content=_render_paragraph(title, paragraph["sentences"]),
                        entities=[{"type": "document_title", "name": title}],
                        metadata={
                            "question_id": query_id,
                            "title": title,
                            "paragraph_index": index,
                            "is_supporting_title": title in supporting_titles,
                            "supporting_sentence_ids": [
                                sent_id
                                for support_title, sent_id in supporting_facts
                                if support_title == title
                            ],
                            "type": record.get("type"),
                            "level": record.get("level"),
                        },
                    )
                )

            query = BenchmarkQuery(
                query_id=query_id,
                tenant_id=tenant_id,
                query_text=str(record.get("question", "")),
                query_type="multi_hop_qa",
                gold_answer=record.get("answer"),
                gold_evidence_ids=gold_evidence_ids,
                metadata={
                    "type": record.get("type"),
                    "level": record.get("level"),
                    "supporting_facts": [
                        [title, sent_id] for title, sent_id in supporting_facts
                    ],
                },
            )
            queries.append(query)
            gold[query_id] = GoldLabels(
                answer=record.get("answer"),
                evidence_ids=gold_evidence_ids,
                bridge_node_ids=sorted(supporting_titles),
                metadata={
                    "type": record.get("type"),
                    "level": record.get("level"),
                    "source_path": str(self.data_path),
                    "supporting_facts": [
                        [title, sent_id] for title, sent_id in supporting_facts
                    ],
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


def _observation_id(query_id: str, title: str, index: int) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_").lower() or "untitled"
    return f"hotpotqa:{query_id}:paragraph:{index}:{slug}"


def _supporting_fact_pairs(raw: Any) -> list[tuple[str, int]]:
    if isinstance(raw, dict):
        titles = raw.get("title") or raw.get("titles") or []
        sent_ids = raw.get("sent_id") or raw.get("sent_ids") or []
        return [
            (str(title), int(sent_id))
            for title, sent_id in zip(titles, sent_ids)
        ]
    if not isinstance(raw, list):
        return []
    pairs: list[tuple[str, int]] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            pairs.append((str(item[0]), int(item[1])))
        except (TypeError, ValueError):
            continue
    return pairs


def _context_paragraphs(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, dict):
        titles = raw.get("title") or raw.get("titles") or []
        sentences = raw.get("sentences") or raw.get("sentence") or []
        return [
            {"title": str(title), "sentences": list(sents or [])}
            for title, sents in zip(titles, sentences)
        ]
    if not isinstance(raw, list):
        return []
    paragraphs: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            title = str(item.get("title", ""))
            sentences = item.get("sentences") or item.get("sentence") or []
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            title = str(item[0])
            sentences = item[1]
        else:
            continue
        paragraphs.append({"title": title, "sentences": list(sentences or [])})
    return paragraphs


def _render_paragraph(title: str, sentences: list[Any]) -> str:
    body = " ".join(str(sentence).strip() for sentence in sentences if str(sentence).strip())
    return f"{title}\n{body}".strip()
