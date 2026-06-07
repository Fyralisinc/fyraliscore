"""Adapter for the public MEMTRACK archive.

MEMTRACK's public Drive archive contains scenario config YAML files plus
chronological event history JSON files. The full paper benchmark is an
interactive agent environment; this adapter intentionally provides a
retrieval-only view over the public event timelines without exposing the
benchmark questions or expected answers as retrievable observations.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

from benchmarks.adapters.base import (
    BenchmarkAdapter,
    BenchmarkObservation,
    BenchmarkQuery,
    GoldLabels,
)


_ANSWER_SUPPORT_THRESHOLD = 0.75
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_.:/#-]*", re.IGNORECASE)
_STOPWORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "were",
    "what",
    "which",
    "who",
    "with",
}


class MemTrackAdapter(BenchmarkAdapter):
    benchmark_name = "memtrack"

    def __init__(
        self,
        data_path: Path | str,
        *,
        max_cases: int | None = None,
        include_config_observation: bool = True,
    ) -> None:
        self.data_path = Path(data_path)
        self.max_cases = max_cases
        self.include_config_observation = include_config_observation
        self._cases: list[dict[str, Any]] | None = None
        self._observations: list[BenchmarkObservation] = []
        self._queries: list[BenchmarkQuery] = []
        self._gold: dict[str, GoldLabels] = {}

    def load_raw(self) -> None:
        root = _dataset_root(self.data_path)
        config_dir = root / "test_configs"
        history_dir = root / "test_event_histories"
        if not config_dir.exists() or not history_dir.exists():
            raise FileNotFoundError(
                "MEMTRACK archive root must contain test_configs/ and "
                f"test_event_histories/: {root}"
            )

        cases: list[dict[str, Any]] = []
        for config_path in sorted(config_dir.glob("*.yaml")):
            raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if not isinstance(raw_config, dict):
                continue
            benchmark = raw_config.get("benchmark")
            if not isinstance(benchmark, dict):
                continue
            history_rel = benchmark.get("event_history")
            if not history_rel:
                continue
            history_path = root / str(history_rel)
            if not history_path.exists():
                history_path = history_dir / Path(str(history_rel)).name
            if not history_path.exists():
                raise FileNotFoundError(
                    f"MEMTRACK event history missing for {config_path.name}: {history_rel}"
                )
            events = json.loads(history_path.read_text(encoding="utf-8"))
            if not isinstance(events, list):
                raise ValueError(f"Expected list event history in {history_path}")
            case_id = _case_id(config_path)
            cases.append({
                "case_id": case_id,
                "config_path": config_path,
                "history_path": history_path,
                "config": raw_config,
                "events": events,
            })
            if self.max_cases is not None and len(cases) >= self.max_cases:
                break
        self._cases = cases

    def preprocess(self) -> None:
        if self._cases is None:
            self.load_raw()
        assert self._cases is not None

        observations: list[BenchmarkObservation] = []
        observations_by_case: dict[str, list[BenchmarkObservation]] = {}
        queries: list[BenchmarkQuery] = []
        gold: dict[str, GoldLabels] = {}

        for case in self._cases:
            case_id = str(case["case_id"])
            config = case["config"]
            benchmark = config.get("benchmark") or {}
            if not isinstance(benchmark, dict):
                continue
            questions = [str(q) for q in benchmark.get("questions") or []]
            answers = [str(a) for a in benchmark.get("expected_answers") or []]
            tenant_id = f"memtrack:{case_id}"
            case_observations: list[BenchmarkObservation] = []

            if self.include_config_observation:
                config_observation = _config_observation(
                    tenant_id=tenant_id,
                    case_id=case_id,
                    config=config,
                    config_path=case["config_path"],
                    history_path=case["history_path"],
                )
                observations.append(config_observation)
                case_observations.append(config_observation)

            for index, event in enumerate(case["events"]):
                if not isinstance(event, dict):
                    continue
                observation = _event_observation(
                    tenant_id=tenant_id,
                    case_id=case_id,
                    index=index,
                    event=event,
                    config_path=case["config_path"],
                    history_path=case["history_path"],
                )
                observations.append(observation)
                case_observations.append(observation)
            observations_by_case[case_id] = case_observations

            for question_index, question in enumerate(questions):
                answer = answers[question_index] if question_index < len(answers) else None
                query_id = f"{case_id}:q{question_index + 1}"
                supporting_ids, best_support = _supporting_observation_ids(
                    answer,
                    case_observations,
                )
                case_support = answer_support_score(
                    "\n".join(observation.content for observation in case_observations),
                    answer,
                )
                answer_observable = case_support >= _ANSWER_SUPPORT_THRESHOLD
                required_tool_surfaces = _required_tool_surfaces(question)
                query = BenchmarkQuery(
                    query_id=query_id,
                    tenant_id=tenant_id,
                    query_text=question,
                    query_type=_question_type(question),
                    gold_answer=answer,
                    gold_evidence_ids=supporting_ids,
                    metadata={
                        "benchmark": "MEMTRACK",
                        "case_id": case_id,
                        "question_index": question_index,
                        "config_file": case["config_path"].name,
                        "event_history_file": case["history_path"].name,
                        "repository_name": (config.get("repository") or {}).get("name"),
                        "timeline_event_count": len(case["events"]),
                        "gold_answer_observable": answer_observable,
                        "gold_answer_case_support_score": round(case_support, 6),
                        "gold_answer_single_observation_observable": bool(supporting_ids),
                        "gold_answer_best_observation_support": best_support,
                        "support_threshold": _ANSWER_SUPPORT_THRESHOLD,
                        "requires_external_tool_surface": bool(required_tool_surfaces),
                        "required_tool_surfaces": required_tool_surfaces,
                    },
                )
                queries.append(query)
                gold[query_id] = GoldLabels(
                    answer=answer,
                    evidence_ids=supporting_ids,
                    metadata={
                        "benchmark": "MEMTRACK",
                        "case_id": case_id,
                        "question_index": question_index,
                        "config_file": case["config_path"].name,
                        "event_history_file": case["history_path"].name,
                        "gold_answer_observable": answer_observable,
                        "gold_answer_case_support_score": round(case_support, 6),
                        "gold_answer_single_observation_observable": bool(supporting_ids),
                        "gold_answer_best_observation_support": best_support,
                        "support_threshold": _ANSWER_SUPPORT_THRESHOLD,
                        "requires_external_tool_surface": bool(required_tool_surfaces),
                        "required_tool_surfaces": required_tool_surfaces,
                    },
                )

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


def _dataset_root(path: Path) -> Path:
    if (path / "test_configs").exists() and (path / "test_event_histories").exists():
        return path
    nested = path / "Memtrak"
    if (nested / "test_configs").exists() and (nested / "test_event_histories").exists():
        return nested
    return path


def _case_id(config_path: Path) -> str:
    stem = config_path.stem
    return stem.removeprefix("config_")


def _config_observation(
    *,
    tenant_id: str,
    case_id: str,
    config: dict[str, Any],
    config_path: Path,
    history_path: Path,
) -> BenchmarkObservation:
    repository = config.get("repository") if isinstance(config.get("repository"), dict) else {}
    linear = config.get("linear") if isinstance(config.get("linear"), dict) else {}
    agent = config.get("agent") if isinstance(config.get("agent"), dict) else {}
    lines = [
        "MEMTRACK scenario setup",
        f"Case: {case_id}",
        f"Repository name: {repository.get('name')}",
        f"Repository url: {repository.get('url')}",
        f"Repository branch: {repository.get('branch')}",
        f"Repository commit: {repository.get('commit')}",
        f"Local repository name: {repository.get('local_name')}",
        f"Agent title: {agent.get('title')}",
        f"Agent tools: {', '.join(str(tool) for tool in agent.get('tools') or [])}",
        f"Linear teams: {_compact_json(linear.get('teams') or [])}",
        f"Linear milestones: {_compact_json(linear.get('milestones') or [])}",
    ]
    return BenchmarkObservation(
        observation_id=f"memtrack:{case_id}:config",
        source="benchmark_memtrack_config",
        tenant_id=tenant_id,
        occurred_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        content="\n".join(line for line in lines if line.strip()),
        entities=_config_entities(config),
        metadata={
            "benchmark": "MEMTRACK",
            "case_id": case_id,
            "observation_kind": "scenario_config",
            "config_file": config_path.name,
            "event_history_file": history_path.name,
            "repository_name": repository.get("name"),
            "repository_url": repository.get("url"),
            "repository_branch": repository.get("branch"),
            "repository_commit": repository.get("commit"),
        },
    )


def _event_observation(
    *,
    tenant_id: str,
    case_id: str,
    index: int,
    event: dict[str, Any],
    config_path: Path,
    history_path: Path,
) -> BenchmarkObservation:
    meta = event.get("generation_meta_data")
    if not isinstance(meta, dict):
        meta = {}
    occurred_at = _parse_timestamp(str(event.get("timestamp") or ""), index=index)
    platform = str(event.get("platform") or "unknown")
    lines = [
        "MEMTRACK timeline event",
        f"Case: {case_id}",
        f"Event index: {index}",
        f"Timestamp: {event.get('timestamp')}",
        f"Platform: {platform}",
        f"Generation type: {event.get('generation_type')}",
    ]
    for key in sorted(meta):
        lines.append(f"{_label(key)}: {_render_value(meta[key])}")
    return BenchmarkObservation(
        observation_id=f"memtrack:{case_id}:event:{index:04d}",
        source=f"benchmark_memtrack_{platform}",
        tenant_id=tenant_id,
        occurred_at=occurred_at,
        content="\n".join(lines),
        entities=_event_entities(platform, meta),
        metadata={
            "benchmark": "MEMTRACK",
            "case_id": case_id,
            "observation_kind": "timeline_event",
            "event_index": index,
            "platform": platform,
            "generation_type": event.get("generation_type"),
            "timestamp_raw": event.get("timestamp"),
            "config_file": config_path.name,
            "event_history_file": history_path.name,
            "sender": meta.get("sender"),
            "channel": meta.get("channel"),
            "team": meta.get("team"),
            "lead": meta.get("lead"),
            "status": meta.get("status"),
            "priority": meta.get("priority"),
            "title": meta.get("title"),
            "commit_id": meta.get("commit_id"),
            "pr": meta.get("pr"),
        },
    )


def _parse_timestamp(raw: str, *, index: int) -> datetime:
    for fmt in ("%Y%m%dT%H%M", "%Y%m%dT%H%M%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index)


def _render_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return _compact_json(value)
    return str(value)


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _label(key: str) -> str:
    return key.replace("_", " ").strip().title()


def _config_entities(config: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    repository = config.get("repository") if isinstance(config.get("repository"), dict) else {}
    for key in ("name", "local_name"):
        if repository.get(key):
            out.append({"type": "repository", "id": str(repository[key])})
    linear = config.get("linear") if isinstance(config.get("linear"), dict) else {}
    for team in linear.get("teams") or []:
        if isinstance(team, dict) and team.get("name"):
            out.append({"type": "team", "id": str(team["name"])})
            for member in team.get("members") or []:
                out.append({"type": "actor", "id": str(member)})
    return _dedupe_entities(out)


def _event_entities(platform: str, meta: dict[str, Any]) -> list[dict[str, Any]]:
    out = [{"type": "platform", "id": platform}]
    scalar_keys = {
        "sender": "actor",
        "author": "actor",
        "lead": "actor",
        "channel": "channel",
        "team": "team",
        "status": "status",
        "priority": "priority",
        "milestone": "milestone",
        "commit_id": "commit",
        "pr": "pull_request",
    }
    for key, entity_type in scalar_keys.items():
        value = meta.get(key)
        if value is not None and value != "":
            out.append({"type": entity_type, "id": str(value)})
    for label in meta.get("labels") or []:
        out.append({"type": "label", "id": str(label)})
    return _dedupe_entities(out)


def _dedupe_entities(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for entity in entities:
        key = (str(entity.get("type")), str(entity.get("id")))
        if key in seen or not key[1]:
            continue
        seen.add(key)
        out.append(entity)
    return out


def _supporting_observation_ids(
    answer: str | None,
    observations: list[BenchmarkObservation],
) -> tuple[list[str], float]:
    if not answer:
        return [], 0.0
    scored: list[tuple[float, str]] = []
    best = 0.0
    for observation in observations:
        support = answer_support_score(observation.content, answer)
        best = max(best, support)
        if support >= _ANSWER_SUPPORT_THRESHOLD:
            scored.append((support, observation.observation_id))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [observation_id for _, observation_id in scored], round(best, 6)


def answer_support_score(text: str, answer: str | None) -> float:
    if not answer:
        return 0.0
    normalized_text = _normalize_text(text)
    normalized_answer = _normalize_text(answer)
    if not normalized_answer:
        return 0.0
    if _contains_phrase(normalized_text, normalized_answer):
        return 1.0
    answer_tokens = _content_tokens(normalized_answer)
    if not answer_tokens:
        return 0.0
    text_tokens = set(_content_tokens(normalized_text))
    if len(answer_tokens) <= 3:
        return 1.0 if set(answer_tokens) <= text_tokens else 0.0
    return len(set(answer_tokens) & text_tokens) / len(set(answer_tokens))


def _contains_phrase(normalized_text: str, normalized_answer: str) -> bool:
    if not normalized_text or not normalized_answer:
        return False
    pattern = r"(?<![a-z0-9])" + re.escape(normalized_answer) + r"(?![a-z0-9])"
    return re.search(pattern, normalized_text) is not None


def _content_tokens(text: str) -> list[str]:
    return [
        normalized_token
        for token in _TOKEN_RE.findall(text.casefold())
        if (
            normalized_token := token.strip("._:/#-")
        ) not in _STOPWORDS and len(normalized_token) > 1
    ]


def _normalize_text(text: str) -> str:
    normalized = text.casefold().replace("_", " ").replace("-", " ")
    normalized = re.sub(r"[^a-z0-9.#:/\\s]+", " ", normalized)
    return " ".join(normalized.split())


def _question_type(question: str) -> str:
    q = question.casefold()
    if any(term in q for term in ("who", "team member", "author", "owner")):
        return "ownership"
    if any(term in q for term in ("caused", "root cause", "why", "mechanism")):
        return "causal_state_tracking"
    if any(term in q for term in ("first", "during", "after", "before", "timeline")):
        return "timeline"
    if any(term in q for term in ("specific", "exact", "hash", "name", "number")):
        return "fact_lookup"
    return "memory_qa"


def _required_tool_surfaces(question: str) -> list[str]:
    q = question.casefold()
    surfaces: list[str] = []
    if any(
        marker in q
        for marker in (
            "actual codebase",
            "actual repository",
            "clone the repository",
            "codebase structure",
            "examine the source",
            "repository code",
            "repository structure",
            "source code",
        )
    ) or any(marker in q for marker in (".py", "def statement", "function definition")):
        surfaces.append("repository")
    if any(
        marker in q
        for marker in (
            "directory",
            "exact filename",
            "file in the",
            "filesystem",
            "line number",
            "subdirectory",
            "without path",
        )
    ):
        surfaces.append("filesystem")
    if any(
        marker in q
        for marker in (
            "commit message",
            "git history",
            "git ",
            "pr was marked",
            "pull request",
        )
    ):
        surfaces.append("git")
    if any(marker in q for marker in ("linear", "ticket", "tickets")):
        surfaces.append("ticket_system")
    return sorted(set(surfaces))


__all__ = [
    "MemTrackAdapter",
    "answer_support_score",
]
