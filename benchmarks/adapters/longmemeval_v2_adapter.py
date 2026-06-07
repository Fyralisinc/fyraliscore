"""Adapter for LongMemEval-V2 dataset roots.

LongMemEval-V2 stores questions, trajectory histories, and haystack mappings
as separate files. This adapter streams only trajectories needed for selected
questions and normalizes each trajectory state into a Fyralis observation.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from benchmarks.adapters.base import (
    BenchmarkAdapter,
    BenchmarkObservation,
    BenchmarkQuery,
    GoldLabels,
)

_LABEL_TEXT_LIMIT = 40_000
_LABEL_LINE_LIMIT = 320
_LABEL_MAX_CANDIDATES = 240
_FORM_CONTROL_TEXT_LIMIT = 220_000
_QUOTED_LABEL_RE = re.compile(r"['\"]([^'\"]{2,120})['\"]")
_ROLE_LABEL_RE = re.compile(
    r"\b(?:button|link|option|menuitem|textbox|combobox|checkbox|tab|heading|StaticText)\s+([^,\n]{2,140})",
    flags=re.IGNORECASE,
)
_TITLE_LABEL_RE = re.compile(r"\b[A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){1,5}\b")
_SORT_FIELD_RE = re.compile(
    r"Order results by the following fields\.?\s+([^'\"\n|]{2,100})",
    flags=re.IGNORECASE,
)
_STAGE_STATUS_RE = re.compile(
    r"([A-Za-z][A-Za-z0-9 .'/&-]{1,80}?)\s*"
    r"\((Approved|Request Approved|Skipped|In progress|Pending - has not started|Catalog item removed)\)",
    flags=re.IGNORECASE,
)


class LongMemEvalV2Adapter(BenchmarkAdapter):
    benchmark_name = "longmemeval_v2"

    def __init__(
        self,
        data_path: Path | str,
        *,
        max_cases: int | None = None,
        haystack_tier: str = "small",
        max_accessibility_chars: int = 12_000,
    ) -> None:
        self.data_path = Path(data_path)
        self.max_cases = max_cases
        self.haystack_tier = _normalize_haystack_tier(haystack_tier)
        self.max_accessibility_chars = max(512, int(max_accessibility_chars))
        self._questions: list[dict[str, Any]] | None = None
        self._haystacks: dict[str, list[str]] = {}
        self._trajectories: dict[str, dict[str, Any]] = {}
        self._observations: list[BenchmarkObservation] = []
        self._queries: list[BenchmarkQuery] = []
        self._gold: dict[str, GoldLabels] = {}

    def load_raw(self) -> None:
        root = _dataset_root(self.data_path)
        questions_path = root / "questions.jsonl"
        trajectories_path = root / "trajectories.jsonl"
        haystack_path = root / "haystacks" / f"lme_v2_{self.haystack_tier}.json"
        for path in (questions_path, trajectories_path, haystack_path):
            if not path.exists():
                raise FileNotFoundError(f"LongMemEval-V2 file not found: {path}")

        questions: list[dict[str, Any]] = []
        with questions_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                question = json.loads(line)
                if not isinstance(question, dict):
                    continue
                questions.append(question)
                if self.max_cases is not None and len(questions) >= self.max_cases:
                    break

        haystacks_raw = json.loads(haystack_path.read_text(encoding="utf-8"))
        if not isinstance(haystacks_raw, dict):
            raise ValueError(f"Expected haystack JSON object in {haystack_path}")
        haystacks = {
            str(question_id): [str(item) for item in trajectory_ids]
            for question_id, trajectory_ids in haystacks_raw.items()
            if isinstance(trajectory_ids, list)
        }
        needed_trajectory_ids = {
            trajectory_id
            for question in questions
            for trajectory_id in haystacks.get(str(question.get("id")), [])
        }
        trajectories = _load_needed_trajectories(
            trajectories_path,
            needed_trajectory_ids,
        )
        missing = sorted(needed_trajectory_ids - trajectories.keys())
        if missing:
            preview = ", ".join(missing[:5])
            raise ValueError(
                f"Missing {len(missing)} LongMemEval-V2 trajectories: {preview}"
            )

        self._questions = questions
        self._haystacks = haystacks
        self._trajectories = trajectories

    def preprocess(self) -> None:
        if self._questions is None:
            self.load_raw()
        assert self._questions is not None

        observations_by_id: dict[str, BenchmarkObservation] = {}
        queries: list[BenchmarkQuery] = []
        gold: dict[str, GoldLabels] = {}
        haystack_tenants: dict[tuple[str, ...], str] = {}
        trajectory_observations: dict[tuple[str, int, str], list[BenchmarkObservation]] = {}

        for question in self._questions:
            question_id = str(question["id"])
            trajectory_ids = self._haystacks.get(question_id, [])
            tenant_id = haystack_tenants.setdefault(
                tuple(trajectory_ids),
                _haystack_tenant_id(self.haystack_tier, trajectory_ids),
            )

            for trajectory_order, trajectory_id in enumerate(trajectory_ids):
                trajectory = self._trajectories[trajectory_id]
                cache_key = (tenant_id, trajectory_order, trajectory_id)
                cached_observations = trajectory_observations.get(cache_key)
                if cached_observations is None:
                    cached_observations = list(
                        self._observations_for_trajectory(
                            tenant_id=tenant_id,
                            trajectory_order=trajectory_order,
                            trajectory=trajectory,
                        )
                    )
                    trajectory_observations[cache_key] = cached_observations
                for observation in cached_observations:
                    observations_by_id.setdefault(observation.observation_id, observation)

            query = BenchmarkQuery(
                query_id=question_id,
                tenant_id=tenant_id,
                query_text=str(question.get("question", "")),
                query_type=str(question.get("question_type", "memory_qa")),
                gold_answer=question.get("answer"),
                metadata={
                    "benchmark": "LongMemEval-V2",
                    "domain": question.get("domain"),
                    "environment": question.get("environment"),
                    "question_type": question.get("question_type"),
                    "image": question.get("image"),
                    "eval_function": question.get("eval_function"),
                    "haystack_tier": self.haystack_tier,
                    "haystack_trajectory_count": len(trajectory_ids),
                },
            )
            queries.append(query)
            gold[question_id] = GoldLabels(
                answer=question.get("answer"),
                evidence_ids=[],
                metadata={
                    "benchmark": "LongMemEval-V2",
                    "domain": question.get("domain"),
                    "environment": question.get("environment"),
                    "question_type": question.get("question_type"),
                    "eval_function": question.get("eval_function"),
                    "haystack_tier": self.haystack_tier,
                    "source_path": str(_dataset_root(self.data_path)),
                },
            )

        self._observations = list(observations_by_id.values())
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

    def _observations_for_trajectory(
        self,
        *,
        tenant_id: str,
        trajectory_order: int,
        trajectory: dict[str, Any],
    ) -> Iterable[BenchmarkObservation]:
        trajectory_id = str(trajectory["id"])
        states = trajectory.get("states")
        if not isinstance(states, list):
            return
        base_time = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
            days=trajectory_order
        )
        state_count = len(states)
        previous_labels: list[str] = []
        previous_state: dict[str, Any] | None = None
        previous_operational: dict[str, Any] | None = None
        for index, state in enumerate(states):
            if not isinstance(state, dict):
                continue
            state_index = _int_or_zero(state.get("state_index"))
            current_labels = _extract_ui_labels(
                str(state.get("accessibility_tree") or ""),
                focus_text=" ".join(
                    str(part or "")
                    for part in (
                        state.get("action"),
                        state.get("thought"),
                        trajectory.get("goal"),
                    )
                ),
            )
            tree = str(state.get("accessibility_tree") or "")
            operational = _operational_state_fields(
                trajectory,
                state,
                current_labels=current_labels,
                previous_labels=previous_labels,
                state_count=state_count,
                accessibility_tree=tree,
            )
            previous_labels = current_labels
            yield BenchmarkObservation(
                observation_id=_observation_id(tenant_id, trajectory_id, state_index),
                source="benchmark_longmemeval_v2",
                tenant_id=tenant_id,
                occurred_at=base_time + timedelta(seconds=state_index),
                content=self._render_state(trajectory, state, operational=operational),
                entities=_operational_entities(trajectory, state, operational),
                metadata={
                    "benchmark": "LongMemEval-V2",
                    "operational_memory_version": "lme_v2_operational_state_v3",
                    "domain": trajectory.get("domain"),
                    "environment": trajectory.get("environment"),
                    "trajectory_id": trajectory_id,
                    "trajectory_order": trajectory_order,
                    "trajectory_goal": trajectory.get("goal"),
                    "trajectory_outcome": trajectory.get("outcome"),
                    "state_index": state_index,
                    "step": state.get("step"),
                    "url": state.get("url"),
                    "url_path": operational["url_path"],
                    "workflow_phase": operational["workflow_phase"],
                    "ui_labels": operational["ui_labels"],
                    "ui_labels_added": operational["ui_labels_added"],
                    "ui_labels_removed": operational["ui_labels_removed"],
                    "sort_fields": operational["sort_fields"],
                    "form_controls": operational["form_controls"],
                    "form_control_focus": operational["form_control_focus"],
                    "structured_ui_facts": operational["structured_ui_facts"],
                    "pipeline_items": operational["pipeline_items"],
                    "stage_chains": operational["stage_chains"],
                    "screenshot": state.get("screenshot"),
                },
            )
            if previous_state is not None and previous_operational is not None:
                previous_state_index = _int_or_zero(previous_state.get("state_index"))
                yield BenchmarkObservation(
                    observation_id=_transition_observation_id(
                        tenant_id,
                        trajectory_id,
                        previous_state_index,
                        state_index,
                    ),
                    source="benchmark_longmemeval_v2_transition",
                    tenant_id=tenant_id,
                    occurred_at=(
                        base_time
                        + timedelta(seconds=previous_state_index, milliseconds=500)
                    ),
                    content=self._render_transition(
                        trajectory,
                        before_state=previous_state,
                        after_state=state,
                        before_operational=previous_operational,
                        after_operational=operational,
                    ),
                    entities=_transition_entities(
                        trajectory,
                        before_state=previous_state,
                        after_state=state,
                        before_operational=previous_operational,
                        after_operational=operational,
                    ),
                    metadata={
                        "benchmark": "LongMemEval-V2",
                        "operational_memory_version": (
                            "lme_v2_operational_transition_v3"
                        ),
                        "observation_kind": "state_transition",
                        "domain": trajectory.get("domain"),
                        "environment": trajectory.get("environment"),
                        "trajectory_id": trajectory_id,
                        "trajectory_order": trajectory_order,
                        "trajectory_goal": trajectory.get("goal"),
                        "trajectory_outcome": trajectory.get("outcome"),
                        "from_state_index": previous_state_index,
                        "to_state_index": state_index,
                        "state_index": state_index,
                        "action": state.get("action"),
                        "from_url": previous_state.get("url"),
                        "to_url": state.get("url"),
                        "from_url_path": previous_operational["url_path"],
                        "to_url_path": operational["url_path"],
                        "from_workflow_phase": previous_operational["workflow_phase"],
                        "to_workflow_phase": operational["workflow_phase"],
                        "ui_labels_added": operational["ui_labels_added"],
                        "ui_labels_removed": operational["ui_labels_removed"],
                        "sort_fields": operational["sort_fields"],
                        "form_controls": operational["form_controls"],
                        "form_control_focus": operational["form_control_focus"],
                        "structured_ui_facts": operational["structured_ui_facts"],
                        "pipeline_items": operational["pipeline_items"],
                        "stage_chains": operational["stage_chains"],
                    },
                )
            previous_state = state
            previous_operational = operational

    def _render_state(
        self,
        trajectory: dict[str, Any],
        state: dict[str, Any],
        *,
        operational: dict[str, Any],
    ) -> str:
        tree = str(state.get("accessibility_tree") or "").strip()
        if len(tree) > self.max_accessibility_chars:
            tree = f"{tree[: self.max_accessibility_chars]}\n[accessibility_tree_truncated]"
        lines = [
            "Operational memory record: web_agent_trajectory_state",
            f"Domain: {trajectory.get('domain')}",
            f"Environment: {trajectory.get('environment')}",
            f"Goal: {trajectory.get('goal')}",
            f"Outcome: {trajectory.get('outcome')}",
            f"Workflow phase: {operational['workflow_phase']}",
            f"State index: {state.get('state_index')}",
            f"URL: {state.get('url')}",
            f"URL path: {operational['url_path']}",
        ]
        action = state.get("action")
        if action:
            lines.append(f"Action: {action}")
        thought = state.get("thought")
        if thought:
            lines.append(f"Agent thought: {thought}")
        lines.append(f"State summary: {operational['state_summary']}")
        if operational["ui_labels"]:
            lines.append("Key UI labels: " + "; ".join(operational["ui_labels"][:12]))
        if operational["ui_labels_added"]:
            lines.append(
                "Newly visible UI labels: "
                + "; ".join(operational["ui_labels_added"][:12])
            )
        if operational["sort_fields"]:
            lines.append("Sort fields visible: " + "; ".join(operational["sort_fields"][:8]))
        if operational["form_controls"]:
            lines.extend(_render_form_control_lines("Form", operational))
        if operational["pipeline_items"]:
            lines.append(
                "Pipeline items visible: "
                + "; ".join(operational["pipeline_items"][:8])
            )
        if operational["stage_chains"]:
            lines.append("Pipeline stage chains: " + " | ".join(operational["stage_chains"][:3]))
        if tree:
            lines.append("Accessibility tree:")
            lines.append(tree)
        return "\n".join(lines)

    def _render_transition(
        self,
        trajectory: dict[str, Any],
        *,
        before_state: dict[str, Any],
        after_state: dict[str, Any],
        before_operational: dict[str, Any],
        after_operational: dict[str, Any],
    ) -> str:
        action = after_state.get("action") or "No explicit action recorded"
        added = after_operational["ui_labels_added"][:20]
        removed = after_operational["ui_labels_removed"][:12]
        before_labels = before_operational["ui_labels"][:12]
        after_labels = after_operational["ui_labels"][:12]
        lines = [
            "Operational memory record: web_agent_state_transition",
            f"Domain: {trajectory.get('domain')}",
            f"Environment: {trajectory.get('environment')}",
            f"Goal: {trajectory.get('goal')}",
            f"Trajectory outcome: {trajectory.get('outcome')}",
            (
                "Transition: "
                f"state {before_state.get('state_index')} -> state {after_state.get('state_index')}"
            ),
            f"Action taken: {action}",
            (
                "Route transition: "
                f"{before_operational['url_path'] or 'unknown'} -> "
                f"{after_operational['url_path'] or 'unknown'}"
            ),
            (
                "Workflow phase transition: "
                f"{before_operational['workflow_phase']} -> "
                f"{after_operational['workflow_phase']}"
            ),
        ]
        if after_state.get("thought"):
            lines.append(f"Agent transition intent: {after_state.get('thought')}")
        if added:
            lines.append("Newly visible after action: " + "; ".join(added))
        if removed:
            lines.append("No longer visible after action: " + "; ".join(removed))
        if before_labels:
            lines.append("Before state key UI labels: " + "; ".join(before_labels))
        if after_labels:
            lines.append("After state key UI labels: " + "; ".join(after_labels))
        if after_operational["sort_fields"]:
            lines.append(
                "After action sort fields visible: "
                + "; ".join(after_operational["sort_fields"][:8])
            )
        if after_operational["form_controls"]:
            lines.extend(_render_form_control_lines("After action form", after_operational))
        if after_operational["pipeline_items"]:
            lines.append(
                "After action pipeline items visible: "
                + "; ".join(after_operational["pipeline_items"][:8])
            )
        if after_operational["stage_chains"]:
            lines.append(
                "After action pipeline stage chains: "
                + " | ".join(after_operational["stage_chains"][:3])
            )
        lines.append(
            "Transition summary: "
            + _transition_summary(
                trajectory,
                before_state=before_state,
                after_state=after_state,
                before_operational=before_operational,
                after_operational=after_operational,
            )
        )
        return "\n".join(lines)


def _dataset_root(data_path: Path) -> Path:
    if data_path.is_dir():
        return data_path
    if data_path.name in {"questions.jsonl", "trajectories.jsonl"}:
        return data_path.parent
    return data_path


def _load_needed_trajectories(
    trajectories_path: Path,
    needed_ids: set[str],
) -> dict[str, dict[str, Any]]:
    trajectories: dict[str, dict[str, Any]] = {}
    if not needed_ids:
        return trajectories
    with trajectories_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if len(trajectories) >= len(needed_ids):
                break
            if not line.strip():
                continue
            trajectory = json.loads(line)
            if not isinstance(trajectory, dict):
                continue
            trajectory_id = str(trajectory.get("id"))
            if trajectory_id in needed_ids:
                trajectories[trajectory_id] = trajectory
    return trajectories


def _normalize_haystack_tier(value: str) -> str:
    normalized = value.strip().casefold().replace("lme_v2_", "")
    if normalized not in {"small", "medium"}:
        raise ValueError("haystack_tier must be 'small' or 'medium'")
    return normalized


def _haystack_tenant_id(tier: str, trajectory_ids: list[str]) -> str:
    digest = hashlib.sha1("\n".join(trajectory_ids).encode("utf-8")).hexdigest()[:12]
    return f"bench_longmemeval_v2_{tier}_{digest}"


def _observation_id(tenant_id: str, trajectory_id: str, state_index: int) -> str:
    return f"longmemeval_v2:{tenant_id}:trajectory:{trajectory_id}:state:{state_index}"


def _transition_observation_id(
    tenant_id: str,
    trajectory_id: str,
    from_state_index: int,
    to_state_index: int,
) -> str:
    return (
        f"longmemeval_v2:{tenant_id}:trajectory:{trajectory_id}:"
        f"transition:{from_state_index}->{to_state_index}"
    )


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _operational_state_fields(
    trajectory: dict[str, Any],
    state: dict[str, Any],
    *,
    current_labels: list[str],
    previous_labels: list[str],
    state_count: int,
    accessibility_tree: str,
) -> dict[str, Any]:
    current_norm = {_normalize_label(label): label for label in current_labels}
    previous_norm = {_normalize_label(label): label for label in previous_labels}
    added = [
        label
        for norm, label in current_norm.items()
        if norm and norm not in previous_norm
    ][:24]
    removed = [
        label
        for norm, label in previous_norm.items()
        if norm and norm not in current_norm
    ][:16]
    state_index = _int_or_zero(state.get("state_index"))
    phase = _workflow_phase(state_index, state_count)
    url_path = _url_path(str(state.get("url") or ""))
    summary_parts = [
        f"{phase} state for goal '{trajectory.get('goal')}'",
        f"at {url_path or 'unknown route'}",
    ]
    if state.get("action"):
        summary_parts.append(f"after action '{state.get('action')}'")
    sort_fields = _extract_sort_fields(accessibility_tree)
    form_controls = _extract_form_controls(accessibility_tree)
    form_control_focus = _form_control_focus_groups(form_controls)
    structured_ui_facts = _extract_structured_ui_facts(
        accessibility_tree,
        form_controls=form_controls,
    )
    pipeline_items = _extract_pipeline_items(accessibility_tree)
    stage_chains = _extract_stage_chains(accessibility_tree)
    if added:
        summary_parts.append("newly showing " + ", ".join(added[:8]))
    elif current_labels:
        summary_parts.append("showing " + ", ".join(current_labels[:8]))
    if sort_fields:
        summary_parts.append("sort row fields " + ", ".join(sort_fields[:4]))
    if form_controls:
        summary_parts.append("form controls " + "; ".join(form_controls[:10]))
    if pipeline_items:
        summary_parts.append("pipeline items " + ", ".join(pipeline_items[:4]))
    if stage_chains:
        summary_parts.append("pipeline stages " + " | ".join(stage_chains[:2]))
    return {
        "workflow_phase": phase,
        "url_path": url_path,
        "ui_labels": current_labels[:40],
        "ui_labels_added": added,
        "ui_labels_removed": removed,
        "sort_fields": sort_fields,
        "form_controls": form_controls,
        "form_control_focus": form_control_focus,
        "structured_ui_facts": structured_ui_facts,
        "pipeline_items": pipeline_items,
        "stage_chains": stage_chains,
        "state_summary": "; ".join(summary_parts),
    }


def _workflow_phase(state_index: int, state_count: int) -> str:
    if state_index <= 0:
        return "start"
    if state_count > 0 and state_index >= state_count - 1:
        return "final"
    if state_count > 0 and state_index >= max(1, int(state_count * 0.75)):
        return "late"
    if state_count > 0 and state_index <= max(1, int(state_count * 0.25)):
        return "early"
    return "middle"


def _url_path(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return parsed.netloc or url[:120]
    return path[:160]


def _extract_ui_labels(tree: str, *, focus_text: str = "") -> list[str]:
    candidates: list[tuple[int, int, str]] = []
    seen: set[str] = set()
    clipped = tree[:_LABEL_TEXT_LIMIT]
    focus_terms = _important_terms(focus_text)
    for line_index, line in enumerate(clipped.splitlines()):
        sample = line[:_LABEL_LINE_LIMIT]
        for match in _QUOTED_LABEL_RE.finditer(sample):
            _append_label(candidates, seen, match.group(1), focus_terms, line_index)
        for match in _ROLE_LABEL_RE.finditer(sample):
            _append_label(candidates, seen, match.group(1), focus_terms, line_index)
        for match in _TITLE_LABEL_RE.finditer(sample):
            _append_label(candidates, seen, match.group(0), focus_terms, line_index)
    return _rank_labels(candidates)


def _extract_sort_fields(tree: str) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()
    for match in _SORT_FIELD_RE.finditer(tree[:_LABEL_TEXT_LIMIT]):
        field = _clean_sort_field(match.group(1))
        norm = _normalize_label(field)
        if field and norm and norm not in seen:
            seen.add(norm)
            fields.append(field)
        if len(fields) >= 8:
            break
    return fields


def _clean_sort_field(value: str) -> str:
    field = re.sub(r"\s+", " ", value).strip(" .,:;")
    field = re.split(
        r"\s+(?:a to z|z to a|ascending|descending|Remove condition|New sort order|Add Sort)\b",
        field,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip(" .,:;")
    words = field.split()
    if len(words) >= 2 and len(words) % 2 == 0:
        half = len(words) // 2
        if [word.casefold() for word in words[:half]] == [
            word.casefold() for word in words[half:]
        ]:
            field = " ".join(words[:half])
    return field[:80]


def _extract_form_controls(tree: str) -> list[str]:
    controls: list[tuple[int, int, str]] = []
    seen: set[str] = set()
    limited = tree[:_FORM_CONTROL_TEXT_LIMIT]
    patterns = (
        (
            re.compile(
                r"\b(checkbox|radio)\s+'([^']+)'.{0,180}?\bchecked='?(true|false)'?",
                re.IGNORECASE,
            ),
            "checked",
        ),
        (
            re.compile(
                r"\boption\s+'([^']+)'.{0,120}?\bselected=(True|False|true|false)",
                re.IGNORECASE,
            ),
            "selected",
        ),
    )
    order = 0
    for pattern, state_name in patterns:
        for match in pattern.finditer(limited):
            if state_name == "checked":
                kind = match.group(1).casefold()
                label = _clean_form_control_label(match.group(2))
                value = match.group(3).casefold()
            else:
                kind = "option"
                label = _clean_form_control_label(match.group(1))
                value = match.group(2).casefold()
            if not label:
                continue
            summary = f"{kind} {label} {state_name}={value}"
            norm = _normalize_label(summary)
            if norm and norm not in seen:
                seen.add(norm)
                controls.append((_form_control_priority(summary), order, summary))
                order += 1
            if len(controls) >= 400:
                break
    return _rank_form_controls(controls, limit=140)


def _rank_form_controls(
    controls: list[tuple[int, int, str]],
    *,
    limit: int,
) -> list[str]:
    controls.sort(key=lambda item: (-item[0], item[1]))
    return [summary for _score, _order, summary in controls[:limit]]


def _form_control_priority(summary: str) -> int:
    lower = summary.casefold()
    score = 0
    high_signal_terms = (
        "depreciat",
        "compact rows",
        "active row",
        "wrap column",
        "software",
        "eclipse",
        "adobe",
        "photoshop",
        "acrobat",
        "ubuntu",
        "windows",
        "250 gb",
        "500 gb",
        "solid state",
        "business phone",
        "fulfillment automation",
        "warranty expiration",
        "access type",
    )
    for term in high_signal_terms:
        if term in lower:
            score += 110
            break

    if "[add $" in lower or "[subtract $" in lower:
        score += 95
    if "checked=true" in lower or "selected=true" in lower:
        score += 42
    if "checked=false" in lower or "selected=false" in lower:
        score += 34
    if lower.startswith(("radio ", "checkbox ")):
        score += 18
    if lower.startswith("option "):
        score += 8

    noisy_terms = (
        "select record for action",
        "select all",
        "assign tag",
        "remove tag",
        "actions on selected rows",
        "preview selected",
    )
    if any(term in lower for term in noisy_terms):
        score -= 90
    label = re.sub(
        r"^(?:checkbox|radio|option)\s+",
        "",
        summary,
        flags=re.IGNORECASE,
    )
    label = re.sub(
        r"\s+(?:checked|selected)=(?:true|false).*$",
        "",
        label,
        flags=re.IGNORECASE,
    ).strip()
    if re.fullmatch(r"\d{1,4}", label) or re.fullmatch(r"[a-z]?\d{1,4}", label.casefold()):
        score -= 70
    return score


def _form_control_focus_groups(controls: list[str]) -> dict[str, list[str]]:
    unchecked: list[str] = []
    selected: list[str] = []
    price_controls: list[str] = []
    field_controls: list[str] = []
    for control in controls:
        lower = control.casefold()
        if "checked=false" in lower or "selected=false" in lower:
            unchecked.append(control)
        if "checked=true" in lower or "selected=true" in lower:
            selected.append(control)
        if "[add $" in lower or "[subtract $" in lower:
            price_controls.append(control)
        if any(
            term in lower
            for term in (
                "depreciat",
                "compact rows",
                "active row",
                "wrap column",
                "software",
                "business phone",
                "fulfillment automation",
                "warranty expiration",
                "access type",
            )
        ):
            field_controls.append(control)
    return {
        "unchecked_or_unselected": unchecked[:36],
        "selected_or_checked": selected[:36],
        "price_delta_controls": price_controls[:24],
        "field_configuration_controls": field_controls[:36],
    }


def _render_form_control_lines(prefix: str, operational: dict[str, Any]) -> list[str]:
    controls = operational.get("form_controls", [])
    focus = operational.get("form_control_focus") or {}
    lines = [f"{prefix} controls visible: " + "; ".join(controls[:28])]
    field_controls = focus.get("field_configuration_controls") or []
    if field_controls:
        lines.append(
            f"{prefix} field/configuration controls visible: "
            + "; ".join(field_controls[:20])
        )
    price_controls = focus.get("price_delta_controls") or []
    if price_controls:
        lines.append(
            f"{prefix} price option controls visible: "
            + "; ".join(price_controls[:16])
        )
    selected = focus.get("selected_or_checked") or []
    if selected:
        lines.append(
            f"{prefix} selected/checked controls visible: "
            + "; ".join(selected[:18])
        )
    unchecked = focus.get("unchecked_or_unselected") or []
    if unchecked:
        lines.append(
            f"{prefix} unchecked/unselected controls visible: "
            + "; ".join(unchecked[:18])
        )
    return lines


def _clean_form_control_label(value: str) -> str:
    label = re.sub(r"\s+", " ", value).strip(" .,:;")
    if not label or label in {"true", "false"}:
        return ""
    if len(label) > 160:
        label = label[:160].rstrip()
    return label


def _extract_structured_ui_facts(
    tree: str,
    *,
    form_controls: list[str],
) -> list[str]:
    facts: list[str] = []
    seen: set[str] = set()
    for fact in (
        _extract_field_list_facts(tree)
        + _extract_table_value_facts(tree)
        + _extract_editable_form_field_facts(tree)
        + _extract_autocomplete_popup_facts(tree)
        + _extract_checkbox_choice_facts(form_controls)
    ):
        norm = _normalize_label(fact)
        if fact and norm and norm not in seen:
            seen.add(norm)
            facts.append(fact)
        if len(facts) >= 40:
            break
    return facts


def _extract_field_list_facts(tree: str) -> list[str]:
    facts: list[str] = []
    pattern = re.compile(
        r"listbox\s+'Search a specific field of the ([^']+) list,\s*\d+ items'"
        r"[^\n]*\n(?P<body>(?:\s+\[[^\n]+\]\s+option\s+'[^']+'[^\n]*\n?)+)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(tree[:_FORM_CONTROL_TEXT_LIMIT]):
        list_name = _clean_structured_fact_value(match.group(1), max_len=80)
        options = [
            _clean_structured_fact_value(option, max_len=80)
            for option in re.findall(r"\boption\s+'([^']+)'", match.group("body"), re.IGNORECASE)
        ]
        options = [option for option in options if option]
        if list_name and options:
            facts.append(
                f"field list {list_name} option order: "
                + "; ".join(options[:18])
                + f"; bottom_option={options[-1]}"
            )
        if len(facts) >= 8:
            break
    return facts


def _extract_table_value_facts(tree: str) -> list[str]:
    facts: list[str] = []
    lines = tree[:_FORM_CONTROL_TEXT_LIMIT].splitlines()
    for index, line in enumerate(lines):
        for label in re.findall(r"\bgridcell\s+'([^']*)'", line, re.IGNORECASE):
            clean_label = _clean_structured_fact_value(label, max_len=80)
            if not _is_total_like_label(clean_label):
                continue
            for next_line in lines[index + 1 : index + 8]:
                values = re.findall(r"\bgridcell\s+'([^']*)'", next_line, re.IGNORECASE)
                if not values:
                    continue
                value = _clean_structured_fact_value(values[0], max_len=120)
                facts.append(f"table summary row {clean_label}: value={value or '(empty)'}")
                break
        if len(facts) >= 10:
            break
    return facts


def _is_total_like_label(label: str) -> bool:
    folded = label.casefold()
    return folded in {"total", "subtotal", "price total", "order total"} or folded.endswith(" total")


def _extract_editable_form_field_facts(tree: str) -> list[str]:
    editable: list[str] = []
    required: list[str] = []
    disabled: list[str] = []
    seen: set[str] = set()
    pattern = re.compile(
        r"\b(textbox|searchbox|combobox|checkbox)\s+'([^']+)'.{0,180}",
        re.IGNORECASE,
    )
    for match in pattern.finditer(tree[:_FORM_CONTROL_TEXT_LIMIT]):
        raw = match.group(0)
        label = _clean_form_field_label(match.group(2))
        norm = _normalize_label(label)
        if not label or not norm or norm in seen:
            continue
        seen.add(norm)
        value_match = re.search(r"\bvalue='([^']*)'", raw, re.IGNORECASE)
        value = _clean_structured_fact_value(value_match.group(1), max_len=80) if value_match else ""
        item = f"{label}={value}" if value else label
        folded_raw = raw.casefold()
        if "disabled=true" in folded_raw or "read only" in folded_raw:
            disabled.append(item)
        else:
            editable.append(item)
            if "required" in folded_raw or "mandatory" in folded_raw:
                required.append(item)
        if len(editable) + len(disabled) >= 60:
            break

    facts: list[str] = []
    if required:
        facts.append("required editable form fields: " + "; ".join(required[:14]))
    if editable:
        facts.append("editable form fields: " + "; ".join(editable[:18]))
    if disabled:
        facts.append("disabled/read-only form fields: " + "; ".join(disabled[:14]))
    return facts


def _clean_form_field_label(value: str) -> str:
    label = _clean_structured_fact_value(value, max_len=120)
    label = re.sub(
        r"^(?:mandatory - must be populated before submit|read only - cannot be modified|link opens in new window)\s+",
        "",
        label,
        flags=re.IGNORECASE,
    ).strip()
    label = re.sub(r"^[^\w]*(?:Owner|Assigned to|Caller)\b", lambda m: m.group(0).strip(), label)
    return label.strip(" .,:;")


def _extract_autocomplete_popup_facts(tree: str) -> list[str]:
    facts: list[str] = []
    lines = tree[:_FORM_CONTROL_TEXT_LIMIT].splitlines()
    for index, line in enumerate(lines):
        if not re.search(r"\blistbox\s+''", line, re.IGNORECASE):
            continue
        body = "\n".join(lines[index + 1 : index + 18])
        title_match = re.search(
            r"(?:StaticText|LayoutTableCell)\s+'([^']+)'",
            body,
            re.IGNORECASE,
        )
        if not title_match:
            continue
        title = _clean_structured_fact_value(title_match.group(1), max_len=80)
        if not title:
            continue
        field = _autocomplete_field_context(lines[max(0, index - 24) : index])
        options = [
            _clean_structured_fact_value(option, max_len=80)
            for option in re.findall(r"\boption\s+'([^']+)'", body, re.IGNORECASE)
        ]
        options = [option for option in options if option]
        facts.append(
            f"autocomplete popup title: {title}"
            + (f"; field={field}" if field else "")
            + (("; options: " + "; ".join(options[:8])) if options else "")
        )
        if len(facts) >= 8:
            break
    return facts


def _autocomplete_field_context(lines: list[str]) -> str:
    for line in reversed(lines):
        match = re.search(
            r"\b(?:searchbox|textbox|combobox)\s+'([^']+)'",
            line,
            re.IGNORECASE,
        )
        if not match:
            continue
        label = _clean_form_field_label(match.group(1))
        if label and label.casefold() not in {"search", "choose search context"}:
            return label
    return ""


def _extract_checkbox_choice_facts(form_controls: list[str]) -> list[str]:
    choices: list[str] = []
    for control in form_controls:
        lower = control.casefold()
        if not lower.startswith("checkbox "):
            continue
        if any(
            noise in lower
            for noise in (
                "active checked",
                "password needs reset",
                "locked out",
                "web service access only",
                "internal integration user",
                "inherited",
                "select record for action",
                "select all",
                "active row highlighting",
                "compact rows",
                "wrap column text",
                "modern cell coloring",
                "enable list edit",
                "double click to edit",
            )
        ):
            continue
        label = re.sub(r"^checkbox\s+", "", control, flags=re.IGNORECASE)
        label = re.sub(r"\s+checked=(?:true|false)$", "", label, flags=re.IGNORECASE)
        label = _clean_structured_fact_value(label, max_len=80)
        if label:
            choices.append(label)
    if not choices:
        return []
    configuration_markers = (
        "adobe",
        "acrobat",
        "photoshop",
        "eclipse",
        "software",
        "license",
        "access",
        "option",
    )
    if not any(
        marker in choice.casefold()
        for marker in configuration_markers
        for choice in choices
    ):
        return []
    return [
        "checkbox choice group visible: "
        f"count={len(choices)}; choices="
        + "; ".join(choices[:24])
    ]


def _clean_structured_fact_value(value: str, *, max_len: int) -> str:
    cleaned = re.sub(r"\s+", " ", value).strip(" .,:;")
    if cleaned.casefold() in {"true", "false", "visible", "clickable"}:
        return ""
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip()
    return cleaned


def _extract_pipeline_items(tree: str) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    lines = tree[:_LABEL_TEXT_LIMIT].splitlines()
    for index, line in enumerate(lines):
        if "stage state display" not in line.casefold():
            continue
        window = lines[max(0, index - 8) : index]
        for candidate_line in reversed(window):
            candidates = re.findall(
                r"(?:gridcell|link|heading)\s+'([^']+)'",
                candidate_line,
                flags=re.IGNORECASE,
            )
            for candidate in candidates:
                item = _clean_pipeline_item(candidate)
                norm = _normalize_label(item)
                if item and norm and norm not in seen:
                    seen.add(norm)
                    items.append(item)
                    break
            if items and _normalize_label(items[-1]) in seen:
                break
        if len(items) >= 8:
            break
    return items


def _clean_pipeline_item(value: str) -> str:
    item = re.sub(r"\s+", " ", value).strip(" .,:;")
    folded = item.casefold()
    if not item or len(item) < 3:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", item):
        return ""
    if re.fullmatch(r"[$€£]?\d+(?:[.,]\d+)?", item):
        return ""
    if any(
        marker in folded
        for marker in (
            "stage state display",
            "pending - has not started",
            "in progress",
            "request approved",
            "waiting for approval",
            "description",
            "delivery date",
            "price",
            "quantity",
            "total",
        )
    ):
        return ""
    return item[:100]


def _extract_stage_chains(tree: str) -> list[str]:
    chains: list[str] = []
    seen: set[str] = set()
    for line in tree[:_LABEL_TEXT_LIMIT].splitlines():
        if "stage state display" not in line.casefold():
            continue
        stages = [
            (_clean_stage_name(name), _clean_stage_status(status))
            for name, status in _STAGE_STATUS_RE.findall(line)
        ]
        stages = [(name, status) for name, status in stages if name and status]
        if len(stages) < 2:
            continue
        summary = "; ".join(f"{name} ({status})" for name, status in stages[:12])
        pending_count = sum(
            1 for _name, status in stages if status.casefold() == "pending - has not started"
        )
        in_progress_count = sum(
            1 for _name, status in stages if status.casefold() == "in progress"
        )
        remaining_excluding_in_progress_count = sum(
            1
            for _name, status in stages
            if status.casefold() != "in progress"
        )
        if pending_count or in_progress_count:
            summary += (
                f"; pending_not_started_count={pending_count}; "
                f"in_progress_count={in_progress_count}; "
                "remaining_excluding_in_progress_count="
                f"{remaining_excluding_in_progress_count}"
            )
        norm = _normalize_label(summary)
        if norm and norm not in seen:
            seen.add(norm)
            chains.append(summary[:700])
        if len(chains) >= 6:
            break
    return chains


def _clean_stage_name(value: str) -> str:
    name = re.sub(r"\s+", " ", value).strip(" .,:;")
    name = re.sub(r"^.*Toggle stage state display\s+", "", name, flags=re.IGNORECASE)
    return name[-90:].strip(" .,:;")


def _clean_stage_status(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip(" .,:;")


def _append_label(
    candidates: list[tuple[int, int, str]],
    seen: set[str],
    value: str,
    focus_terms: set[str],
    line_index: int,
) -> None:
    label = re.sub(r"\s+", " ", value).strip(" .,:;")
    if not label or len(label) < 2:
        return
    folded = label.casefold()
    if folded in {
        "none",
        "true",
        "false",
        "all",
        "assertive",
        "polite",
        "clickable",
        "visible",
        "additions text",
        "statictext",
    }:
        return
    noise_fragments = (
        "[",
        "clickable",
        "visible",
        "haspopup",
        "expanded=",
        "autocomplete=",
        "live=",
        "relevant=",
        "selected=",
        "checked=",
        "disabled=",
    )
    if any(fragment in folded for fragment in noise_fragments):
        return
    norm = _normalize_label(label)
    if norm and norm not in seen:
        seen.add(norm)
        score = _label_priority(label, focus_terms)
        item = (score, line_index, label[:120])
        if len(candidates) < _LABEL_MAX_CANDIDATES:
            candidates.append(item)
            return
        worst_index, worst_item = min(
            enumerate(candidates),
            key=lambda pair: (pair[1][0], -pair[1][1], pair[1][2].casefold()),
        )
        if (score, -line_index, label.casefold()) > (
            worst_item[0],
            -worst_item[1],
            worst_item[2].casefold(),
        ):
            candidates[worst_index] = item


def _rank_labels(candidates: list[tuple[int, int, str]]) -> list[str]:
    ranked = sorted(candidates, key=lambda item: (-item[0], item[1], item[2].casefold()))
    return [label for _score, _line_index, label in ranked[:60]]


def _label_priority(label: str, focus_terms: set[str]) -> int:
    label_terms = _important_terms(label)
    score = 0
    if label_terms & focus_terms:
        score += 100 + (8 * len(label_terms & focus_terms))
    folded = label.casefold()
    if any(word in folded for word in ("option", "filter", "dropdown", "choice", "selected")):
        score += 20
    if any(word in folded for word in ("skip to", "accessibility", "unpinned", "servicenow")):
        score -= 30
    if len(label) <= 80:
        score += 5
    return score


def _important_terms(text: str) -> set[str]:
    stop = {
        "action",
        "after",
        "before",
        "click",
        "field",
        "from",
        "list",
        "menu",
        "open",
        "page",
        "portal",
        "service",
        "servicenow",
        "state",
        "task",
        "the",
        "this",
        "with",
    }
    terms: set[str] = set()
    for token in re.findall(r"[a-z0-9]+", text.casefold()):
        if len(token) < 4 or token in stop:
            continue
        terms.add(token)
        if token.endswith("s") and len(token) > 4:
            terms.add(token[:-1])
    return terms


def _normalize_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", label.casefold()).strip()


def _operational_entities(
    trajectory: dict[str, Any],
    state: dict[str, Any],
    operational: dict[str, Any],
) -> list[dict[str, str]]:
    entities: list[dict[str, str]] = []
    _append_entity(entities, "environment", str(trajectory.get("environment") or ""))
    _append_entity(entities, "workflow", str(trajectory.get("goal") or ""))
    _append_entity(entities, "trajectory", str(trajectory.get("id") or ""))
    _append_entity(entities, "route", str(operational.get("url_path") or ""))
    _append_entity(entities, "phase", str(operational.get("workflow_phase") or ""))
    if state.get("action"):
        _append_entity(entities, "action", str(state.get("action")))
    for label in operational.get("ui_labels", [])[:16]:
        _append_entity(entities, "ui_label", str(label))
    for label in operational.get("ui_labels_added", [])[:10]:
        _append_entity(entities, "ui_label_added", str(label))
    for field in operational.get("sort_fields", [])[:6]:
        _append_entity(entities, "sort_field", str(field))
    for control in operational.get("form_controls", [])[:12]:
        _append_entity(entities, "form_control", str(control))
    for controls in (operational.get("form_control_focus") or {}).values():
        for control in controls[:8]:
            _append_entity(entities, "form_control_focus", str(control))
    for fact in operational.get("structured_ui_facts", [])[:12]:
        _append_entity(entities, "structured_ui_fact", str(fact))
    for item in operational.get("pipeline_items", [])[:6]:
        _append_entity(entities, "pipeline_item", str(item))
    for chain in operational.get("stage_chains", [])[:4]:
        _append_entity(entities, "pipeline_stage_chain", str(chain))
    return entities


def _transition_entities(
    trajectory: dict[str, Any],
    *,
    before_state: dict[str, Any],
    after_state: dict[str, Any],
    before_operational: dict[str, Any],
    after_operational: dict[str, Any],
) -> list[dict[str, str]]:
    entities: list[dict[str, str]] = []
    _append_entity(entities, "environment", str(trajectory.get("environment") or ""))
    _append_entity(entities, "workflow", str(trajectory.get("goal") or ""))
    _append_entity(entities, "trajectory", str(trajectory.get("id") or ""))
    _append_entity(entities, "transition", _transition_entity_id(before_state, after_state))
    _append_entity(entities, "route_from", str(before_operational.get("url_path") or ""))
    _append_entity(entities, "route_to", str(after_operational.get("url_path") or ""))
    _append_entity(entities, "phase_from", str(before_operational.get("workflow_phase") or ""))
    _append_entity(entities, "phase_to", str(after_operational.get("workflow_phase") or ""))
    if after_state.get("action"):
        _append_entity(entities, "action", str(after_state.get("action")))
    for label in after_operational.get("ui_labels_added", [])[:16]:
        _append_entity(entities, "ui_label_added", str(label))
    for label in after_operational.get("ui_labels_removed", [])[:8]:
        _append_entity(entities, "ui_label_removed", str(label))
    for field in after_operational.get("sort_fields", [])[:6]:
        _append_entity(entities, "sort_field", str(field))
    for control in after_operational.get("form_controls", [])[:16]:
        _append_entity(entities, "form_control", str(control))
    for controls in (after_operational.get("form_control_focus") or {}).values():
        for control in controls[:10]:
            _append_entity(entities, "form_control_focus", str(control))
    for fact in after_operational.get("structured_ui_facts", [])[:16]:
        _append_entity(entities, "structured_ui_fact", str(fact))
    for item in after_operational.get("pipeline_items", [])[:6]:
        _append_entity(entities, "pipeline_item", str(item))
    for chain in after_operational.get("stage_chains", [])[:4]:
        _append_entity(entities, "pipeline_stage_chain", str(chain))
    return entities


def _transition_entity_id(
    before_state: dict[str, Any],
    after_state: dict[str, Any],
) -> str:
    return f"{before_state.get('state_index')} to {after_state.get('state_index')}"


def _transition_summary(
    trajectory: dict[str, Any],
    *,
    before_state: dict[str, Any],
    after_state: dict[str, Any],
    before_operational: dict[str, Any],
    after_operational: dict[str, Any],
) -> str:
    parts = [
        (
            f"action '{after_state.get('action')}' moved the trajectory "
            f"from state {before_state.get('state_index')} to state {after_state.get('state_index')}"
        ),
        f"for goal '{trajectory.get('goal')}'",
    ]
    if before_operational["url_path"] != after_operational["url_path"]:
        parts.append(
            "route changed from "
            f"{before_operational['url_path'] or 'unknown'} to "
            f"{after_operational['url_path'] or 'unknown'}"
        )
    added = after_operational.get("ui_labels_added", [])
    removed = after_operational.get("ui_labels_removed", [])
    if added:
        parts.append("newly visible labels include " + ", ".join(added[:8]))
    if removed:
        parts.append("removed labels include " + ", ".join(removed[:6]))
    return "; ".join(parts)


def _append_entity(entities: list[dict[str, str]], entity_type: str, raw_id: str) -> None:
    normalized = _normalize_label(raw_id)
    if not normalized:
        return
    item = {"type": entity_type, "id": normalized[:160]}
    if item not in entities:
        entities.append(item)
