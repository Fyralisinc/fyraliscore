#!/usr/bin/env python3
"""Observe sealed Slack gold through the existing context-selection surface."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.contracts.perception import SufficiencyDisposition
from lib.evaluation.slack_reconstruction_gold import (
    SlackReconstructionGoldCase,
    SlackReconstructionObservation,
    SlackRevisionFate,
    evaluate_slack_reconstruction,
    load_slack_reconstruction_gold,
)
from lib.shared.errors import ValidationError
from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    prepare_context_selection,
)
from services.ingest.ingestion.handlers import ObservationDraft
from services.ingest.ingestion.handlers.slack import handle_slack_message


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD = (
    ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "slack_reconstruction_gold_v1.jsonl"
)


async def observe_existing_slack_reconstruction(
    cases: tuple[SlackReconstructionGoldCase, ...],
) -> tuple[SlackReconstructionObservation, ...]:
    observations = []
    for case in cases:
        observations.append(await _observe_case(case))
    return tuple(observations)


async def _observe_case(
    case: SlackReconstructionGoldCase,
) -> SlackReconstructionObservation:
    drafts: dict[str, ObservationDraft] = {}
    revision_fates: dict[str, SlackRevisionFate] = {}
    unsupported: list[str] = []
    for event in case.events:
        try:
            draft = await handle_slack_message(event.payload, {})
        except ValidationError as exc:
            revision_fates[event.event_revision_id] = SlackRevisionFate.UNSUPPORTED
            unsupported.append(
                f"{case.family.value}:handler:"
                f"{getattr(exc, 'code', type(exc).__name__)}"
            )
            continue
        drafts[event.event_revision_id] = draft
        revision_fates[event.event_revision_id] = SlackRevisionFate.CURRENT

    focal = drafts.get(case.focal_event_revision_id)
    if focal is None:
        unsupported.append("focal_event_not_supported_by_slack_handler")
        return SlackReconstructionObservation(
            case_id=case.case_id,
            candidate_event_revision_ids=(),
            selected_event_revision_ids=(),
            selected_topology_edge_ids=(),
            revision_fates=revision_fates,
            disposition=SufficiencyDisposition.NON_IDENTIFIABLE,
            selected_token_count=0,
            unsupported_reasons=tuple(dict.fromkeys(unsupported)),
            artifact_refs=(
                f"existing-surface://slack-handler/{case.case_id}",
            ),
        )

    structural, temporal = _current_context_inputs(
        case=case,
        drafts=drafts,
    )
    context_inputs = (*structural, *temporal)
    boundary_hypotheses = []
    if structural:
        boundary_hypotheses.append(
            {
                "kind": "source_topology",
                "candidate_count": len(structural),
                "limits": "current same-channel thread/edit topology",
            }
        )
    boundary_hypotheses.append(
        {
            "kind": "same_source_space_temporal",
            "candidate_count": len(temporal),
            "limits": "current same-channel temporal alternatives",
        }
    )
    focal_observation_id = _observation_id(case.focal_event_revision_id)
    command, outcome = prepare_context_selection(
        tenant_id=_TENANT_ID,
        observation_id=focal_observation_id,
        phrase=case.phrase,
        occurred_at=focal.occurred_at,
        source_channel=focal.source_channel,
        source_space=str(focal.content.get("channel") or focal.source_channel),
        topology_incomplete=not isinstance(focal.content.get("channel"), str),
        boundary_hypotheses=tuple(boundary_hypotheses),
        context_observations=context_inputs,
        selection_dependency_refs=tuple(
            f"{item.observation_id}@{item.occurred_at.isoformat()}"
            for item in context_inputs
        ),
        now=focal.occurred_at + timedelta(seconds=1),
    )
    candidates = tuple(
        dict.fromkeys(
            item.event_revision_id
            for candidate in command.candidates
            for item in candidate.selected_items
        )
    )
    selected = tuple(
        item.event_revision_id
        for item in outcome.snapshot.selected_items
    )
    selected_topology = outcome.snapshot.topology_edge_ids
    if case.required_topology_edge_ids and not selected_topology:
        unsupported.append("selected_topology_edges_not_materialized")
    for event_id, expected in case.expected_revision_fates.items():
        if (
            expected is SlackRevisionFate.SUPERSEDED
            and revision_fates.get(event_id) is SlackRevisionFate.CURRENT
        ):
            unsupported.append("edit_supersession_fate_not_materialized")
    excluded_required = {
        event_id
        for sufficient_set in case.acceptable_sufficient_sets
        for event_id in sufficient_set
        if event_id not in candidates
    }
    if excluded_required:
        unsupported.append(
            "required_context_outside_current_same_channel_candidate_lane"
        )
    token_counts = case.token_counts
    return SlackReconstructionObservation(
        case_id=case.case_id,
        candidate_event_revision_ids=candidates,
        selected_event_revision_ids=selected,
        selected_topology_edge_ids=selected_topology,
        revision_fates=revision_fates,
        disposition=outcome.disposition,
        selected_token_count=sum(token_counts.get(event_id, 0) for event_id in selected),
        unsupported_reasons=tuple(dict.fromkeys(unsupported)),
        artifact_refs=(
            f"existing-surface://prepare-context-selection/{case.case_id}",
            f"context-snapshot:{outcome.snapshot.snapshot_id}",
        ),
    )


def _current_context_inputs(
    *,
    case: SlackReconstructionGoldCase,
    drafts: dict[str, ObservationDraft],
) -> tuple[tuple[ContextObservationInput, ...], tuple[ContextObservationInput, ...]]:
    focal = drafts[case.focal_event_revision_id]
    focal_channel = str(focal.content.get("channel") or "")
    focal_root = str(
        focal.content.get("thread_ts")
        or focal.content.get("original_ts")
        or focal.content.get("ts")
        or ""
    )
    focal_ts = str(focal.content.get("ts") or "")
    structural: list[ContextObservationInput] = []
    temporal: list[ContextObservationInput] = []
    structural_ids = set()
    ordered = sorted(
        (
            (event_id, draft)
            for event_id, draft in drafts.items()
            if event_id != case.focal_event_revision_id
            and draft.occurred_at <= focal.occurred_at
            and str(draft.content.get("channel") or "") == focal_channel
        ),
        key=lambda item: (item[1].occurred_at, item[0]),
    )
    for event_id, draft in ordered:
        content = draft.content
        candidate_ts = str(content.get("ts") or "")
        candidate_thread = str(content.get("thread_ts") or "")
        candidate_original = str(content.get("original_ts") or "")
        topological = bool(
            focal_root
            and (
                candidate_ts == focal_root
                or candidate_thread == focal_root
                or candidate_original == focal_root
                or (focal_ts and candidate_original == focal_ts)
            )
        )
        if not topological:
            continue
        structural_ids.add(event_id)
        structural.append(
            _context_input(
                event_id=event_id,
                draft=draft,
                inclusion_layer="source_topology",
                reasons=("same Slack channel", "thread/reply/edit lineage"),
            )
        )
    for event_id, draft in reversed(ordered):
        if event_id in structural_ids:
            continue
        temporal.append(
            _context_input(
                event_id=event_id,
                draft=draft,
                inclusion_layer="temporal_candidate",
                reasons=("same exact source space", "as-known cutoff"),
            )
        )
    return tuple(structural), tuple(temporal)


def _context_input(
    *,
    event_id: str,
    draft: ObservationDraft,
    inclusion_layer: str,
    reasons: tuple[str, ...],
) -> ContextObservationInput:
    return ContextObservationInput(
        observation_id=_observation_id(event_id),
        occurred_at=draft.occurred_at,
        source_channel=draft.source_channel,
        source_space=str(draft.content.get("channel") or draft.source_channel),
        inclusion_layer=inclusion_layer,
        inclusion_reasons=reasons,
    )


def _observation_id(event_revision_id: str) -> UUID:
    prefix = "observation:"
    suffix = ":v1"
    if not event_revision_id.startswith(prefix) or not event_revision_id.endswith(
        suffix
    ):
        raise ValueError(
            "existing-surface observer requires observation:<uuid>:v1 revisions"
        )
    return UUID(event_revision_id[len(prefix) : -len(suffix)])


async def _run(args: argparse.Namespace) -> int:
    cases = load_slack_reconstruction_gold(args.gold)
    observations = await observe_existing_slack_reconstruction(cases)
    report = evaluate_slack_reconstruction(
        cases=cases,
        observations=observations,
        run_id=args.run_id,
        system_version=args.system_version,
        artifact_refs=(
            f"gold:{args.gold.resolve()}",
            "observer:scripts/observe_slack_reconstruction_gold.py",
        ),
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    observations_path = (
        args.output_dir / "slack_reconstruction_observations.jsonl"
    )
    observations_path.write_text(
        "".join(
            json.dumps(observation.model_dump(mode="json"), sort_keys=True)
            + "\n"
            for observation in observations
        ),
        encoding="utf-8",
    )
    report_path = (
        args.output_dir / "slack_reconstruction_existing_surface_report.json"
    )
    report_path.write_text(
        json.dumps(
            {
                "report": report.model_dump(mode="json"),
                "report_digest": report.digest,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"observations={observations_path}")
    print(f"report={report_path}")
    print(
        "status={status} correct_rate={correct} recall={recall} "
        "contamination={contamination} abstention={abstention}".format(
            status=report.status,
            correct=report.metrics.correct_case_rate,
            recall=report.metrics.mean_sufficient_set_recall,
            contamination=report.metrics.contamination_rate,
            abstention=report.metrics.abstention_under_insufficiency_rate,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_run(_parse_args(argv)))


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Measure sealed Slack reconstruction gold through the current "
            "Slack handler and pure context-selection surface."
        )
    )
    parser.add_argument("--gold", type=Path, default=DEFAULT_GOLD)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / f"slack-existing-surface-{timestamp}",
    )
    parser.add_argument(
        "--run-id",
        default=f"slack-existing-surface-{timestamp}",
    )
    parser.add_argument("--system-version", default="local-working-tree")
    return parser.parse_args(argv)


_TENANT_ID = UUID("55555555-5555-4555-8555-555555555555")


if __name__ == "__main__":
    raise SystemExit(main())
