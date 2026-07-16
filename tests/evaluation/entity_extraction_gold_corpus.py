"""Sealed, transport-independent gold corpus for company-entity extraction.

The corpus is intentionally synthetic: annotations are fixed before predictions
are produced and cover source shapes that can be exercised without connectors or
an LLM.  A scenario is repeated across four batches to test duplicate evidence
without changing its canonical identity.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from lib.entity_mention_detection import locate_explicit_surface_spans
from lib.evaluation.entity_extraction_gold import (
    GoldMention,
    GoldSignal,
    PredictedMention,
    evaluate_gold_entity_extraction,
)
from services.domain.entity_grounding.mention_fates import _persisted_mention_opportunities


@dataclass(frozen=True)
class Annotation:
    surface: str
    entity_type: str
    referent: str
    occurrence: int = 0


@dataclass(frozen=True)
class Scenario:
    source: str
    text: str
    annotations: tuple[Annotation, ...]
    phenomena: tuple[str, ...]
    slack_context: str = "not_slack"


def _a(surface: str, entity_type: str, referent: str, occurrence: int = 0) -> Annotation:
    return Annotation(surface, entity_type, referent, occurrence)


# These annotations, rather than extractor output, are the source of truth.
_SCENARIOS = (
    Scenario("jira", "Atlas Migration is blocked by IAM Gateway.", (_a("Atlas Migration", "workstream", "workstream:atlas-migration"), _a("IAM Gateway", "system", "system:iam-gateway")), ("explicit", "multi_entity")),
    Scenario("jira", "ACME-482 affects Acme Health and the Enterprise Renewal.", (_a("ACME-482", "issue", "issue:acme-482"), _a("Acme Health", "customer", "customer:acme-health"), _a("Enterprise Renewal", "commitment", "commitment:enterprise-renewal")), ("abbreviation", "multi_entity")),
    Scenario("jira", "Decision D-17: Project Northstar moves to Team Aurora.", (_a("D-17", "decision", "decision:d-17"), _a("Project Northstar", "project", "project:northstar"), _a("Team Aurora", "team", "team:aurora")), ("identifier", "multi_entity")),
    Scenario("jira", "Incident INC-77 degraded Billing API for Globex.", (_a("INC-77", "incident", "incident:inc-77"), _a("Billing API", "service", "service:billing-api"), _a("Globex", "customer", "customer:globex")), ("identifier", "multi_entity")),
    Scenario("jira", "Quarterly Retention is the goal; no owner assigned.", (_a("Quarterly Retention", "goal", "goal:quarterly-retention"),), ("explicit",)),
    Scenario("jira", "Routine punctuation cleanup with no named company object.", (), ("negative",)),
    Scenario("email", "Maya Chen approved the Data Processing Agreement for Acme Health.", (_a("Maya Chen", "person", "person:maya-chen"), _a("Data Processing Agreement", "contract", "contract:acme-dpa"), _a("Acme Health", "customer", "customer:acme-health")), ("explicit", "multi_entity")),
    Scenario("email", "Please ask VP Sales Jordan Lee about the EMEA Expansion role.", (_a("VP Sales", "role", "role:vp-sales"), _a("Jordan Lee", "person", "person:jordan-lee"), _a("EMEA Expansion", "workstream", "workstream:emea-expansion")), ("role", "abbreviation")),
    Scenario("email", "The note says \"Project Atlas\"; Atlas here means the migration program.", (_a("Project Atlas", "project", "project:atlas"), _a("Atlas", "project", "project:atlas", 1)), ("quoted", "duplicate_surface")),
    Scenario("email", "SRE transferred the Runbook Vault to Platform Team.", (_a("SRE", "team", "team:site-reliability"), _a("Runbook Vault", "resource", "resource:runbook-vault"), _a("Platform Team", "team", "team:platform")), ("abbreviation", "multi_entity")),
    Scenario("email", "Mercury renewed Mercury after counsel reviewed both references.", (_a("Mercury", "customer", "customer:mercury", 0), _a("Mercury", "contract", "contract:mercury", 1)), ("ambiguous", "duplicate_surface")),
    Scenario("email", "Thanks, everything looks ordinary and no action is needed.", (), ("negative",)),
    Scenario("slack", "@Maya can you check Acme Health before launch?", (_a("Maya", "person", "person:maya-chen"), _a("Acme Health", "customer", "customer:acme-health")), ("explicit",), "standalone"),
    Scenario("slack", "<@U018|maya> owns Atlas Migration with Team Aurora.", (_a("<@U018|maya>", "person", "person:maya-chen"), _a("Atlas Migration", "workstream", "workstream:atlas-migration"), _a("Team Aurora", "team", "team:aurora")), ("slack_native", "multi_entity"), "standalone"),
    Scenario("slack", "It is blocked again.", (_a("It", "workstream", "workstream:atlas-migration"),), ("anaphora",), "threaded"),
    Scenario("slack", "The customer rejected it after Jordan's review.", (_a("The customer", "customer", "customer:acme-health"), _a("it", "contract", "contract:acme-dpa"), _a("Jordan's", "person", "person:jordan-lee")), ("anaphora", "possessive"), "threaded"),
    Scenario("slack", "same owner as the launch", (_a("same owner", "person", "person:maya-chen"), _a("the launch", "project", "project:northstar")), ("anaphora", "ellipsis"), "cross_thread"),
    Scenario("slack", "Aurora said Mercury is not Mercury the contract.", (_a("Aurora", "team", "team:aurora"), _a("Mercury", "customer", "customer:mercury", 0), _a("Mercury", "contract", "contract:mercury", 1)), ("ambiguous", "duplicate_surface"), "cross_thread"),
    Scenario("slack", "DPA is waiting on Acme; that expires Friday.", (_a("DPA", "contract", "contract:acme-dpa"), _a("Acme", "customer", "customer:acme-health"), _a("that", "contract", "contract:acme-dpa")), ("abbreviation", "anaphora"), "temporally_distributed"),
    Scenario("slack", "Northstar changed. The decision from yesterday still applies.", (_a("Northstar", "project", "project:northstar"), _a("The decision", "decision", "decision:d-17")), ("temporal", "anaphora"), "temporally_distributed"),
    Scenario("slack", "<#C42|proj-atlas> tracks ACME-482 for Maya Chen.", (_a("<#C42|proj-atlas>", "channel", "channel:proj-atlas"), _a("ACME-482", "issue", "issue:acme-482"), _a("Maya Chen", "person", "person:maya-chen")), ("slack_native", "identifier"), "standalone"),
    Scenario("slack", "No blockers today; lunch moved to noon.", (), ("negative",), "standalone"),
    Scenario("slack", "I agree with the above.", (), ("negative", "context_without_entity"), "threaded"),
    Scenario("slack", "The API team paged Billing API during INC-77.", (_a("The API team", "team", "team:platform"), _a("Billing API", "service", "service:billing-api"), _a("INC-77", "incident", "incident:inc-77")), ("nested", "identifier"), "threaded"),
    Scenario("jira", "M&A Readiness depends on Legal Ops.", (_a("M&A Readiness", "goal", "goal:ma-readiness"), _a("Legal Ops", "team", "team:legal-ops")), ("punctuation", "multi_entity")),
    Scenario("email", "Renée Dubois shared Café Europe Forecast with Finance.", (_a("Renée Dubois", "person", "person:renee-dubois"), _a("Café Europe Forecast", "resource", "resource:cafe-europe-forecast"), _a("Finance", "team", "team:finance")), ("unicode", "multi_entity")),
    Scenario("slack", "Quoted from Jira: \"Acme Health blocked Project Northstar\".", (_a("Acme Health", "customer", "customer:acme-health"), _a("Project Northstar", "project", "project:northstar")), ("quoted", "nested"), "cross_thread"),
    Scenario("jira", "IBM and I.B.M. refer to the same customer account.", (_a("IBM", "customer", "customer:ibm"), _a("I.B.M.", "customer", "customer:ibm")), ("abbreviation", "alias")),
    Scenario("email", "Alex Kim met Alex Kim; the first is counsel, the second is engineering.", (_a("Alex Kim", "person", "person:alex-kim-legal", 0), _a("Alex Kim", "person", "person:alex-kim-engineering", 1)), ("ambiguous", "duplicate_surface")),
    Scenario("slack", "they approved it", (_a("they", "team", "team:legal-ops"), _a("it", "decision", "decision:d-17")), ("anaphora", "all_context"), "temporally_distributed"),
)


# Frozen characterization from 2026-07-17. It is descriptive, not a target:
# executable_report always recomputes metrics from the current locator.
PRE_IMPROVEMENT_BASELINE = {
    "adapter": "persisted_batch_mention_opportunities",
    "corpus_sha256": "37f3ecc89dae02ce7009882870cdad23f1066f25173ba0257a34bc11bee2517c",
    "span_precision": 0.7164179104477612,
    "span_recall": 0.7619047619047619,
    "span_f1": 0.7384615384615385,
    "boundary_credit_recall": 0.8332565523194535,
    "slack_span_recall": 0.7931034482758621,
    "temporally_distributed_slack_span_recall": 0.8571428571428571,
    "anaphora_span_recall": 0.8461538461538461,
    "negative_clean_signal_rate": 0.5,
    "type_accuracy": 0.0,
    "canonical_link_coverage": 0.0,
}


def sealed_corpus() -> tuple[tuple[GoldSignal, ...], tuple[GoldMention, ...], dict[str, tuple[str, ...]]]:
    signals: list[GoldSignal] = []
    mentions: list[GoldMention] = []
    phenomena: dict[str, tuple[str, ...]] = {}
    # Four repetitions deliberately model repeated evidence in separate batches.
    for repetition in range(4):
        for index, scenario in enumerate(_SCENARIOS):
            signal_id = f"gold-{repetition:02d}-{index:02d}"
            signal = GoldSignal(
                signal_id=signal_id,
                batch_id=f"batch-{repetition:02d}-{index // 10:02d}",
                source_type=scenario.source,
                text=scenario.text,
                slack_context=scenario.slack_context,
            )
            signals.append(signal)
            phenomena[signal_id] = scenario.phenomena
            for annotation_index, annotation in enumerate(scenario.annotations):
                starts = _literal_occurrences(scenario.text, annotation.surface)
                if annotation.occurrence >= len(starts):
                    raise AssertionError((scenario.text, annotation))
                start = starts[annotation.occurrence]
                mentions.append(GoldMention(
                    mention_id=f"mention-{repetition:02d}-{index:02d}-{annotation_index:02d}",
                    signal_id=signal_id,
                    start=start,
                    end=start + len(annotation.surface),
                    entity_type=annotation.entity_type,
                    canonical_referent=annotation.referent,
                ))
    return tuple(signals), tuple(mentions), phenomena


def persisted_batch_predictions(signals: Iterable[GoldSignal]) -> tuple[PredictedMention, ...]:
    """Adapt today's persisted-batch opportunity path into evaluator predictions.

    The locator does not classify or resolve identity. Reporting ``unknown`` and
    abstention is intentional: it prevents candidate discovery from being
    mistaken for end-to-end entity understanding.
    """
    predictions: list[PredictedMention] = []
    batches: dict[str, list[GoldSignal]] = defaultdict(list)
    for signal in signals:
        batches[signal.batch_id].append(signal)
    for batch_id in sorted(batches):
        batch = batches[batch_id]
        if len(batch) != 10:
            raise ValueError(f"sealed evaluation requires 10-signal batches; {batch_id} has {len(batch)}")
        for signal in batch:
            surfaces = _persisted_mention_opportunities(
                content={},
                content_text=signal.text,
                source_channel=("slack:message" if signal.source_type == "slack" else signal.source_type),
                has_structural_context=(
                    signal.source_type == "slack" and signal.slack_context != "standalone"
                ),
            )
            for surface_index, surface in enumerate(surfaces):
                spans = locate_explicit_surface_spans(signal.text, surface)
                for occurrence, (start, end) in enumerate(spans):
                    predictions.append(PredictedMention(
                        prediction_id=f"prediction-{signal.signal_id}-{surface_index}-{occurrence}",
                        signal_id=signal.signal_id,
                        start=start,
                        end=end,
                        entity_type="unknown",
                        canonical_referent=None,
                        confidence=0.5,
                        abstained=True,
                        candidate_fate="detected_unresolved",
                    ))
    return tuple(predictions)


def executable_report() -> dict:
    signals, gold, phenomena = sealed_corpus()
    predictions = persisted_batch_predictions(signals)
    overall = evaluate_gold_entity_extraction(signals=signals, gold_mentions=gold, predictions=predictions)
    report = overall.model_dump(mode="json")
    report["prediction_adapter"] = {
        "name": "persisted_batch_bootstrap_locator",
        "capability_boundary": "candidate_surface_discovery_only",
        "classification_available": False,
        "canonical_resolution_available": False,
    }
    report["pre_improvement_baseline"] = PRE_IMPROVEMENT_BASELINE
    report["corpus"] = {
        "sealed_sha256": corpus_sha256(signals, gold, phenomena),
        "signals": len(signals),
        "batches": len({signal.batch_id for signal in signals}),
        "gold_mentions": len(gold),
        "negative_signals": sum(not any(item.signal_id == signal.signal_id for item in gold) for signal in signals),
    }
    negative_ids = {
        signal.signal_id for signal in signals
        if not any(item.signal_id == signal.signal_id for item in gold)
    }
    negative_predictions = [item for item in predictions if item.signal_id in negative_ids]
    report["negative_control"] = {
        "signal_count": len(negative_ids),
        "prediction_count": len(negative_predictions),
        "clean_signal_rate": (
            sum(not any(item.signal_id == signal_id for item in negative_predictions) for signal_id in negative_ids)
            / len(negative_ids)
        ),
    }
    report["by_entity_type"] = _subset_metrics(
        signals,
        gold,
        predictions,
        {item.entity_type: set() for item in gold},
        gold_filter=True,
    )
    phenomenon_groups = {
        name: {signal_id for signal_id, names in phenomena.items() if name in names}
        for name in sorted({name for names in phenomena.values() for name in names})
    }
    report["by_phenomenon"] = _subset_metrics(signals, gold, predictions, phenomenon_groups)
    report["by_batch"] = _subset_metrics(signals, gold, predictions, {
        batch_id: {signal.signal_id for signal in signals if signal.batch_id == batch_id}
        for batch_id in sorted({signal.batch_id for signal in signals})
    })
    return report


def corpus_sha256(signals, gold, phenomena) -> str:
    payload = {
        "signals": [item.model_dump(mode="json") for item in signals],
        "gold": [item.model_dump(mode="json") for item in gold],
        "phenomena": phenomena,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _subset_metrics(signals, gold, predictions, groups, *, gold_filter: bool = False):
    output = {}
    for name, ids in groups.items():
        if gold_filter:
            subset_gold = [item for item in gold if item.entity_type == name]
            ids = {item.signal_id for item in subset_gold}
        else:
            subset_gold = [item for item in gold if item.signal_id in ids]
        subset_signals = [item for item in signals if item.signal_id in ids]
        subset_predictions = [item for item in predictions if item.signal_id in ids]
        output[name] = evaluate_gold_entity_extraction(
            signals=subset_signals,
            gold_mentions=subset_gold,
            predictions=subset_predictions,
        ).overall.model_dump(mode="json")
    return output


def _literal_occurrences(text: str, surface: str) -> tuple[int, ...]:
    starts: list[int] = []
    cursor = 0
    while (position := text.find(surface, cursor)) >= 0:
        starts.append(position)
        cursor = position + len(surface)
    return tuple(starts)
