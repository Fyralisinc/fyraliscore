"""Sealed 12x25 mixed-stream population for the P6 decisive run.

Runtime signals intentionally contain no benchmark labels or memory instructions.
All episode, mention, thesis, and distractor truth lives in the sealed oracle.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from lib.contracts.kernel import canonical_sha256


P6_POPULATION_VERSION = "epistemic-repair-p6-mixed-stream-12x25-v3"
P6_BATCH_COUNT = 12
P6_SIGNALS_PER_BATCH = 25
P6_SIGNAL_COUNT = 300
P6_STORYLINES = ("atlas", "beacon", "cobalt", "delta")


@dataclass(frozen=True, slots=True)
class P6Signal:
    signal_id: str
    batch_number: int
    position: int
    source_channel: str
    source_space: str
    text: str


@dataclass(frozen=True, slots=True)
class P6MentionGold:
    surface: str
    entity_types: tuple[str, ...]
    required: bool = True


@dataclass(frozen=True, slots=True)
class P6Gold:
    signal_id: str
    storyline_id: str | None
    role: str
    entity_surface: str | None
    entity_type: str | None
    canonical_ref: str | None
    claim_id: str | None
    lifecycle_phase: str
    local_mentions: tuple[P6MentionGold, ...] = ()


@dataclass(frozen=True, slots=True)
class P6Batch:
    batch_number: int
    signals: tuple[P6Signal, ...]


@dataclass(frozen=True, slots=True)
class P6Population:
    version: str
    batches: tuple[P6Batch, ...]
    gold: tuple[P6Gold, ...]
    thesis_by_storyline: tuple[tuple[str, str], ...]
    synthesis_signal_by_storyline: tuple[tuple[str, str], ...]
    population_digest: str
    preregistration_digest: str

    @property
    def signals(self) -> tuple[P6Signal, ...]:
        return tuple(signal for batch in self.batches for signal in batch.signals)


def _source(storyline: str, batch: int, ordinal: int) -> tuple[str, str]:
    """Return the preregistered source regime without encoding it in text."""

    if storyline == "atlas":
        return "slack:message", "slack:release-room"
    if storyline == "beacon":
        return (("slack:message", "slack:engineering") if ordinal == 5 else
                ("jira:issue", "jira:beacon-migration"))
    if storyline == "cobalt":
        return (("email:message", "email:cobalt-renewal") if batch <= 6 else
                ("crm:activity", "crm:cobalt-account"))
    return (
        ("slack:message", "slack:support"),
        ("jira:issue", "jira:delta-incidents"),
        ("email:message", "email:delta-escalations"),
        ("crm:activity", "crm:delta-account"),
        ("slack:message", "slack:operations"),
    )[ordinal - 1]
_ENTITY = {
    "atlas": ("Atlas release", "workstream", "workstream:atlas-release"),
    "beacon": ("Beacon migration", "workstream", "workstream:beacon-migration"),
    "cobalt": ("Cobalt renewal", "commitment", "commitment:cobalt-renewal"),
    "delta": ("Delta handoff", "workstream", "workstream:delta-handoff"),
}
_THESIS = {
    "atlas": "Atlas slips recur when certificate ownership changes during handoff.",
    "beacon": "Beacon completion depends on access review, not deploy readiness.",
    "cobalt": "Cobalt renewal risk follows customer approval despite optimistic CRM state.",
    "delta": "Delta incidents recur when support handoff lacks an explicit owner.",
}
_SYNTHESIS_BATCH = {"atlas": 4, "beacon": 6, "cobalt": 9, "delta": 12}


def _phase(batch: int) -> str:
    if batch <= 3:
        return "weak_initial"
    if batch <= 6:
        return "corroboration"
    if batch <= 8:
        return "contradiction"
    if batch <= 10:
        return "correction"
    return "external_outcome"


def _story_text(storyline: str, batch: int, ordinal: int) -> str:
    surface = _ENTITY[storyline][0]
    phase = _phase(batch)
    if batch == _SYNTHESIS_BATCH[storyline] and ordinal == 3:
        # Ordinary asserted report selected by the provider-free production seam.
        return f"{surface} is ready."
    mechanisms = {
        "atlas": {
            "dependency": "release certificate",
            "transition": "certificate ownership handoff",
            "authority": "infrastructure owner",
            "outcome": "rollout window",
            "conflict": "release dashboard",
        },
        "beacon": {
            "dependency": "privileged access review",
            "transition": "security approval transition",
            "authority": "identity reviewer",
            "outcome": "migration completion",
            "conflict": "deployment dashboard",
        },
        "cobalt": {
            "dependency": "customer approval email",
            "transition": "renewal approval transition",
            "authority": "customer procurement lead",
            "outcome": "renewal signature",
            "conflict": "CRM health field",
        },
        "delta": {
            "dependency": "named support owner",
            "transition": "support-to-operations handoff",
            "authority": "incident commander",
            "outcome": "repeat incident rate",
            "conflict": "handoff checklist",
        },
    }[storyline]
    d, t, a, o, c = (mechanisms[key] for key in
                      ("dependency", "transition", "authority", "outcome", "conflict"))
    variants = {
        "weak_initial": (
            f"The {d} still has no clearly recorded owner.",
            f"A late reply asks whether the {t} happened before today's check.",
            f"The {c} remains optimistic while the underlying record is incomplete.",
            f"Someone says 'they have it now' without naming the {a}.",
            f"The {o} moved again after the ownership question resurfaced.",
        ),
        "corroboration": (
            f"A second source links the open {d} to the delayed {o}.",
            f"The structured ticket records another {t} just before status changed.",
            f"The {a} supplied a timestamp that matches the earlier thread.",
            f"Two independent records now connect ownership of {d} with {o}.",
            f"A later reply clarifies that 'it' meant the {d}, not the launch note.",
        ),
        "contradiction": (
            f"The {c} says complete, but the {a} says {d} is still open.",
            f"No completed {t} appears in the audit trail.",
            f"The higher-trust message conflicts with the optimistic {c} value.",
            f"A returning participant disputes who owned {d} at the cutoff.",
            f"The {o} remains at risk despite a completion label.",
        ),
        "correction": (
            f"The {a} corrected the ownership timestamp for {d}.",
            f"Adjudication identified the missing {t}.",
            f"The {c} now reflects that {d} was incomplete at the cutoff.",
            f"Dependent {o} notes were revised after the ownership correction.",
            f"The earlier completion interpretation is retained only as history.",
        ),
        "external_outcome": (
            f"After the corrected {t}, the next {o} completed without the prior delay.",
            f"A later external result independently matches the adjudicated {d} state.",
            f"The retained ownership explanation answered the follow-up without replay.",
            f"No stale {d} state appears in the current view.",
            f"The final audit preserves the evidence chain from {t} to {o}.",
        ),
    }
    return f"{surface}, update {batch}: {variants[phase][ordinal - 1]}"


def build_p6_population() -> P6Population:
    batches: list[P6Batch] = []
    gold: list[P6Gold] = []
    synthesis: list[tuple[str, str]] = []
    for batch in range(1, P6_BATCH_COUNT + 1):
        rows: list[tuple[P6Signal, P6Gold]] = []
        for ordinal in range(1, 6):
            for storyline in P6_STORYLINES:
                position = len(rows) + 1
                channel, space = _source(storyline, batch, ordinal)
                surface, entity_type, canonical_ref = _ENTITY[storyline]
                signal_id = f"p6-b{batch:02d}-s{position:02d}"
                is_synthesis = batch == _SYNTHESIS_BATCH[storyline] and ordinal == 3
                role = "synthesis" if is_synthesis else "storyline"
                signal = P6Signal(signal_id, batch, position, channel, space,
                                  _story_text(storyline, batch, ordinal))
                item = P6Gold(signal_id, storyline, role, surface, entity_type,
                              canonical_ref, f"{storyline}:{_phase(batch)}:{ordinal}",
                              _phase(batch))
                rows.append((signal, item))
                if is_synthesis:
                    synthesis.append((storyline, signal_id))
        for ordinal in range(1, 4):
            position = len(rows) + 1
            signal_id = f"p6-b{batch:02d}-s{position:02d}"
            text = (
                "Facilities changed the lunch delivery entrance.",
                "The book club moved its informal discussion.",
                "A test calendar received a new color label.",
            )[ordinal - 1]
            text = f"Week {batch}: {text}"
            rows.append((
                P6Signal(signal_id, batch, position, "slack:message", "slack:general", text),
                P6Gold(
                    signal_id, None, "noise", None, None, None, None,
                    _phase(batch),
                    local_mentions=(
                        (P6MentionGold("Facilities", ("organizational_unit", "team"), False),)
                        if ordinal == 1 else ()
                    ),
                ),
            ))
        for ordinal in range(1, 3):
            position = len(rows) + 1
            signal_id = f"p6-b{batch:02d}-s{position:02d}"
            text = (
                "The Atlas certificate training example uses a handoff checklist.",
                "Cobalt paint approval is listed in the Beacon office ticket.",
            )[ordinal - 1]
            text = f"Week {batch}: {text}"
            rows.append((
                P6Signal(signal_id, batch, position, "jira:comment", "jira:workplace", text),
                P6Gold(
                    signal_id, None, "high_similarity_distractor", None, None,
                    None, None, _phase(batch),
                    local_mentions=(
                        (
                            P6MentionGold("Cobalt paint approval", ("work_item",)),
                            P6MentionGold("Beacon office ticket", ("work_item",)),
                        )
                        if ordinal == 2 else ()
                    ),
                ),
            ))
        assert len(rows) == P6_SIGNALS_PER_BATCH
        # Rotate physical order so transport position never exposes storyline.
        offset = (batch * 7) % P6_SIGNALS_PER_BATCH
        rotated = rows[offset:] + rows[:offset]
        normalized: list[P6Signal] = []
        for position, (signal, item) in enumerate(rotated, 1):
            normalized_signal = P6Signal(signal.signal_id, batch, position,
                                         signal.source_channel, signal.source_space,
                                         signal.text)
            normalized.append(normalized_signal)
            gold.append(P6Gold(signal.signal_id, item.storyline_id, item.role,
                               item.entity_surface, item.entity_type,
                               item.canonical_ref, item.claim_id,
                               item.lifecycle_phase, item.local_mentions))
        batches.append(P6Batch(batch, tuple(normalized)))
    payload = {
        "version": P6_POPULATION_VERSION,
        "batches": [asdict(batch) for batch in batches],
        "gold": [asdict(item) for item in gold],
        "theses": _THESIS,
        "synthesis": synthesis,
    }
    preregistration = {
        "population_digest": canonical_sha256(payload),
        "threshold_contract": "coordinator-sections-19.5-19.6",
        "provider_mode": "provider_free_deterministic_production_seams",
    }
    return P6Population(
        P6_POPULATION_VERSION, tuple(batches), tuple(gold), tuple(_THESIS.items()),
        tuple(synthesis), canonical_sha256(payload), canonical_sha256(preregistration),
    )


__all__ = ["P6_BATCH_COUNT", "P6_SIGNALS_PER_BATCH", "P6_SIGNAL_COUNT",
           "P6_STORYLINES", "P6Batch", "P6Gold", "P6MentionGold", "P6Population", "P6Signal",
           "build_p6_population"]
