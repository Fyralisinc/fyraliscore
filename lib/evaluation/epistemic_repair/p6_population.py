"""Sealed 12x25 mixed-stream population for the P6 decisive run.

Runtime signals intentionally contain no benchmark labels or memory instructions.
All episode, mention, thesis, and distractor truth lives in the sealed oracle.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from lib.contracts.kernel import canonical_sha256


P6_POPULATION_VERSION = "epistemic-repair-p6-mixed-stream-12x25-v1"
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
class P6Gold:
    signal_id: str
    storyline_id: str | None
    role: str
    entity_surface: str | None
    entity_type: str | None
    canonical_ref: str | None
    claim_id: str | None
    lifecycle_phase: str


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
    variants = {
        "weak_initial": (
            "A short thread returned to the same owner question.",
            "The linked record still shows yesterday's status.",
            "Someone asked whether the handoff happened before the check.",
            "A pronoun-only reply arrived after a long pause.",
            "The calendar moved while the operational state stayed unclear.",
        ),
        "corroboration": (
            "A second source mentioned the same transition.",
            "The structured ticket linked the earlier conversation.",
            "An independent owner supplied a matching timestamp.",
            "The evidence now spans two source systems.",
            "A later reply clarified which object the thread meant.",
        ),
        "contradiction": (
            "The newest source conflicts with the optimistic status.",
            "A required transition is absent from the audit trail.",
            "The higher-trust record disagrees with the summary field.",
            "A returning participant challenged the earlier interpretation.",
            "The dependency appears open despite a completion label.",
        ),
        "correction": (
            "The accountable owner corrected the disputed timestamp.",
            "Adjudication identified the missing transition.",
            "The linked record now reflects the authoritative state.",
            "Dependent notes were revised after the correction.",
            "The previous interpretation is retained only as history.",
        ),
        "external_outcome": (
            "The subsequent operational result matched the adjudicated state.",
            "A later customer-visible event supplied independent evidence.",
            "The retained explanation answered the follow-up without replay.",
            "No stale dependency appeared in the current view.",
            "The final audit preserved the original evidence chain.",
        ),
    }
    return f"{surface}: {variants[phase][ordinal - 1]}"


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
            rows.append((
                P6Signal(signal_id, batch, position, "slack:message", "slack:general", text),
                P6Gold(signal_id, None, "noise", None, None, None, None, _phase(batch)),
            ))
        for ordinal in range(1, 3):
            position = len(rows) + 1
            signal_id = f"p6-b{batch:02d}-s{position:02d}"
            text = (
                "The Atlas certificate training example uses a handoff checklist.",
                "Cobalt paint approval is listed in the Beacon office ticket.",
            )[ordinal - 1]
            rows.append((
                P6Signal(signal_id, batch, position, "jira:comment", "jira:workplace", text),
                P6Gold(signal_id, None, "high_similarity_distractor", None, None,
                       None, None, _phase(batch)),
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
                               item.lifecycle_phase))
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
           "P6_STORYLINES", "P6Batch", "P6Gold", "P6Population", "P6Signal",
           "build_p6_population"]
