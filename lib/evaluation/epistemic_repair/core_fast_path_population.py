"""Provider-blind 4x25 population for the core fast-path M0 vertical.

The population contains normalized company signals only.  Semantic roles,
expected entities, lifecycle transitions, and scoring instructions live in the
separate post-execution gold module.  Production-shaped runners may import this
module but must never import ``core_fast_path_gold``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from lib.contracts.kernel import canonical_sha256


CORE_FAST_PATH_POPULATION_VERSION = "company-learning-core-fast-path-4x25-v1"
CORE_FAST_PATH_BATCH_COUNT = 4
CORE_FAST_PATH_SIGNALS_PER_BATCH = 25
CORE_FAST_PATH_SIGNAL_COUNT = 100
CORE_FAST_PATH_STORYLINES = ("harbor", "northstar", "access", "delta")


@dataclass(frozen=True, slots=True)
class CoreFastPathSignal:
    signal_id: str
    batch_number: int
    position: int
    source_channel: str
    source_space: str
    text: str
    trust_tier: str = "unvetted"


@dataclass(frozen=True, slots=True)
class CoreFastPathBatch:
    batch_number: int
    signals: tuple[CoreFastPathSignal, ...]


@dataclass(frozen=True, slots=True)
class CoreFastPathPopulation:
    version: str
    batches: tuple[CoreFastPathBatch, ...]
    population_digest: str

    @property
    def signals(self) -> tuple[CoreFastPathSignal, ...]:
        return tuple(signal for batch in self.batches for signal in batch.signals)


_SURFACE = {
    "harbor": "Harbor release",
    "northstar": "Northstar pilot",
    "access": "Access review",
    "delta": "Delta handoff",
}


def _source(storyline: str, ordinal: int) -> tuple[str, str]:
    regimes = {
        "harbor": (
            ("slack:message", "slack:harbor-release"),
            ("jira:issue", "jira:harbor-release"),
            ("email:message", "email:infrastructure"),
            ("crm:activity", "crm:launch-readiness"),
            ("slack:message", "slack:harbor-release"),
        ),
        "northstar": (
            ("email:message", "email:northstar-pilot"),
            ("crm:activity", "crm:northstar"),
            ("slack:message", "slack:customer-success"),
            ("jira:issue", "jira:northstar-pilot"),
            ("email:message", "email:northstar-pilot"),
        ),
        "access": (
            ("jira:issue", "jira:access-review"),
            ("slack:message", "slack:security"),
            ("email:message", "email:identity-review"),
            ("jira:comment", "jira:access-review"),
            ("slack:message", "slack:security"),
        ),
        "delta": (
            ("slack:message", "slack:support"),
            ("jira:issue", "jira:delta-handoff"),
            ("email:message", "email:delta-escalation"),
            ("crm:activity", "crm:delta-account"),
            ("slack:message", "slack:operations"),
        ),
    }
    return regimes[storyline][ordinal - 1]


def _harbor_text(batch: int, ordinal: int) -> str:
    rows = {
        1: (
            "Harbor release, update 1: The release certificate has no confirmed owner.",
            "Harbor release, update 1: The Jira gate remains blocked on certificate renewal.",
            "Harbor release, update 1: Infrastructure has not confirmed certificate renewal.",
            "Harbor release, update 1: The readiness field is green although the certificate record is open.",
            "In the Harbor release thread, it is still waiting on the owner handoff.",
        ),
        2: (
            "Harbor release, update 2: A second thread links the delay to the open certificate.",
            "Harbor release, update 2: The ticket records another ownership handoff before the slip.",
            "Harbor release, update 2: The infrastructure owner confirms renewal is incomplete.",
            "Harbor release, update 2: The launch window moved after the certificate handoff.",
            "In the Harbor release thread, it meant the certificate rather than the launch note.",
        ),
        3: (
            "Harbor release, update 3: Two independent records connect the open certificate to the delay.",
            "Harbor release, update 3: Certificate ownership changed immediately before the blocked gate.",
            "Harbor release is blocked by incomplete certificate renewal.",
            "Harbor release, update 3: The owner timestamp agrees with the delayed rollout window.",
            "Harbor release, update 3: The readiness field still conflicts with the source records.",
        ),
        4: (
            "Harbor release, update 4: The identity owner recorded the completed certificate handoff.",
            "Harbor release, update 4: The certificate renewal ticket closed with an authority timestamp.",
            "Harbor release is no longer blocked after certificate renewal completed.",
            "Harbor release, update 4: The rollout gate reopened after the corrected certificate state.",
            "Harbor release, update 4: The earlier blocked interpretation remains only as history.",
        ),
    }
    return rows[batch][ordinal - 1]


def _background_text(storyline: str, batch: int, ordinal: int) -> str:
    surface = _SURFACE[storyline]
    themes = {
        "northstar": (
            "the kickoff agenda now includes SSO setup",
            "the billing contact was added to the worksheet",
            "legal returned a formatting comment",
            "the sample import opened without validation errors",
            "the weekly recap remains scheduled",
        ),
        "access": (
            "the token review remains scheduled for Friday",
            "two dormant accounts were removed from the report",
            "the audit export is ready for review",
            "the scanner found no new critical issue",
            "the service-account owner label is under review",
        ),
        "delta": (
            "support added the escalation owner to the agenda",
            "the incident ticket received a routine status update",
            "the customer reply includes the current handoff time",
            "the account note retains its existing health value",
            "operations moved the next check-in by fifteen minutes",
        ),
    }
    return f"{surface}, update {batch}: {themes[storyline][ordinal - 1]}."


def _story_text(storyline: str, batch: int, ordinal: int) -> str:
    return (
        _harbor_text(batch, ordinal)
        if storyline == "harbor"
        else _background_text(storyline, batch, ordinal)
    )


def _build_batch(batch_number: int) -> CoreFastPathBatch:
    rows: list[CoreFastPathSignal] = []
    for ordinal in range(1, 6):
        for storyline in CORE_FAST_PATH_STORYLINES:
            channel, space = _source(storyline, ordinal)
            rows.append(CoreFastPathSignal(
                signal_id=(
                    f"cf2-{storyline}-b{batch_number:02d}-o{ordinal:02d}"
                ),
                batch_number=batch_number,
                position=len(rows) + 1,
                source_channel=channel,
                source_space=space,
                text=_story_text(storyline, batch_number, ordinal),
                trust_tier=(
                    "authoritative"
                    if storyline == "harbor" and batch_number == 4
                    and ordinal in {1, 2, 3}
                    else "unvetted"
                ),
            ))
    noise = (
        "Facilities changed the lunch delivery entrance.",
        "The book club moved its informal discussion.",
        "A test calendar received a new color label.",
    )
    for ordinal, text in enumerate(noise, 1):
        rows.append(CoreFastPathSignal(
            signal_id=f"cf2-noise-b{batch_number:02d}-o{ordinal:02d}",
            batch_number=batch_number,
            position=len(rows) + 1,
            source_channel="slack:message",
            source_space="slack:general",
            text=f"Week {batch_number}: {text}",
        ))
    distractors = (
        "The Harbor certificate training example uses a handoff checklist.",
        "Northstar paint approval appears in the Access office ticket.",
    )
    for ordinal, text in enumerate(distractors, 1):
        rows.append(CoreFastPathSignal(
            signal_id=f"cf2-distractor-b{batch_number:02d}-o{ordinal:02d}",
            batch_number=batch_number,
            position=len(rows) + 1,
            source_channel="jira:comment",
            source_space="jira:workplace",
            text=f"Week {batch_number}: {text}",
        ))
    if len(rows) != CORE_FAST_PATH_SIGNALS_PER_BATCH:
        raise AssertionError("core fast-path batch must contain exactly 25 signals")

    # Rotate physical order so neither position nor source ordering exposes the
    # semantic role.  Signal identity remains stable across construction.
    offset = (batch_number * 7) % CORE_FAST_PATH_SIGNALS_PER_BATCH
    rotated = rows[offset:] + rows[:offset]
    return CoreFastPathBatch(
        batch_number=batch_number,
        signals=tuple(
            CoreFastPathSignal(
                signal_id=signal.signal_id,
                batch_number=batch_number,
                position=position,
                source_channel=signal.source_channel,
                source_space=signal.source_space,
                text=signal.text,
                trust_tier=signal.trust_tier,
            )
            for position, signal in enumerate(rotated, 1)
        ),
    )


def build_core_fast_path_population() -> CoreFastPathPopulation:
    batches = tuple(
        _build_batch(batch_number)
        for batch_number in range(1, CORE_FAST_PATH_BATCH_COUNT + 1)
    )
    payload = {
        "version": CORE_FAST_PATH_POPULATION_VERSION,
        "batches": [asdict(batch) for batch in batches],
    }
    return CoreFastPathPopulation(
        version=CORE_FAST_PATH_POPULATION_VERSION,
        batches=batches,
        population_digest=canonical_sha256(payload),
    )


__all__ = [
    "CORE_FAST_PATH_BATCH_COUNT",
    "CORE_FAST_PATH_POPULATION_VERSION",
    "CORE_FAST_PATH_SIGNAL_COUNT",
    "CORE_FAST_PATH_SIGNALS_PER_BATCH",
    "CORE_FAST_PATH_STORYLINES",
    "CoreFastPathBatch",
    "CoreFastPathPopulation",
    "CoreFastPathSignal",
    "build_core_fast_path_population",
]
