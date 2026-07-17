"""Sealed 3x25 zero-seed population for the P5 vertical learning proof.

The signal text is ordinary company activity.  Oracle roles live only in this
sealed evaluator contract; no signal contains benchmark labels, scoring hints,
or instructions to create or retrieve memory.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from lib.contracts.kernel import canonical_sha256


P5_POPULATION_VERSION = "epistemic-repair-p5-zero-seed-3x25-v2"
P5_BATCH_COUNT = 3
P5_SIGNALS_PER_BATCH = 25
P5_SIGNAL_COUNT = P5_BATCH_COUNT * P5_SIGNALS_PER_BATCH

CENTRAL_EPISODE = "harbor-release"
CUSTOMER_EPISODE = "northstar-pilot"
SECURITY_EPISODE = "access-review"
P5_EPISODE_IDS = (CENTRAL_EPISODE, CUSTOMER_EPISODE, SECURITY_EPISODE)

@dataclass(frozen=True, slots=True)
class P5Signal:
    signal_id: str
    batch_number: int
    position: int
    episode_id: str
    source_channel: str
    source_space: str
    text: str


@dataclass(frozen=True, slots=True)
class P5VerticalOracle:
    atomic_signal_id: str
    reuse_relation_signal_id: str
    correction_signal_id: str
    expected_relation_kind: str


@dataclass(frozen=True, slots=True)
class P5Batch:
    batch_number: int
    signals: tuple[P5Signal, ...]


@dataclass(frozen=True, slots=True)
class P5Population:
    version: str
    batches: tuple[P5Batch, ...]
    oracle: P5VerticalOracle
    population_digest: str

    @property
    def signals(self) -> tuple[P5Signal, ...]:
        return tuple(signal for batch in self.batches for signal in batch.signals)


_TARGET_POSITION = {1: 13, 2: 10, 3: 16}
_TARGET_TEXT = {
    1: "Harbor release is blocked.",
    2: "Certificate renewal is not complete.",
    3: "Harbor release is not blocked.",
}
_NOISE: dict[tuple[int, str], tuple[str, ...]] = {
    (1, CENTRAL_EPISODE): (
        "Mara moved the Harbor rollout check-in to Thursday.",
        "The release checklist still has two owners to confirm.",
        "Finance approved the small overage for the rollout window.",
        "The mobile build finished its routine smoke test.",
        "Ari posted revised screenshots for the launch note.",
        "The support handoff remains on tomorrow's agenda.",
        "The release room will open fifteen minutes before the check-in.",
        "Documentation is waiting on the final navigation labels.",
        "The rollout calendar now reflects the regional holiday.",
    ),
    (2, CENTRAL_EPISODE): (
        "Mara added the infrastructure owner to the Harbor thread.",
        "The rollout checklist now links to the certificate request.",
        "Support drafted a short reply for early access users.",
        "The mobile build remains unchanged from yesterday.",
        "Ari will review the launch note after lunch.",
        "The release room invite was sent to the on-call engineer.",
        "Finance left the rollout overage approved.",
        "Documentation corrected a product name in the guide.",
        "The regional calendar has no further changes this week.",
    ),
    (3, CENTRAL_EPISODE): (
        "Mara asked the infrastructure owner to verify the Harbor thread.",
        "The certificate record shows a completion timestamp from Monday.",
        "Support removed the draft delay language from its reply.",
        "The mobile build passed the same smoke test again.",
        "Ari restored the original date in the launch note.",
        "The release room invite remains active for this afternoon.",
        "Finance made no change to the approved overage.",
        "Documentation published the corrected product name.",
        "The regional calendar still shows the original rollout window.",
    ),
    (1, CUSTOMER_EPISODE): (
        "Northstar asked whether the pilot agenda can include SSO setup.",
        "Lena offered two times for the customer kickoff.",
        "The pilot worksheet is missing one billing contact.",
        "Northstar's legal team acknowledged the data-processing addendum.",
        "The solutions engineer added a sample import file.",
        "A customer success note mentions a preference for weekly recaps.",
        "The kickoff deck uses the customer's updated logo.",
        "Lena is waiting for the final attendee list.",
        "The pilot workspace has the default notification settings.",
    ),
    (2, CUSTOMER_EPISODE): (
        "Northstar confirmed that SSO setup can follow the kickoff.",
        "Lena reserved the earlier of the two proposed times.",
        "The billing contact was added to the pilot worksheet.",
        "Legal returned a formatting comment on the addendum.",
        "The sample import file opened without validation errors.",
        "Customer success scheduled the first weekly recap.",
        "The kickoff deck now includes an implementation timeline.",
        "Two additional attendees joined the pilot invite.",
        "Notification settings remain at their defaults.",
    ),
    (3, CUSTOMER_EPISODE): (
        "Northstar completed the first SSO configuration screen.",
        "Lena shared notes from the customer kickoff.",
        "The pilot worksheet now has all required contacts.",
        "Legal accepted the addendum formatting change.",
        "A second sample import file is ready for review.",
        "Customer success sent the weekly recap as scheduled.",
        "The kickoff deck was archived after the meeting.",
        "The attendee list includes the new operations lead.",
        "Northstar enabled email notifications for the pilot workspace.",
    ),
    (1, SECURITY_EPISODE): (
        "Security triage moved the token-rotation review to Friday.",
        "The access report contains three dormant contractor accounts.",
        "Ravi assigned the audit export to the identity team.",
        "A routine scanner update completed overnight.",
        "The policy draft uses the existing exception template.",
        "One service account still lacks an owner label.",
        "The weekly vulnerability digest arrived before standup.",
        "Identity operations opened a ticket for the oldest account.",
        "The audit channel topic now links to the review calendar.",
    ),
    (2, SECURITY_EPISODE): (
        "Security triage kept the token-rotation review on Friday.",
        "Two dormant contractor accounts were removed from the report.",
        "Ravi uploaded the first audit export for review.",
        "The scanner update produced no new critical findings.",
        "The policy draft received a wording suggestion.",
        "The service account owner label is under review.",
        "The vulnerability digest includes last week's remediations.",
        "Identity operations closed the oldest account ticket.",
        "The review calendar now includes the compliance observer.",
    ),
    (3, SECURITY_EPISODE): (
        "Security triage completed the token-rotation review.",
        "The final dormant contractor account was disabled.",
        "Ravi stored the approved audit export in the evidence folder.",
        "The scanner ran again with the updated signatures.",
        "The policy wording suggestion was accepted.",
        "The service account now has an owner label.",
        "The vulnerability digest has no overdue critical item.",
        "Identity operations linked the closure record to the audit.",
        "The compliance observer added a note to the review calendar.",
    ),
}


def _source(episode_id: str) -> tuple[str, str]:
    return {
        CENTRAL_EPISODE: ("jira:issue", "jira:harbor-release"),
        CUSTOMER_EPISODE: ("email:message", "email:northstar-pilot"),
        SECURITY_EPISODE: ("slack:message", "slack:security-operations"),
    }[episode_id]


def _build_batch(batch_number: int) -> P5Batch:
    episode_counts = {episode_id: 0 for episode_id in P5_EPISODE_IDS}
    signals: list[P5Signal] = []
    for position in range(1, P5_SIGNALS_PER_BATCH + 1):
        episode_id = P5_EPISODE_IDS[(position - 1) % len(P5_EPISODE_IDS)]
        sequence = episode_counts[episode_id]
        episode_counts[episode_id] += 1
        is_target = position == _TARGET_POSITION[batch_number]
        text = (
            _TARGET_TEXT[batch_number]
            if is_target
            else _NOISE[(batch_number, episode_id)][sequence]
        )
        channel, space = _source(episode_id)
        signals.append(
            P5Signal(
                signal_id=f"p5-b{batch_number}-s{position:02d}",
                batch_number=batch_number,
                position=position,
                episode_id=episode_id,
                source_channel=channel,
                source_space=space,
                text=text,
            )
        )
    return P5Batch(batch_number=batch_number, signals=tuple(signals))


def build_p5_population() -> P5Population:
    batches = tuple(_build_batch(batch_number) for batch_number in range(1, 4))
    oracle = P5VerticalOracle(
        atomic_signal_id="p5-b1-s13",
        reuse_relation_signal_id="p5-b2-s10",
        correction_signal_id="p5-b3-s16",
        expected_relation_kind="dependency_constraint",
    )
    payload = {
        "version": P5_POPULATION_VERSION,
        "batches": [asdict(batch) for batch in batches],
        "oracle": asdict(oracle),
    }
    return P5Population(
        version=P5_POPULATION_VERSION,
        batches=batches,
        oracle=oracle,
        population_digest=canonical_sha256(payload),
    )


__all__ = [
    "CENTRAL_EPISODE",
    "CUSTOMER_EPISODE",
    "P5_BATCH_COUNT",
    "P5_EPISODE_IDS",
    "P5_POPULATION_VERSION",
    "P5_SIGNAL_COUNT",
    "P5_SIGNALS_PER_BATCH",
    "P5Batch",
    "P5Population",
    "P5Signal",
    "P5VerticalOracle",
    "SECURITY_EPISODE",
    "build_p5_population",
]
