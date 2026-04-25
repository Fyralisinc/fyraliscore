"""Signal generators: template-based (deterministic) and LLM (stubbed)."""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Optional, Protocol, runtime_checkable

from lsob_contracts import Signal, SourceChannel

from lsob_simulation.state import ActorState, CommitmentState, CustomerState


# --------------------------- Template library --------------------------------

# Channel-specific templates indexed by (trigger_kind, persona_tone).
# Each template references placeholders that the generator substitutes.
_SLACK_CHATTER: dict[str, list[str]] = {
    "start": [
        "Kicking off {commitment}. Hoping to wrap by +{days}d.",
        "Starting {commitment} today. Should be ~{days} days.",
        "Took on {commitment}. My gut says {days}d.",
    ],
    "progress": [
        "Made progress on {commitment}. ~{pct}% done.",
        "{commitment} coming along. ~{pct}% through it.",
        "Update on {commitment}: about {pct}% done.",
    ],
    "slip": [
        "Hit a snag on {commitment}. Probably slipping.",
        "Realistically {commitment} is going to take longer.",
        "Flagging: {commitment} is behind.",
    ],
    "done": [
        "{commitment} is done. Finally.",
        "Shipped {commitment}. Closing it out.",
        "Merged and deployed {commitment}.",
    ],
    "customer": [
        "Customer {customer} raising issues again.",
        "{customer} is getting restless — need to pay attention.",
        "Heads up: {customer} escalated a support ticket.",
    ],
}

_EMAIL_BODIES: dict[str, list[str]] = {
    "progress": [
        "Brief weekly update on {commitment}: we're tracking at roughly {pct}% completion. Next milestone remains {days} days out.",
        "Status on {commitment}: approximately {pct}% complete; on pace for delivery.",
    ],
    "slip": [
        "Quick note — {commitment} is likely to slip beyond its original window. Will share revised timeline shortly.",
        "Flag: {commitment} is behind plan; expect +{days}d delay.",
    ],
    "customer": [
        "Prep note for {customer} review meeting: health trajectory has softened; see attached.",
        "{customer} touchpoint — please review before the call.",
    ],
}

_PR_BODIES: dict[str, list[str]] = {
    "start": [
        "PR: scaffolding for {commitment}.",
        "PR: initial skeleton of {commitment} worker.",
    ],
    "progress": [
        "PR update: addressing review comments on {commitment}.",
        "PR: still iterating on {commitment} edge cases.",
    ],
    "done": [
        "PR merged: {commitment} is live.",
        "PR closed: {commitment} shipped to prod.",
    ],
}

_DOC_BODIES: dict[str, list[str]] = {
    "progress": [
        "Design doc updated for {commitment}. Open questions: dependency ordering.",
        "Spec notes for {commitment}: trade-offs captured.",
    ],
    "customer": [
        "Account plan for {customer} — updated health view.",
    ],
}

_CAL_BODIES: dict[str, list[str]] = {
    "progress": [
        "Calendar: Sync on {commitment} scheduled for tomorrow.",
    ],
    "customer": [
        "Calendar: {customer} QBR prep block added.",
    ],
}

_TICKET_BODIES: dict[str, list[str]] = {
    "customer": [
        "Ticket opened by {customer}: dashboard latency regression.",
        "Ticket from {customer}: P1 on ingest.",
    ],
    "slip": [
        "Ticket re: {commitment} — blocker flagged by QA.",
    ],
}

_TEMPLATE_INDEX: dict[SourceChannel, dict[str, list[str]]] = {
    SourceChannel.slack: _SLACK_CHATTER,
    SourceChannel.email: _EMAIL_BODIES,
    SourceChannel.pr: _PR_BODIES,
    SourceChannel.doc: _DOC_BODIES,
    SourceChannel.calendar: _CAL_BODIES,
    SourceChannel.ticket: _TICKET_BODIES,
}


# --------------------------- Protocol & impls -------------------------------

@runtime_checkable
class SignalGenerator(Protocol):
    """Abstract producer of Signals. Implementations may be deterministic or LLM-backed."""

    def generate(
        self,
        *,
        actor: ActorState,
        tick: int,
        timestamp: datetime,
        rng: random.Random,
        commitment: CommitmentState | None = None,
        customer: CustomerState | None = None,
        channel: SourceChannel,
        trigger_kind: str,
        signal_id: str,
    ) -> Signal:
        ...


class TemplateSignalGenerator:
    """Deterministic signal generator using seeded random + templates.

    Varies text by source_channel, commitment state, and actor persona (tone hints).
    """

    def generate(
        self,
        *,
        actor: ActorState,
        tick: int,
        timestamp: datetime,
        rng: random.Random,
        commitment: CommitmentState | None = None,
        customer: CustomerState | None = None,
        channel: SourceChannel,
        trigger_kind: str,
        signal_id: str,
    ) -> Signal:
        family = _TEMPLATE_INDEX.get(channel, _SLACK_CHATTER)
        templates = family.get(trigger_kind) or next(iter(family.values()))
        template = templates[rng.randrange(len(templates))]
        commitment_label = commitment.truth.commitment_id if commitment else "the project"
        pct = int((commitment.true_progress if commitment else 0.0) * 100)
        days = commitment.truth.asserted_duration_days if commitment else 7
        customer_label = customer.truth.customer_id if customer else "the account"
        body = template.format(
            commitment=commitment_label,
            pct=max(5, min(95, pct)),
            days=days,
            customer=customer_label,
        )
        # Persona voice. Append a small marker by persona archetype.
        tone = _persona_tone(actor)
        if tone and channel == SourceChannel.slack and rng.random() < 0.45:
            body = f"{body} {tone}"
        # Optimists under-report slips; pessimists amplify.
        if actor.persona.estimation_bias > 0.25 and trigger_kind == "slip" and rng.random() < 0.5:
            body = body.replace("slipping", "slightly delayed").replace("behind", "a touch late")
        if actor.persona.estimation_bias < -0.25 and trigger_kind == "progress" and rng.random() < 0.4:
            body = body + " (not confident in this estimate)"

        metadata: dict[str, object] = {
            "tick": tick,
            "trigger_kind": trigger_kind,
            "actor_id": actor.persona.actor_id,
        }
        if commitment is not None:
            metadata["commitment_ref"] = commitment.truth.commitment_id
            metadata["true_progress"] = round(commitment.true_progress, 3)
            metadata["perceived_progress"] = round(commitment.perceived_progress, 3)
        if customer is not None:
            metadata["customer_ref"] = customer.truth.customer_id
            metadata["customer_health"] = customer.current_health
        return Signal(
            signal_id=signal_id,
            source_channel=channel,
            author_id=actor.persona.actor_id,
            content_text=body,
            timestamp=timestamp,
            metadata=metadata,
        )


def _persona_tone(actor: ActorState) -> str:
    """Short persona-flavored tail token, stable per persona class."""
    bias = actor.persona.estimation_bias
    rel = actor.persona.reliability_parameter
    if bias > 0.3 and rel < 0.6:
        return "🤞"  # flaky optimist
    if bias > 0.3:
        return "— we've got this."
    if bias < -0.3:
        return "— cautiously."
    if rel > 0.85:
        return "— tracking."
    return ""


class LLMSignalGenerator:
    """Structural stub for an Anthropic-backed generator.

    NOTE: This class is wiring-only. Tests and mini-corpus runs must NOT use this
    implementation; it exists so Phase 2 work can swap it in later. The `anthropic`
    import is lazy to avoid forcing an API key on consumers of the template generator.
    """

    def __init__(self, client: object | None = None, model: str = "claude-haiku-4-5") -> None:
        self._client = client
        self._model = model

    async def generate(
        self,
        *,
        actor: ActorState,
        tick: int,
        timestamp: datetime,
        rng: random.Random,
        commitment: CommitmentState | None = None,
        customer: CustomerState | None = None,
        channel: SourceChannel,
        trigger_kind: str,
        signal_id: str,
    ) -> Signal:  # pragma: no cover - intentional stub
        """Ready to wire; raises if invoked without a real client configured."""
        if self._client is None:
            raise RuntimeError(
                "LLMSignalGenerator.generate() called without a client. "
                "Use TemplateSignalGenerator for tests and mini-corpus runs."
            )
        # Actual API call would go here. We intentionally leave it unimplemented
        # for Phase 1 to avoid external dependencies / API keys in CI.
        raise NotImplementedError(
            "LLMSignalGenerator.generate() is not implemented in Phase 1; "
            "wire in Phase 2 when LLM-backed corpora are needed."
        )
