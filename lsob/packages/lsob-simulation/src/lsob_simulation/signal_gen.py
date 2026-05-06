"""Signal generators: template-based (deterministic) and LLM (stubbed).

Two template paths:

  - **Legacy path** — actors with auto-generated names (e.g. "Reliable 0",
    role from the 9-element role cycle) get the original templates. Existing
    CompanyA/B/C corpora stay byte-identical.
  - **Rich path** — when an actor has `role_family` set on its persona
    (i.e. it came from an `ActorProfile` in `actor_profiles`), templates
    reference the actor by first name, the customer by `company_name`,
    the commitment by its `title`, and occasionally mention a peer drawn
    from the simulator's `peers` list.

Both paths are deterministic given the same RNG seed.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Optional, Protocol, runtime_checkable

from lsob_contracts import Signal, SourceChannel

from lsob_simulation.state import ActorState, CommitmentState, CustomerState


# =====================================================================
# LEGACY TEMPLATE BANK (do not modify — CompanyA/B/C tests depend on it)
# =====================================================================

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


# =====================================================================
# RICH TEMPLATE BANK (used when actor.persona.role_family is set)
# =====================================================================
#
# Placeholders available:
#   {author}        — actor first name
#   {commitment}    — commitment.truth.title (falls back to id)
#   {customer}      — customer.truth.company_name (falls back to id)
#   {peer}          — first name of a randomly-selected peer (if any)
#   {pct}, {days}   — same as legacy
#
# Banks are keyed by role_family → channel → trigger.

_RICH_ENG_SLACK: dict[str, list[str]] = {
    "start": [
        "Kicking off {commitment} this morning — thinking ~{days} days end-to-end.",
        "Picking up {commitment}. Should land in {days}d if no surprises.",
        "Branched off main for {commitment}. {days}d is my honest guess.",
        "Starting on {commitment}. Will sync with {peer} on the API surface.",
    ],
    "progress": [
        "{commitment} is at ~{pct}%. Mostly the wiring left.",
        "Halfway-ish on {commitment}. Edge cases biting harder than I expected.",
        "Update: {commitment} ~{pct}% done. Need {peer}'s eyes on the migration step.",
        "Pushed another commit on {commitment}. About {pct}% there.",
        "Tests passing locally for {commitment} — staging deploy next.",
        "Refactored the {commitment} worker — feels cleaner now.",
    ],
    "slip": [
        "{commitment} is going to slip — found a regression in the dependency graph.",
        "Realistically {commitment} needs another {days} days. Calling it now.",
        "Flagging: {commitment} is blocked on the {peer}-owned change. Sync needed.",
        "Hit a wall on {commitment}. Going to need to redesign the queue layer.",
    ],
    "done": [
        "{commitment} is merged and out the door. Moving on.",
        "Shipped {commitment} to prod, no incidents on the canary.",
        "{commitment} is closed. {peer}, your review caught the off-by-one — thanks.",
        "Done with {commitment}. Will write the postmortem note tomorrow.",
    ],
    "customer": [
        "Heads up — saw {customer} hit a 500 on the dashboard. Looking now.",
        "Picking up the {customer} ticket. Looks reproducible.",
    ],
}

_RICH_ENG_PR: dict[str, list[str]] = {
    "start": [
        "PR opened: scaffolding for {commitment}. WIP, no review yet.",
        "Draft PR for {commitment} — sharing early for shape feedback from {peer}.",
        "PR: initial implementation of {commitment}. Tests stubbed.",
    ],
    "progress": [
        "PR update on {commitment}: addressing review notes from {peer}.",
        "Pushed iteration #3 on {commitment}. Edge cases now covered.",
        "PR for {commitment}: rebased on main, tests green.",
    ],
    "done": [
        "Merged: {commitment} ({pct}% behind plan but landing today).",
        "PR closed: {commitment} shipped. Rolling out behind the flag.",
    ],
}

_RICH_ENG_DOC: dict[str, list[str]] = {
    "progress": [
        "Updated the design doc for {commitment} with the dependency-ordering question.",
        "Notes from the {commitment} sync: scope locked, trade-offs captured.",
        "Spec for {commitment} — added the migration runbook section.",
    ],
}

_RICH_ENG_EMAIL: dict[str, list[str]] = {
    "progress": [
        "Weekly update on {commitment}: tracking at ~{pct}%, on pace for the {days}-day target.",
        "Status: {commitment} ~{pct}% complete. {peer} unblocked the data-layer piece.",
    ],
    "slip": [
        "Hi — quick flag that {commitment} is going to slip. New target is +{days}d. Will share root cause in standup.",
    ],
}

_RICH_SALES_SLACK: dict[str, list[str]] = {
    "progress": [
        "{customer} call went well — they want a follow-up on the forecast accuracy claim.",
        "Pipeline update: {customer} moved to procurement. Slow but real.",
        "Pinged {customer} on the renewal date — waiting for their finance team.",
        "Demo with {customer} prep — anyone seen the latest dashboard screenshots?",
    ],
    "customer": [
        "{customer} pushed back on pricing — need to escalate to {peer}.",
        "Flagging: {customer} is going dark on emails. Three messages no reply.",
        "{customer} is asking for an SSO feature before they sign. Not in scope yet.",
        "{customer} called the support line — security review concerns. Routing to {peer}.",
        "{customer} renewal at risk — they're talking to a competitor.",
    ],
    "done": [
        "Closed: {customer} signed the order form. ${pct}K ARR.",
        "{customer} renewal in. Took longer than I'd hoped.",
    ],
}

_RICH_SALES_EMAIL: dict[str, list[str]] = {
    "customer": [
        "Notes from the {customer} call: they're evaluating us against two others. Decision in {days}d.",
        "{customer} prep — please review the proposal before the QBR. {peer} flagged the procurement angle.",
        "Following up on the {customer} ask around forecast attribution. Will need eng input from {peer}.",
    ],
    "progress": [
        "Pipeline: {customer} stage moved to legal review. Expecting close in {days}d.",
    ],
}

_RICH_CS_SLACK: dict[str, list[str]] = {
    "customer": [
        "{customer} health check this morning — usage flat for two weeks.",
        "Heads up: {customer} just opened their fourth ticket on Salesforce sync this month.",
        "{customer} not showing up to weekly syncs. Re-engaging via {peer}.",
        "QBR prep for {customer} — {peer} can you review the deck?",
        "{customer} executive sponsor changed. New person hasn't logged in yet.",
        "Flagging churn risk on {customer} — usage drift past 30 days.",
    ],
    "progress": [
        "{customer} adoption is picking up — weekly active up {pct}%.",
        "Onboarding for {customer}: completed the data integration step today.",
    ],
    "done": [
        "{customer} renewal closed. Kept the multi-year discount.",
    ],
}

_RICH_CS_EMAIL: dict[str, list[str]] = {
    "customer": [
        "Hi {peer} — wanted to flag that {customer} has been quiet. Three weeks since last touchpoint. Plan to re-engage.",
        "Health summary for {customer}: amber. Usage trending down, exec turnover. Recommending a save play.",
        "{customer} QBR notes attached — they're asking for ICP scoring as a renewal condition.",
    ],
    "progress": [
        "{customer} weekly: adoption ~{pct}% of seats active, on track for healthy renewal.",
    ],
}

_RICH_FOUNDER_SLACK: dict[str, list[str]] = {
    "progress": [
        "Quick FYI — {commitment} is the current critical path. Leaning on {peer} for delivery.",
        "Board update prep — pulling latest numbers on {customer} and {commitment}.",
        "Talked to {customer} this morning. They want a path to ICP scoring. {peer}, can we sync?",
        "Spending the day on hiring — VP Eng search continues.",
        "Reviewing the roadmap with {peer} — too many parallel workstreams.",
    ],
    "slip": [
        "{commitment} is going to slip. I'd rather we slip cleanly than ship something half-baked.",
        "Flagging to the team: {commitment} pushed by {days}d. Communicating to design partners today.",
    ],
    "customer": [
        "Just got off a call with {customer}. They're considering churning. Need a save play this week.",
        "{customer} escalated to me directly. Looping {peer}.",
    ],
}

_RICH_FOUNDER_EMAIL: dict[str, list[str]] = {
    "progress": [
        "Team — weekly update: {commitment} progressing at {pct}%, {customer} moving through procurement, hiring tracking against plan.",
        "Quick thoughts after the {customer} meeting: we should re-scope {commitment} to address their feedback. {peer}, let's discuss.",
    ],
    "slip": [
        "Heads-up note to investors: anticipating {days}d slip on {commitment}. Will brief in the next monthly.",
    ],
    "customer": [
        "Brief: {customer} expansion conversation is live. Need {peer}'s input on the technical scope.",
    ],
}

_RICH_PRODUCT_SLACK: dict[str, list[str]] = {
    "progress": [
        "{commitment} spec update — incorporated {peer}'s feedback on the data model.",
        "Roadmap review: {commitment} stays in this quarter, {customer} feedback validated.",
        "PRD for {commitment} ready for review. {peer} please take a pass.",
    ],
    "slip": [
        "Re-scoping {commitment} based on {customer} feedback. Will reduce the surface area.",
    ],
    "customer": [
        "{customer} discovery notes: their forecast accuracy is suffering on multi-product deals.",
        "Three customers ({customer} included) all asking for the same thing. Worth productizing.",
    ],
}

_RICH_DESIGN_SLACK: dict[str, list[str]] = {
    "progress": [
        "{commitment} mocks updated. {peer} please take a pass before I post in the channel.",
        "User testing notes from {customer} — the new flow tested well. Iterating on edge cases.",
    ],
}

_RICH_OPS_SLACK: dict[str, list[str]] = {
    "progress": [
        "Vendor renewal due in {days}d for the {commitment} stack.",
        "Pulling together the offsite logistics — {peer}, can you confirm the headcount?",
    ],
}

_RICH_FINANCE_SLACK: dict[str, list[str]] = {
    "progress": [
        "Burn update: tracking ~{pct}% of plan. Runway model attached.",
        "{customer} invoice cleared. AR aging looks fine.",
    ],
}

_RICH_PEOPLE_SLACK: dict[str, list[str]] = {
    "progress": [
        "Pipeline for the open eng role: {pct} candidates this week, two strong.",
        "{peer}'s offer goes out today — fingers crossed.",
    ],
}

_RICH_BANKS: dict[str, dict[SourceChannel, dict[str, list[str]]]] = {
    "engineering": {
        SourceChannel.slack: _RICH_ENG_SLACK,
        SourceChannel.pr: _RICH_ENG_PR,
        SourceChannel.doc: _RICH_ENG_DOC,
        SourceChannel.email: _RICH_ENG_EMAIL,
    },
    "data_ml": {
        SourceChannel.slack: _RICH_ENG_SLACK,
        SourceChannel.pr: _RICH_ENG_PR,
        SourceChannel.doc: _RICH_ENG_DOC,
        SourceChannel.email: _RICH_ENG_EMAIL,
    },
    "sales": {
        SourceChannel.slack: _RICH_SALES_SLACK,
        SourceChannel.email: _RICH_SALES_EMAIL,
    },
    "customer_success": {
        SourceChannel.slack: _RICH_CS_SLACK,
        SourceChannel.email: _RICH_CS_EMAIL,
    },
    "founder": {
        SourceChannel.slack: _RICH_FOUNDER_SLACK,
        SourceChannel.email: _RICH_FOUNDER_EMAIL,
    },
    "exec": {
        SourceChannel.slack: _RICH_FOUNDER_SLACK,
        SourceChannel.email: _RICH_FOUNDER_EMAIL,
    },
    "product": {
        SourceChannel.slack: _RICH_PRODUCT_SLACK,
        SourceChannel.email: _RICH_FOUNDER_EMAIL,  # exec-tone is OK
    },
    "design": {
        SourceChannel.slack: _RICH_DESIGN_SLACK,
    },
    "ops": {
        SourceChannel.slack: _RICH_OPS_SLACK,
    },
    "finance": {
        SourceChannel.slack: _RICH_FINANCE_SLACK,
    },
    "legal": {
        SourceChannel.slack: _RICH_OPS_SLACK,
    },
    "people": {
        SourceChannel.slack: _RICH_PEOPLE_SLACK,
    },
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
        peers: list[ActorState] | None = None,
    ) -> Signal:
        ...


def _first_name(full_name: str) -> str:
    parts = full_name.strip().split()
    return parts[0] if parts else full_name


class TemplateSignalGenerator:
    """Deterministic signal generator using seeded random + templates.

    Dispatches between the legacy bank (no role_family on persona) and the
    rich bank (role_family set, e.g. by ActorProfile)."""

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
        peers: list[ActorState] | None = None,
    ) -> Signal:
        rich = actor.persona.role_family is not None
        if rich:
            body = self._render_rich(
                actor=actor, rng=rng, commitment=commitment, customer=customer,
                channel=channel, trigger_kind=trigger_kind, peers=peers or [],
            )
        else:
            body = self._render_legacy(
                actor=actor, rng=rng, commitment=commitment, customer=customer,
                channel=channel, trigger_kind=trigger_kind,
            )
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

    # ------------ legacy ------------

    def _render_legacy(
        self,
        *,
        actor: ActorState,
        rng: random.Random,
        commitment: CommitmentState | None,
        customer: CustomerState | None,
        channel: SourceChannel,
        trigger_kind: str,
    ) -> str:
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
        tone = _persona_tone(actor)
        if tone and channel == SourceChannel.slack and rng.random() < 0.45:
            body = f"{body} {tone}"
        if actor.persona.estimation_bias > 0.25 and trigger_kind == "slip" and rng.random() < 0.5:
            body = body.replace("slipping", "slightly delayed").replace("behind", "a touch late")
        if actor.persona.estimation_bias < -0.25 and trigger_kind == "progress" and rng.random() < 0.4:
            body = body + " (not confident in this estimate)"
        return body

    # ------------ rich ------------

    def _render_rich(
        self,
        *,
        actor: ActorState,
        rng: random.Random,
        commitment: CommitmentState | None,
        customer: CustomerState | None,
        channel: SourceChannel,
        trigger_kind: str,
        peers: list[ActorState],
    ) -> str:
        family_key = actor.persona.role_family or "engineering"
        family_bank = _RICH_BANKS.get(family_key) or _RICH_BANKS["engineering"]
        channel_bank = family_bank.get(channel)
        if channel_bank is None:
            # Fall back to slack bank for channels this family doesn't author often.
            channel_bank = family_bank.get(SourceChannel.slack, _RICH_ENG_SLACK)
        templates = channel_bank.get(trigger_kind)
        if not templates:
            # Try other triggers within the same channel; then any trigger.
            for t in ("progress", "customer", "slip", "done", "start"):
                templates = channel_bank.get(t)
                if templates:
                    trigger_kind = t
                    break
        if not templates:
            templates = next(iter(channel_bank.values()))

        template = templates[rng.randrange(len(templates))]

        commitment_label = (
            (commitment.truth.title or commitment.truth.commitment_id)
            if commitment else "the project"
        )
        pct = int((commitment.true_progress if commitment else 0.0) * 100)
        days = commitment.truth.asserted_duration_days if commitment else 7
        customer_label = (
            (customer.truth.company_name or customer.truth.customer_id)
            if customer else "the account"
        )
        author = _first_name(actor.persona.name)
        peer_label = "the team"
        if peers:
            peer_label = _first_name(peers[rng.randrange(len(peers))].persona.name)

        body = template.format(
            author=author,
            commitment=commitment_label,
            pct=max(5, min(95, pct)),
            days=days,
            customer=customer_label,
            peer=peer_label,
        )

        # Persona tone — same idea as legacy.
        tone = _persona_tone(actor)
        if tone and channel == SourceChannel.slack and rng.random() < 0.35:
            body = f"{body} {tone}"
        # Optimist softening / pessimist hedging.
        if actor.persona.estimation_bias > 0.25 and trigger_kind == "slip" and rng.random() < 0.5:
            body = body.replace("slipping", "slightly delayed").replace("behind", "a touch late")
        if actor.persona.estimation_bias < -0.25 and trigger_kind == "progress" and rng.random() < 0.35:
            body += " (still digesting; estimate is rough)"
        return body


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
        peers: list[ActorState] | None = None,
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
