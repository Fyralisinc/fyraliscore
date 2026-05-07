"""Category 1 — Linguistic adversarial inputs.

The Reasoning Engine reads natural language. Production text is messy
(sarcasm, hedging, code-switching, typos, conditional commitments).
The existing harness uses clean declarative single-clause English.
These scenarios hit the substrate with real-feeling workplace voice
to find extraction failure modes.

All cases drive the live Think pipeline (DeepSeek). Each case skips
cleanly if no LLM provider is configured, so HARNESS_SKIP_LLM=1 keeps
the suite fast for local iteration. When the LLM IS available, the
scenarios will likely surface real bugs — most are written as
underspecified-with-strong-evidence: we know what the engine SHOULD do
on the strongest interpretation; if it does something different, that's
the finding.

Domain spread (deliberate): sales, eng, finance, hiring, support,
leadership, product. Avoiding over-index on customer-renewal.
"""
from __future__ import annotations

import asyncpg

from .. import _fixtures as F
from .._runner import Case
from . import _helpers as H


# =====================================================================
# Skip helper — every linguistic case shares the same skip path.
# =====================================================================


async def _setup_with_actor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    if not H.llm_available():
        return {"skip": True}
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant, display_name="Engineer")
            return {
                "tenant": tenant,
                "actor": actor,
                "skip": False,
            }


def _skipped(actual: dict) -> bool:
    return actual.get("skipped") is True


# =====================================================================
# L1 — Sarcasm: surface polarity inverts intent
# =====================================================================
# "Oh great, ACME loves us so much they haven't replied in three weeks"
# A naive extractor reads "loves us" and emits a positive-sentiment
# state Model. The substrate should produce a churn-risk concern,
# not a positive signal.


async def _run_sarcasm(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "Oh great, ACME just loves us so much they haven't replied to a "
        "single email in three weeks. Real engagement there. Anyway "
        "QBR is supposedly next month."
    )
    return await H.run_think_with_text(
        pool,
        tenant_id=ctx["tenant"],
        actor_id=ctx["actor"],
        content_text=text,
        seed_text="ACME silent for three weeks; QBR next month",
    )


def _assert_sarcasm(actual: dict, _expected: dict, _ctx: dict) -> tuple[bool, str]:
    if _skipped(actual):
        return True, "skipped (no LLM)"
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["status"] not in ("success", "skipped_idempotent"):
        return False, f"think outcome: {actual['status']}"
    naturals = " | ".join(actual.get("model_naturals", [])).lower()
    kinds = actual.get("model_kinds", [])
    # Concern OR a negative-state Model is acceptable. A pure
    # positive-state Model is wrong.
    has_concern = "concern" in kinds
    has_negative_signal = any(
        word in naturals
        for word in ("risk", "concern", "silent", "no reply",
                     "unresponsive", "disengag", "churn", "stall")
    )
    if has_concern or has_negative_signal:
        return True, ""
    return False, (
        f"sarcasm read as positive; naturals={actual.get('model_naturals')!r}"
    )


CASE_SARCASM = Case(
    stage="adversarial.linguistic",
    name="sarcasm_inverted_polarity",
    intent="Sarcastic 'they love us'-style phrasing with a negative event "
           "must produce a concern, not a positive-sentiment state",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_sarcasm),
    expected=lambda _ctx: {},
    assertion=_assert_sarcasm,
    failure_mode_under_test=(
        "extractor reads surface polarity ('loves us') and emits a "
        "positive Model despite a clearly negative event ('silent for "
        "three weeks')"
    ),
    expected_behavior="specified",
    domain="sales",
)


# =====================================================================
# L2 — Double negation
# =====================================================================
# "I don't think we shouldn't ship by Q3" — double negative reduces
# to a commitment-leaning signal. The engine has to handle the
# negation arithmetic correctly.


async def _run_double_neg(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "Look, I don't think we shouldn't be shipping the v2 export pipeline "
        "by Q3. Eng leadership has been clear it's a priority. Let's not "
        "underdeliver on this one."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text,
        seed_text="v2 export pipeline Q3 ship discussion",
    )


def _assert_double_neg(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if _skipped(actual):
        return True, "skipped (no LLM)"
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    naturals = " | ".join(actual.get("model_naturals", [])).lower()
    # Anti-pattern: extracted as "we shouldn't ship" (negation flip).
    if any(p in naturals for p in (
        "should not ship", "shouldn't ship", "won't ship",
        "not shipping", "do not ship",
    )):
        return False, f"negation flipped: naturals={actual.get('model_naturals')}"
    return True, ""


CASE_DOUBLE_NEG = Case(
    stage="adversarial.linguistic",
    name="double_negation_does_not_flip_polarity",
    intent="Double negation 'don't think we shouldn't ship' must NOT "
           "extract as a 'we will not ship' claim",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_double_neg),
    expected=lambda _ctx: {},
    assertion=_assert_double_neg,
    failure_mode_under_test=(
        "naive negation handling reads only the inner 'shouldn't' and "
        "extracts a 'we will not ship' commitment"
    ),
    expected_behavior="specified",
    domain="engineering",
)


# =====================================================================
# L3 — Conditional commitment
# =====================================================================
# "If they renew, we'll ship the integration by Q3." — is this a
# commitment? Architecturally underspecified.


async def _run_conditional(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "If Pelago renews their contract by end of August, we'll ship the "
        "Stripe integration by Q3. Otherwise we deprioritize."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="Pelago Stripe integration conditional",
    )


CASE_CONDITIONAL = Case(
    stage="adversarial.linguistic",
    name="conditional_commitment_handling",
    intent="A conditional commitment ('if X then we ship') should NOT be "
           "stored as an unconditional commitment Node",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_conditional),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine emits an unconditional commitment Node ignoring the "
        "'if Pelago renews' precondition; downstream consumers treat it "
        "as a hard ship date"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "How should the substrate represent conditional commitments? "
        "Options: (a) Hypothesis Node with test_conditions, "
        "(b) Commitment with a special precondition field, "
        "(c) drop entirely until the condition resolves, "
        "(d) Pattern Node. The codebase has no documented choice."
    ),
    domain="sales",
)


# =====================================================================
# L4 — Tense ambiguity ("we're shipping")
# =====================================================================


async def _run_tense(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "We're shipping the migration tool. Status is good. Trent owns it."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="migration tool ship status",
    )


CASE_TENSE_AMBIG = Case(
    stage="adversarial.linguistic",
    name="tense_ambiguity_present_continuous",
    intent="'We're shipping X' is ambiguous: is it shipping now, or "
           "as part of normal work, or future-tense?",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_tense),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine commits to one tense reading without recording "
        "uncertainty; downstream calibration overconfident"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "When tense is genuinely ambiguous, should the engine: "
        "(a) commit to one reading at full confidence, "
        "(b) emit at lower confidence with falsifier 'observed shipped event', "
        "(c) emit two competing Models, or (d) emit a hypothesis? "
        "Pick a convention so calibration tracks reality."
    ),
    domain="engineering",
)


# =====================================================================
# L5 — Code-switched signal (English + Spanish)
# =====================================================================


async def _run_code_switch(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "Hey team, el cliente ACME está super molesto with the latency. "
        "Nos pidieron a fix by Friday or escalating to legal. "
        "Dale prioridad maxima please."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="ACME urgent latency escalation",
    )


CASE_CODE_SWITCH = Case(
    stage="adversarial.linguistic",
    name="code_switched_english_spanish",
    intent="Code-switched signal must extract critical facts (deadline, "
           "escalation threat) regardless of language interleave",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_code_switch),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine extracts only English clauses, missing 'Friday or "
        "escalating to legal' which is the critical Spanish phrase"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Is multilingual extraction in scope? If yes, document supported "
        "languages; if no, document a fallback (drop, flag, or translate "
        "before extraction)."
    ),
    domain="customer_support",
)


# =====================================================================
# L6 — Typos and autocorrect failures
# =====================================================================


async def _run_typos(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "We comitted to ship the dadhboard by Q3 but Sarah said we wont "
        "be able to deliver becuase the auth migartion is blcoked."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="dashboard Q3 commitment auth blocker",
    )


def _assert_typos(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if _skipped(actual):
        return True, "skipped"
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual.get("model_count", 0) == 0:
        return False, (
            "no Models produced — engine should be robust to typos"
        )
    return True, ""


CASE_TYPOS = Case(
    stage="adversarial.linguistic",
    name="typos_and_autocorrect_failures",
    intent="Typos ('comitted', 'dadhboard', 'migartion') must not "
           "prevent extraction entirely",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_typos),
    expected=lambda _ctx: {},
    assertion=_assert_typos,
    failure_mode_under_test=(
        "engine fails to extract anything because keyword matching "
        "or strict NER is brittle to typos"
    ),
    expected_behavior="specified",
    domain="engineering",
)


# =====================================================================
# L7 — Quoted speech (whose claim is it?)
# =====================================================================


async def _run_quoted(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "John from sales said 'we're killing the ACME pilot — they aren't "
        "going to renew anyway'. He's been pretty negative this week so I'm "
        "not sure we should treat that as final."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="John quote on ACME pilot",
    )


CASE_QUOTED_SPEECH = Case(
    stage="adversarial.linguistic",
    name="quoted_speech_attribution",
    intent="Engine must distinguish 'John said X' from a first-person "
           "claim of X by the speaker",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_quoted),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine emits a high-confidence Decision Node 'kill ACME pilot' "
        "attributed to the speaker, when the actual content is John's "
        "report-of-John's-decision plus the speaker's hedging"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "How does the substrate handle reported speech? Options: "
        "(a) attribute to the quoted speaker (requires actor lookup); "
        "(b) emit at lower confidence with raised_by=John; "
        "(c) drop entirely. No documented convention."
    ),
    domain="sales",
)


# =====================================================================
# L8 — Reported decisions ("apparently leadership decided…")
# =====================================================================


async def _run_reported_dec(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "Apparently leadership decided last Thursday to drop the Beta Corp "
        "account — I heard it secondhand from Mike, who heard it from "
        "Sarah's standup. Has anyone seen this in writing?"
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="Beta Corp drop secondhand report",
    )


def _assert_reported_dec(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if _skipped(actual):
        return True, "skipped"
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    # Look for a Decision Node landing at high confidence.
    high_conf_decision = any(
        m["proposition_kind"] in ("state", "decision")
        and float(m.get("confidence", 0.0)) >= 0.8
        for m in actual.get("models", [])
    )
    if high_conf_decision:
        return False, (
            "secondhand report produced a high-confidence Decision/state "
            "Model — provenance discount missing"
        )
    return True, ""


CASE_REPORTED_DEC = Case(
    stage="adversarial.linguistic",
    name="reported_decision_secondhand",
    intent="Secondhand reported decision must NOT produce a "
           "high-confidence Decision/state Node",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_reported_dec),
    expected=lambda _ctx: {},
    assertion=_assert_reported_dec,
    failure_mode_under_test=(
        "engine treats 'apparently leadership decided' as authoritative "
        "and emits a high-confidence Decision; the speaker's own hedging "
        "('I heard it secondhand', 'has anyone seen this in writing') is "
        "lost"
    ),
    expected_behavior="specified",
    domain="leadership",
)


# =====================================================================
# L9 — Aspirational vs committed phrasing
# =====================================================================


async def _run_aspirational(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "We'd love to ship multi-region by Q4. We're targeting Q4. But "
        "honestly we'll ship it when it's ready and the team has bandwidth."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="multi-region Q4 aspirational",
    )


CASE_ASPIRATIONAL = Case(
    stage="adversarial.linguistic",
    name="aspirational_versus_committed",
    intent="'We'd love to ship by Q4' must NOT be extracted as a "
           "Q4 commitment with high confidence",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_aspirational),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine flattens 'we'd love to ship', 'targeting', and 'when "
        "ready' into a single Q4 commitment, losing the aspirational "
        "vs committed distinction"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Define 'aspirational' vs 'committed' substrate semantics. "
        "Should aspirational be a Hypothesis Node, a Goal, a low-confidence "
        "Commitment, or dropped? Production uses 'we're targeting' "
        "constantly without intent-to-commit."
    ),
    domain="leadership",
)


# =====================================================================
# L10 — Reply / threading without context
# =====================================================================


async def _run_threading(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "+1 to what Sarah said earlier. Should we just kill the project "
        "then? Yeah, agreed."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="reply +1 kill project",
    )


CASE_THREADING = Case(
    stage="adversarial.linguistic",
    name="threading_reply_no_context",
    intent="A reply ('+1 to what Sarah said', 'should we kill it', "
           "'agreed') without the parent message must NOT produce "
           "a confident decision",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_threading),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine reads 'kill the project' standalone and emits a "
        "high-confidence kill Decision; the reply's dependence on "
        "'what Sarah said earlier' is dropped"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should the engine refuse to extract from clearly-truncated "
        "reply context, or extract at low confidence with a falsifier "
        "tied to the parent message? Slack/email signals are mostly "
        "replies in the wild."
    ),
    domain="engineering",
)


# =====================================================================
# L11 — Hedged commitment without explicit deadline
# =====================================================================


async def _run_hedge(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "I think we'll probably get to the dashboard work eventually, "
        "maybe sometime this quarter or next, no promises."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="dashboard hedged commitment",
    )


def _assert_hedge(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if _skipped(actual):
        return True, "skipped"
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    # Acceptable: low-confidence Model OR no Model at all.
    high_conf = any(
        float(m.get("confidence", 0.0)) >= 0.7 for m in actual.get("models", [])
    )
    if high_conf:
        return False, "stack of hedges produced a high-confidence Model"
    return True, ""


CASE_HEDGED = Case(
    stage="adversarial.linguistic",
    name="hedged_commitment_low_confidence",
    intent="Stacked hedges ('I think', 'probably', 'eventually', "
           "'maybe', 'no promises') must not produce a >=0.7 confidence Model",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_hedge),
    expected=lambda _ctx: {},
    assertion=_assert_hedge,
    failure_mode_under_test=(
        "engine ignores hedge stack and emits Commitment at default "
        "(>=0.6-0.7) confidence — calibration drift on the optimistic side"
    ),
    expected_behavior="specified",
    domain="engineering",
)


# =====================================================================
# L12 — Sarcastic reversal of stated intent
# =====================================================================


async def _run_sarcastic_reversal(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "Sure, that will *definitely* happen on time. Just like the last "
        "five deadlines we hit. Anyway, I'm putting the launch at risk."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="launch risk sarcastic reversal",
    )


CASE_SARCASTIC_REVERSAL = Case(
    stage="adversarial.linguistic",
    name="sarcastic_reversal_definitely",
    intent="'Sure, that will definitely happen' followed by sarcastic "
           "context must produce a launch-risk concern, not a confident "
           "on-time prediction",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_sarcastic_reversal),
    expected=lambda _ctx: {},
    assertion=_assert_sarcasm,
    failure_mode_under_test=(
        "literal extractor emits 'launch will happen on time' as a "
        "high-confidence Prediction; the 'just like the last five "
        "deadlines we hit' sarcastic frame is the actual claim"
    ),
    expected_behavior="specified",
    domain="engineering",
)


# =====================================================================
# L13 — Bot / system-actor signal
# =====================================================================


async def _run_bot_signal(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "[CI bot] Build #4711 failed. 3 retries exhausted. "
        "Test: test_payment_capture_idempotency. "
        "Owner: @payment-team. Last passing: 4 days ago."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="CI build 4711 failure payment",
    )


CASE_BOT_SIGNAL = Case(
    stage="adversarial.linguistic",
    name="bot_system_actor_signal",
    intent="Bot-authored signal (CI failure) — should this produce a "
           "Concern Node, an Observation only, or an Act-layer change?",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_bot_signal),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine treats bot CI signal as a human-style claim and emits "
        "a Concern with raised_by=harness, dropping the actor-context "
        "that would let downstream consumers route this to oncall"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "What's the Substrate's stance on bot/system actors? Options: "
        "(a) emit Models normally (current), (b) emit at lower trust "
        "tier with bot-aware falsifier, (c) skip Model emission and "
        "only enqueue T1 triggers, (d) emit a Pattern Node. Need a "
        "documented choice."
    ),
    domain="engineering",
)


# =====================================================================
# L14 — Ambiguous entity reference (two ACMEs)
# =====================================================================


async def _run_ambig_entity(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "ACME is going to churn this quarter. Pricing pushback came up "
        "again on the call. Also, ACME on the engineering side is fine — "
        "those are different ACMEs by the way, the customer ACME and our "
        "internal ACME tooling team."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="ACME ambiguous reference",
    )


CASE_AMBIG_ENTITY = Case(
    stage="adversarial.linguistic",
    name="ambiguous_entity_reference",
    intent="Same string 'ACME' refers to two different entities; "
           "the substrate must not collapse them into one scope",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_ambig_entity),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "engine emits one churn Concern scoped to a single 'ACME' "
        "actor, conflating the customer ACME with the internal tooling "
        "team — region locks would then collide"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "How is entity disambiguation handled? Currently the engine "
        "must guess from context; with no actor catalogue lookup at "
        "extraction time, this is structurally unsolvable. Document "
        "the design trade-off."
    ),
    domain="sales",
)


# =====================================================================
# L15 — Aspirational + conditional + sarcastic compound
# =====================================================================


async def _run_compound_linguistic(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    text = (
        "Look, if leadership EVER decides to actually fund this team, "
        "we'd love to maybe ship Q4 multi-region. *Realistically*. "
        "But hey, what's another delay? Let me know if you're not not "
        "interested."
    )
    return await H.run_think_with_text(
        pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
        content_text=text, seed_text="compound sarcastic aspirational",
    )


CASE_COMPOUND_LINGUISTIC = Case(
    stage="adversarial.linguistic",
    name="compound_linguistic_pressure",
    intent="Compound (sarcasm + double negation + conditional + "
           "aspirational) — engine should produce nothing high-confidence",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_compound_linguistic),
    expected=lambda _ctx: {},
    assertion=_assert_hedge,  # any Model > 0.7 is a fail
    failure_mode_under_test=(
        "linguistic features stack and the engine 'simplifies' by "
        "picking one literal reading; calibration breaks under "
        "compound hedging"
    ),
    expected_behavior="specified",
    domain="leadership",
)


CASES = [
    CASE_SARCASM,
    CASE_DOUBLE_NEG,
    CASE_CONDITIONAL,
    CASE_TENSE_AMBIG,
    CASE_CODE_SWITCH,
    CASE_TYPOS,
    CASE_QUOTED_SPEECH,
    CASE_REPORTED_DEC,
    CASE_ASPIRATIONAL,
    CASE_THREADING,
    CASE_HEDGED,
    CASE_SARCASTIC_REVERSAL,
    CASE_BOT_SIGNAL,
    CASE_AMBIG_ENTITY,
    CASE_COMPOUND_LINGUISTIC,
]
