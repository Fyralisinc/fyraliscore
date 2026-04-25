"""services/rendering/prompts/exemplars.py — few-shot exemplars.

Every exemplar is lifted from either company-os-design.md §10 or
company-os.html (the reference prototype). These are the gold voice
samples. Prompt modules select the subset relevant to their type.

As the voice is calibrated against real substrate output (Phase 5),
these get refined. They are centralised here so every prompt module
improves at once.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Exemplar:
    """One few-shot example: the structured input summary and the
    expected HTML body. The input summary is in the same shape the
    prompt will feed at inference time — structured, terse, grounded."""
    situation: str
    input_summary: str
    html_output: str


# ---------------------------------------------------------------------
# Greeting exemplars
# ---------------------------------------------------------------------

GREETING_EXEMPLARS: tuple[Exemplar, ...] = (
    Exemplar(
        situation="Active-crisis morning. Acme renewal turned unsafe over the weekend.",
        input_summary=(
            "time_of_day_bucket=morning; "
            "top_model=m-2841 'Acme renews Q3' confidence 0.81 → 0.54, "
            "falsifier fired Sun 03:12 (two contracted deliverables slipped); "
            "top_resource=Acme (customer) health warning, revenue_at_risk=$487K; "
            "top_commitment=decide by Thu 24 Apr on Acme scope vs window; "
            "anomaly=silence-in-revenue-channel (0 mentions vs 11 in engineering); "
            "everything-else-is-ok."
        ),
        html_output=(
            "Good morning. One thing is worth your attention before the day "
            "starts \u2014 Acme's renewal is <span class=\"serif\">structurally "
            "unsafe</span> as of Sunday, and revenue hasn't caught it yet. "
            "One decision is on you by Thursday. Everything else is handled."
        ),
    ),
    Exemplar(
        situation="Quiet day. Nothing consequential; company running at normal metabolism.",
        input_summary=(
            "time_of_day_bucket=morning; "
            "top_models=[]; "
            "active_commitments=all on-track; "
            "customer_resources=all healthy; "
            "anomalies=[]; "
            "recent_state_changes=low-volume routine."
        ),
        html_output=(
            "Good morning. Nothing consequential since yesterday; the company "
            "is running at normal metabolism."
        ),
    ),
    Exemplar(
        situation="Late-night check-in. Nothing urgent; hold the Acme update until morning.",
        input_summary=(
            "time_of_day_bucket=late; "
            "conversation_context.was_here_recently=true (11pm check-in); "
            "pending=Acme renewal summary for tomorrow; "
            "anomalies_severity=all low."
        ),
        html_output=(
            "Late check-in. Nothing urgent. I'll hold the Acme update until "
            "tomorrow morning unless you want it now."
        ),
    ),
    Exemplar(
        situation="Afternoon, between meetings. Two things settled since the 8am read.",
        input_summary=(
            "time_of_day_bucket=afternoon; "
            "recent_state_changes=[c-187 Blocked\u2192InProgress Tue 11:20, "
            "m-2841 conf 0.54\u21920.61 after Monica call Tue 13:05]; "
            "top_commitment=none on you today."
        ),
        html_output=(
            "Between meetings? Two things settled this morning. "
            "Acme re-scope is in motion \u2014 c-187 back to <span class=\"n\">"
            "InProgress</span> after Monica's call \u2014 and Model m-2841 "
            "lifted <span class=\"n\">0.54 \u2192 0.61</span>. Nothing on you today."
        ),
    ),
)


# ---------------------------------------------------------------------
# Observation card exemplars
# ---------------------------------------------------------------------

OBSERVATION_CARD_EXEMPLARS: tuple[Exemplar, ...] = (
    Exemplar(
        situation="Acme renewal unsafe; confidence crashed; revenue channel silent.",
        input_summary=(
            "focus_model=m-2841 claim='Acme renews Q3' "
            "confidence_transition=0.81\u21920.54 "
            "falsifier_fired_at=Sun 03:12 "
            "cause='two contracted deliverables slipped (c-187 Blocked, c-203 re-estimated)' "
            "engineering_mentions_since_friday=11 "
            "revenue_mentions_since_friday=0 "
            "customer=Acme revenue_at_risk=$487K"
        ),
        html_output=(
            "Acme's renewal is <span class=\"serif-hot\">structurally unsafe</span>. "
            "Confidence dropped <span class=\"n\">0.81 \u2192 0.54</span> after two "
            "contracted deliverables slipped. Engineering has discussed this "
            "<span class=\"n\">11 times</span> since Friday; the revenue channel "
            "has <span class=\"hl\">zero mentions</span>. Revenue at risk: "
            "<span class=\"n\">$487K</span>."
        ),
    ),
    Exemplar(
        situation="Vertex Labs silence observation.",
        input_summary=(
            "focus_resource=r-cust-vertex kind=customer "
            "touchpoints_last_21_days=0 "
            "expansion_model_transition=0.71\u21920.48 "
            "nothing_negative_surfaced"
        ),
        html_output=(
            "Vertex Labs has gone <span class=\"serif-hot\">quiet</span> for "
            "<span class=\"n\">21 days</span>. Expansion probability drifted "
            "<span class=\"n\">0.71 \u2192 0.48</span> with no touchpoints. "
            "No negative signal \u2014 <span class=\"hl\">the silence itself is the "
            "signal</span>. The account is more worried than you are."
        ),
    ),
)


# ---------------------------------------------------------------------
# Decision card exemplars
# ---------------------------------------------------------------------

DECISION_CARD_EXEMPLARS: tuple[Exemplar, ...] = (
    Exemplar(
        situation="Path A vs Path B on Acme renewal. Decision is on the CEO by Thursday.",
        input_summary=(
            "options=[path_a='re-scope the Acme deliverable', "
            "path_b='extend the renewal window 30 days']; "
            "deadline='Thu 24 Apr'; at_stake='$487K'; "
            "preference=none; "
            "context='Northwind expansion Model would absorb \u22120.06 on path B'"
        ),
        html_output=(
            "<div class=\"card-content\">"
            "<p class=\"dec-text\">Re-scope the Acme deliverable, or "
            "<span class=\"serif\">extend the renewal window</span>. "
            "Drafts for both paths are ready.</p>"
            "<div class=\"dec-chips\">"
            "<span class=\"dec-chip hot\">decide by <b>Thu 24 Apr</b></span>"
            "<span class=\"dec-chip\">at stake <b>$487K</b></span>"
            "</div>"
            "</div>"
        ),
    ),
)


# ---------------------------------------------------------------------
# Question card exemplars
# ---------------------------------------------------------------------

QUESTION_CARD_EXEMPLARS: tuple[Exemplar, ...] = (
    Exemplar(
        situation="DePIN/Nepal long-standing pattern question. Founder has not moved on it.",
        input_summary=(
            "standing_days=41; subject=goal g-42 (DePIN Nepal); "
            "pattern_observed='inspection-only: every Observation in last 6 weeks "
            "is an inspection event; no Commitments, no Model movement, no Resources'; "
            "founder_signal='12 Feb Atlas journal: \"the one thing I'd feel proudest of\"'; "
            "asymmetry='present intention, absent action, strong emotional anchor'"
        ),
        html_output=(
            "Is the DePIN goal a real bet, or is it there because letting it go "
            "would feel like giving up on Nepal? Six weeks, "
            "<span class=\"n\">0.3 FTE</span>, no commitments, no Model movement. "
            "I can tell you're visiting it; <span class=\"hl\">I can't tell you "
            "what visiting means</span>."
        ),
    ),
)


# ---------------------------------------------------------------------
# Query-grid chip exemplars (short labels, not full prose)
# ---------------------------------------------------------------------

QUERY_GRID_EXEMPLARS: tuple[Exemplar, ...] = (
    Exemplar(
        situation="Crisis-day hot query about Acme becoming unsafe.",
        input_summary=(
            "hot=true tag=urgent icon=why "
            "intent='the founder wants to understand why Acme flipped to unsafe'"
        ),
        html_output="Show me why Acme became unsafe",
    ),
    Exemplar(
        situation="Crisis-day relevant query tying to Thursday board update.",
        input_summary=(
            "hot=true tag=relevant icon=brief "
            "intent='what Acme situation means for the Thursday board conversation'"
        ),
        html_output="What this means for Thursday's board update",
    ),
    Exemplar(
        situation="Drafting chip.",
        input_summary=(
            "hot=false tag=2min icon=draft "
            "intent='founder wants a drafted brief for Monica, head of sales, "
            "about the Acme slip before the call'"
        ),
        html_output="Draft a brief for Monica",
    ),
    Exemplar(
        situation="Evergreen retrospective chip.",
        input_summary=(
            "hot=false tag=None icon=timeline "
            "intent='what did the founder miss in the last day?'"
        ),
        html_output="What did I miss yesterday?",
    ),
    Exemplar(
        situation="Evergreen calibration chip.",
        input_summary=(
            "hot=false tag=None icon=calibration "
            "intent='which founder beliefs are least supported by the substrate right now?'"
        ),
        html_output="Which of my beliefs are least supported?",
    ),
    Exemplar(
        situation="Evergreen silence-detection chip.",
        input_summary=(
            "hot=false tag=None icon=observation "
            "intent='where is the company silent where it should be speaking?'"
        ),
        html_output="Where is the company silent where it shouldn't be?",
    ),
)


# ---------------------------------------------------------------------
# Conversation-turn exemplars
# ---------------------------------------------------------------------

CONVERSATION_TURN_EXEMPLARS: tuple[Exemplar, ...] = (
    Exemplar(
        situation="Founder asked: 'Show me why Acme became unsafe.'",
        input_summary=(
            "query='Show me why Acme became unsafe.' "
            "retrieval=[m-2841 falsifier='two contracted deliverables slip past 15 Apr'; "
            "c-187 Blocked (linear webhook Sat 19:03); "
            "c-203 re-estimated 2d\u219210d (Alice slack Sat 22:41); "
            "m-2841 conf 0.81\u21920.54 Sun 03:12; "
            "Monica last touchpoint 04 Apr; "
            "eng channel=11 mentions, revenue=0]"
        ),
        html_output=(
            "<div class=\"t-body\">"
            "Model <span class=\"t-id\">m-2841</span> carried a falsifier: "
            "<em>two or more contracted deliverables slip past 15 April</em>. "
            "It fired Saturday when <span class=\"t-id\">c-187</span> "
            "transitioned to <span class=\"t-kind\">Blocked</span> "
            "<span class=\"cite\">linear webhook \u2014 Sat 19:03</span> "
            "and Alice re-estimated <span class=\"t-id\">c-203</span> from two "
            "days to ten "
            "<span class=\"cite\">Alice \u2014 Sat 22:41</span>."
            "\n\n<span class=\"note\">Neither transition reached the CEO channel.</span> "
            "Monica's last customer touchpoint was 04 April; she has not been "
            "contested on her \u201Crock solid\u201D read. Engineering Slack has "
            "<span class=\"n\">11 mentions</span> of the slip since Friday; "
            "revenue has <span class=\"hl\">zero</span>.\n\nCurrent confidence: "
            "<span class=\"n\">0.54</span>. Revenue at risk, computed structurally "
            "through the customer_commitments spine: <span class=\"n\">$487,000</span>."
            "</div>"
        ),
    ),
    Exemplar(
        situation="Founder asked: 'What did I miss yesterday?'",
        input_summary=(
            "query='What did I miss yesterday?' "
            "retrieval=[alice_re-estimated c-203 Sat 22:41 (original signal behind "
            "Acme unsafe); cryptographer X critique of Nexus attestation 19:40 "
            "(moved m-2791 from 0.51 to 0.58); vertex_labs 21-day silence, "
            "expansion 0.71\u21920.48]"
        ),
        html_output=(
            "<div class=\"t-body\">"
            "Three things from yesterday you haven't engaged with:\n\n"
            "<b>1.</b> Alice re-estimated <span class=\"t-id\">c-203</span> "
            "Saturday night "
            "<span class=\"cite\">Alice \u2014 Sat 22:41</span>. "
            "This is the signal that made Acme unsafe. "
            "<span class=\"note\">You saw the downstream effect this morning; "
            "you haven't seen the original signal.</span>\n\n"
            "<b>2.</b> A cryptographer published a critique of Nexus "
            "attestation pattern on X at <span class=\"n\">19:40</span> "
            "yesterday. Moved <span class=\"t-id\">m-2791</span> "
            "(\u201Cpilot will surface trust issue\u201D) "
            "from <span class=\"n\">0.51 \u2192 0.58</span>. Worth reading "
            "directly.\n\n"
            "<b>3.</b> Vertex Labs hit <span class=\"n\">21 days</span> with "
            "zero touchpoints. Expansion probability drifted "
            "<span class=\"n\">0.71 \u2192 0.48</span>. No one flagged this "
            "internally."
            "</div>"
        ),
    ),
)


# ---------------------------------------------------------------------
# Close-line exemplars
# ---------------------------------------------------------------------

CLOSE_LINE_EXEMPLARS: tuple[Exemplar, ...] = (
    Exemplar(
        situation="Active-day close.",
        input_summary="signals=14206, external_moves=3, calibration_pct=73",
        html_output="That's the signal. You can go.",
    ),
    Exemplar(
        situation="Quiet-day close. Same release phrase; scale is honest.",
        input_summary="signals=2011, external_moves=0, calibration_pct=78",
        html_output="That's the signal. You can go.",
    ),
)


__all__ = [
    "CLOSE_LINE_EXEMPLARS",
    "CONVERSATION_TURN_EXEMPLARS",
    "DECISION_CARD_EXEMPLARS",
    "Exemplar",
    "GREETING_EXEMPLARS",
    "OBSERVATION_CARD_EXEMPLARS",
    "QUERY_GRID_EXEMPLARS",
    "QUESTION_CARD_EXEMPLARS",
]
