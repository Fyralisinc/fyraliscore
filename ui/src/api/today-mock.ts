// Mock fixture for the Today surface. Shapes match today-types.ts.
// Content is the seven-card scenario from FYRALIS_TODAY_SPEC.md §12.
// Used by mock-server.ts (dev backend shim).

import type { TodayResponse, TriageAction } from "./today-types";

export const TODAY_FIXTURE: TodayResponse = {
  brand: { name: "Fyralis", mark: "D", pulse_day: 47 },
  page: {
    date_label: "Saturday, April 25.",
    state_tone: "tense",
    state_text:
      "Acme renewal is the main thing on my mind. Seven items need you today; three are strategic.",
  },
  signal_strip: [
    {
      id: "arr",
      label: "ARR",
      value: "$8.4M",
      trend_html: "↑ <em>4.2% MoM</em> · pace to $10M by Q3",
      tone: "accent",
    },
    {
      id: "runway",
      label: "Runway",
      value: "19",
      value_unit: "months",
      trend_html: "burn $410K/mo · steady",
      tone: "accent",
    },
    {
      id: "commitments",
      label: "Commitments",
      value: "42 / 47",
      trend_html: "↓ <em>3 slipped</em> this week",
      tone: "warn",
    },
    {
      id: "calibration",
      label: "My calibration",
      value: "0.81",
      trend_html: "strong on patterns · weak on personnel",
    },
  ],
  vitals: [
    { id: "v1", label: "Acme renewal",      value: "at risk",   tone: "warn" },
    { id: "v2", label: "Decision drift",    value: "2 active",  tone: "amber" },
    { id: "v3", label: "Slipping commits",  value: "5 of 47",   tone: "amber" },
    { id: "v4", label: "Pattern threshold", value: "3 forming" },
    { id: "v5", label: "Held by you",       value: "3 items" },
  ],
  nav: [
    {
      id: "operate",
      label: "Operate",
      items: [
        { id: "today",     label: "Today",     active: true, badge: "7", shortcut: "⌘7" },
        { id: "structure", label: "Structure" },
        { id: "history",   label: "History" },
        { id: "hold",      label: "Hold",      badge: "3", shortcut: "⌘3" },
      ],
    },
    {
      id: "communicate",
      label: "Communicate",
      items: [
        { id: "threads",   label: "Threads",   disabled: true, badge: "soon" },
        { id: "people",    label: "People",    disabled: true, badge: "soon" },
        { id: "customers", label: "Customers", disabled: true, badge: "soon" },
      ],
    },
    {
      id: "account",
      label: "Account",
      items: [
        { id: "ledger",  label: "Ledger",  disabled: true, badge: "soon" },
        { id: "capital", label: "Capital", disabled: true, badge: "soon" },
      ],
    },
  ],
  cards: [
    {
      id: "rec-1",
      severity: "critical",
      category: "operational",
      kind_label: "Decision drift · d-5",
      meta: "15 min · only you can ratify",
      tag: { kind: "quiet", label: "routed to you" },
      headline_html:
        'Heads up on <em>d-5</em> — three commitments are quietly contradicting <em>"use Redis for rate limiting."</em>',
      supporting_html:
        "Pattern just crossed my action threshold. <em>Three independent enterprise contexts is rarely coincidence.</em>",
      stats: [
        { label: "Confidence",    value: "82%" },
        { label: "Contradicting", value: "3 commits", tone: "warn" },
        { label: "Falsifier",     value: "Marcus reaffirms", tone: "default" },
      ],
      expand_cta: "Open",
      actions: ["act", "hold", "route"],
      detail: {
        reasoning_html:
          "<p>Three commitments in the last 14 days have moved in directions that quietly contradict <em>d-5</em>. None of them is alarming alone; the cluster is.</p><p>If Marcus reaffirms <span class='voice-quote'>'Redis is still the call'</span> in writing, I'd revise down. Otherwise, the drift is real.</p>",
        evidence: [
          {
            src: "apr 12 · commit",
            quote_html: "Switched to Postgres LISTEN/NOTIFY for the renewal-pings worker.",
            attribution: "c-204 · alice@",
          },
          {
            src: "apr 18 · doc",
            quote_html: "Spec for the auth path uses an in-memory token bucket — no Redis.",
            attribution: "design doc · marcus@",
          },
          {
            src: "apr 22 · pr",
            quote_html: "Bay Group integration leans on a per-tenant cache, not the rate limiter.",
            attribution: "pr-c-211 · sarah@",
          },
        ],
        confidence: [
          { label: "On pattern", value_html: "82% — <em>three signals, all enterprise</em>" },
          { label: "On action",  value_html: "67% — <em>this might just be a one-off</em>" },
          { label: "Falsifier",  value_html: "<em>Marcus reaffirms in writing</em>" },
        ],
        paths: [
          {
            id: "p1",
            label: "Reaffirm",
            body_html:
              "<strong>Ask Marcus to restate d-5 in writing this week</strong>. Lowest-friction path; resolves my uncertainty without policy change.",
          },
          {
            id: "p2",
            label: "Revisit",
            body_html:
              "<strong>Schedule a 30-min review</strong> with Marcus and Sarah. Confirm pattern is real, scope work, decide Q2 vs Q3.",
          },
          {
            id: "p3",
            label: "Defer",
            body_html:
              "<strong>Wait two weeks for one more data point</strong>. <em>If the drift continues, my confidence climbs to 0.91.</em>",
          },
        ],
        show_ask: true,
      },
    },
    {
      id: "rec-2",
      severity: "strategic",
      category: "strategic",
      kind_label: "Strategic · feature",
      meta: "3 evidence · 14 days",
      tag: { kind: "new", label: "new" },
      headline_html:
        "Worth thinking about <em>audit logs</em> — three enterprise customers have asked in the last two weeks.",
      supporting_html:
        "Pattern crossed action threshold today. <em>Acme, Meridian, and Bay Group asked independently.</em>",
      stats: [
        { label: "On pattern", value: "87%" },
        { label: "On action",  value: "67%" },
        { label: "At stake",   value: "$770K", tone: "amber" },
      ],
      expand_cta: "See evidence",
      actions: ["act", "hold", "dismiss"],
      detail: {
        reasoning_html:
          "<p>Three enterprise customers brought up audit logs in calls during the last 14 days. <em>None of them is alarming alone; the cluster is.</em></p><p>If two of these were referrals from the same conversation, I'd revise down. The asks were independent.</p>",
        evidence: [
          { src: "apr 12 · call", quote_html: "We'd need audit trails for SOC2 — when's that coming?", attribution: "Acme · q1 review" },
          { src: "apr 18 · email", quote_html: "Compliance asked about user-level activity logs.", attribution: "Meridian · ops" },
          { src: "apr 22 · slack", quote_html: "Audit log API is on our must-have list for renewal.", attribution: "Bay Group · cto" },
        ],
        confidence: [
          { label: "On pattern", value_html: "87% — <em>three independent enterprise contexts</em>" },
          { label: "On action",  value_html: "67% — <em>scope is wide; we'd need to commit</em>" },
          { label: "Falsifier",  value_html: "<em>If deeper conversations reveal these are negotiable, I'd revise down.</em>" },
        ],
        paths: [
          { id: "p1", label: "Enterprise", body_html: "<strong>Scope a v1 audit-log surface for the enterprise tier</strong>. Two engineer-weeks; ships into the Q2 release train." },
          { id: "p2", label: "Light",      body_html: "<strong>Ship a read-only feed</strong> within two weeks. Buys time without a full commit." },
          { id: "p3", label: "Wait",       body_html: "<strong>Wait for one more independent ask</strong>. <em>If it comes, my confidence climbs to 0.95.</em>" },
        ],
        show_ask: true,
      },
    },
    {
      id: "rec-3",
      severity: "strategic",
      category: "strategic",
      kind_label: "Strategic · personnel",
      meta: "curiosity, not concern",
      tag: { kind: "quiet", label: "weak calibration" },
      headline_html:
        "Worth a 1:1 with <em>Sarah</em> in the next two weeks — three weak signals are clustering.",
      supporting_html:
        "My calibration on personnel is weak (3/6). <em>Discount accordingly.</em>",
      stats: [
        { label: "On signal",      value: "71%" },
        { label: "On action",      value: "48%" },
        { label: "My calibration", value: "3/6", tone: "amber" },
      ],
      expand_cta: "See signals",
      actions: ["act", "hold", "dismiss"],
      detail: {
        reasoning_html:
          "<p>Three weak signals in the last three weeks: a slower response cadence on the enterprise channel, a missed retro, and a Slack message tone-shift. <em>Each is small.</em></p><p>I want to flag — my calibration on personnel is weak. <em>Discount accordingly.</em> A 1:1 would resolve my uncertainty either way.</p>",
        confidence: [
          { label: "On signal",      value_html: "71% — <em>three weak signals, clustered</em>" },
          { label: "On action",      value_html: "48% — <em>I'm not sure a 1:1 helps</em>" },
          { label: "My calibration", value_html: "3/6 — <em>I've been wrong about this kind of thing before</em>" },
        ],
        paths: [
          { id: "p1", label: "Reaffirm", body_html: "<strong>Schedule a casual 1:1 within two weeks</strong>. Low-cost, high information." },
          { id: "p2", label: "Wait",      body_html: "<strong>Watch for one more signal</strong>. I'll surface again if the cluster sharpens." },
          { id: "p3", label: "Reject",    body_html: "<strong>Tell me I'm reading this wrong</strong> — and I'll recalibrate." },
        ],
        show_ask: true,
      },
    },
    {
      id: "rec-4",
      severity: "strategic",
      category: "strategic",
      kind_label: "Strategic · prioritization",
      meta: "2 weeks of capacity",
      tag: { kind: "quiet", label: "2 engineers freeable" },
      headline_html:
        "The analytics dashboard rebuild can probably be deprioritized — its weight has dropped relative to enterprise-tier work.",
      supporting_html:
        "Customer mentions in 6 weeks: <em>0 of 6</em>.",
      stats: [
        { label: "Weight shift",   value: "81%" },
        { label: "On deprio",      value: "55%" },
        { label: "Cust. mentions", value: "0 / 6 wks", tone: "amber" },
      ],
      expand_cta: "See paths",
      actions: ["act", "hold", "dismiss"],
      detail: {
        reasoning_html:
          "<p>The analytics dashboard work has carried a high weight for nine weeks. In the last six, no customer has mentioned it. Enterprise-tier asks have crowded into that capacity.</p><p>If I'm wrong, the cost is two engineer-weeks. If I'm right, two engineers are freeable for audit logs.</p>",
        paths: [
          { id: "p1", label: "Reallocate", body_html: "<strong>Pause the rebuild and shift the two engineers to enterprise-tier work</strong>." },
          { id: "p2", label: "Slow",        body_html: "<strong>Cut scope by half</strong>. Keeps the surface alive without the full commit." },
          { id: "p3", label: "Hold",        body_html: "<strong>Don't change anything for two weeks</strong>. <em>If the customer-mention count stays at zero, I'll surface again with higher confidence.</em>" },
        ],
      },
    },
    {
      id: "rec-5",
      severity: "high",
      category: "operational",
      kind_label: "VP signal conflict",
      meta: "5-min · marcus waiting",
      tag: { kind: "quiet", label: "2 teams affected" },
      headline_html:
        "<em>Sarah</em> and <em>Marcus</em> are giving conflicting signals on enterprise vs developer tier sequencing.",
      supporting_html:
        "Quickest fix is a 3-line slack. <em>Capacity bleed is 9 days and counting.</em>",
      stats: [
        { label: "Confidence",     value: "91%" },
        { label: "Capacity bleed", value: "9 days", tone: "amber" },
        { label: "Quickest fix",   value: "3-line slack" },
      ],
      expand_cta: "See evidence",
      actions: ["act", "hold", "snooze"],
      detail: {
        reasoning_html:
          "<p>Both VPs have spoken to the team about sequencing this week. The signals don't reconcile. Two teams have stopped moving while they wait for clarity.</p>",
        evidence: [
          { src: "apr 23 · slack", quote_html: "Enterprise tier ships first.", attribution: "Sarah · #eng-leads" },
          { src: "apr 24 · standup", quote_html: "Developer-tier work is unblocking us; let's keep that lane clear.", attribution: "Marcus · standup notes" },
        ],
      },
    },
    {
      id: "rec-6",
      severity: "med",
      category: "operational",
      kind_label: "Customer reciprocity",
      meta: "tone shift detected",
      tag: { kind: "quiet", label: "routed to you" },
      headline_html:
        "Reach out to <em>Northwind's CEO</em> — two unanswered emails, tone shift detected.",
      stats: [
        { label: "Confidence",   value: "73%" },
        { label: "At stake",     value: "$210K" },
        { label: "Relationship", value: "3.4 yrs" },
      ],
      expand_cta: "Open",
      actions: ["act", "hold", "route"],
      detail: {
        reasoning_html:
          "<p>Two unreplied emails in nine days, tone shift on the third. Northwind has been a 3.4-year reference customer.</p>",
      },
    },
    {
      id: "rec-7",
      severity: "med",
      category: "operational",
      kind_label: "Quick approval",
      meta: "marcus blocked",
      tag: { kind: "quiet", label: "routed to you" },
      headline_html:
        "Approve or block <em>Bay Group's</em> custom-integration request.",
      stats: [
        { label: "At stake",    value: "$120K" },
        { label: "Cost if yes", value: "2 eng-wks" },
        { label: "Marcus",      value: "blocked", tone: "amber" },
      ],
      expand_cta: "See paths",
      actions: ["act", "hold", "route"],
      detail: {
        paths: [
          { id: "p1", label: "Approve", body_html: "<strong>Greenlight the integration</strong>. Two engineer-weeks; ships into Q2." },
          { id: "p2", label: "Block",   body_html: "<strong>Block and offer the standard API</strong>. Lower revenue, lower commitment." },
        ],
      },
    },
  ],
  cleared_today: 0,
  just_updated: {
    text_html:
      "<b>Just now:</b> three customers have asked about audit logs in 14 days — pattern crossed action threshold",
  },
  routed_coda: {
    total: 14,
    rows: [
      { recipient: "Marcus",            count: 5, items: "rate limiter, design review, auth migration sequencing, c-211 review, infra debt" },
      { recipient: "Sarah",             count: 4, items: "Northwind tone shift, customer sentiment dip, Q2 health review, retention patterns" },
      { recipient: "Team leads",        count: 3, items: "technical-debt mentions in backend / platform / devops" },
      { recipient: "Watching, no owner", count: 2, items: "two engineers' language patterns, d-12 contestation cluster" },
    ],
  },
  ask_suggestions: [
    "What are you least sure about?",
    "Show me Alice's recent work",
    "What's on Hold I should look at?",
  ],
};

export function mockTriage(
  recommendationId: string,
  action: TriageAction
): { ok: true; recommendation_id: string; action: TriageAction } {
  return { ok: true, recommendation_id: recommendationId, action };
}
