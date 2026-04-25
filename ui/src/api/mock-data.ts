// Fixture data for the CEO view. Shapes match CONTRACTS.md §1.1.
// Content is lifted verbatim from /company-os.html so the mocked view
// is visually identical to the prototype. Used by:
//   - mock-server.ts (dev backend shim)
//   - MSW handlers (unit tests)
//   - Playwright fixtures (e2e)

import type { AskResponse, HomeResponse, TurnVerb } from "./types";

const NOW_ISO = "2026-04-22T01:00:00Z"; // Tue 22 Apr 06:42 Asia/Kathmandu

export const HOME_FIXTURE: HomeResponse = {
  greeting: {
    meta: {
      date_iso: "2026-04-22",
      recomputed_at: NOW_ISO,
      signals_watched_count: 14206,
    },
    body_html:
      'Good morning. One thing is worth your attention before the day starts &mdash; Acme&rsquo;s renewal is <span class="serif">structurally unsafe</span> as of Sunday, and revenue hasn&rsquo;t caught it yet. One decision is on you by Thursday. Everything else is handled.',
    cached_at: NOW_ISO,
    staleness_seconds: 47,
  },
  query_grid: {
    queries: [
      {
        id: "acme-why",
        icon: "why",
        label: "Show me why Acme became unsafe",
        tag: "urgent",
        hot: true,
      },
      {
        id: "acme-board",
        icon: "brief",
        label: "What this means for Thursday's board update",
        tag: "relevant",
        hot: true,
      },
      {
        id: "monica-brief",
        icon: "draft",
        label: "Draft a brief for Monica",
        tag: "2min",
        hot: false,
      },
      {
        id: "miss",
        icon: "calibration",
        label: "What did I miss yesterday?",
        hot: false,
      },
      {
        id: "beliefs",
        icon: "pattern",
        label: "Which of my beliefs are least supported?",
        hot: false,
      },
      {
        id: "silent",
        icon: "observation",
        label: "Where is the company silent where it shouldn't be?",
        hot: false,
      },
    ],
    cached_at: NOW_ISO,
  },
  cards: [
    {
      id: "obs-1",
      kind: "observation",
      tag_color: "hot",
      tag_label: "Observation · revenue at risk",
      meta: "filed Sun 03:12 · 2d old",
      body_html:
        'Acme&rsquo;s renewal is <span class="serif-hot">structurally unsafe</span>. Confidence dropped <span class="n">0.81 → 0.54</span> after two contracted deliverables slipped. Engineering has discussed this 11 times since Friday; the revenue channel has <span class="hl">zero mentions</span>. Revenue at risk: <span class="n" style="color: var(--hot);">$487K</span>.',
      expanded: {
        reasoning_html:
          '<p>Model <b>m-2841</b> (&ldquo;Acme renews Q3&rdquo;) carried a falsifier: <span class="note">two or more contracted deliverables slip past 15 April</span>. That falsifier fired Saturday when <b>c-187</b> transitioned to Blocked and Alice re-estimated <b>c-203</b> from two days to ten.</p><p>Neither transition reached the CEO channel. Monica&rsquo;s last customer touchpoint was 04 April; she has not been contested on her &ldquo;rock solid&rdquo; read.</p>',
        evidence: [
          {
            label: "Evidence chain · Sat–Sun",
            body_html:
              '<div class="trow"><span class="t-id">obs-88412</span><span class="t-kind">state_change</span><span class="t-body">c-187 <span class="hot">Blocked</span> · linear webhook · Sat 19:03</span></div><div class="trow"><span class="t-id">obs-88430</span><span class="t-kind">slack</span><span class="t-body">Alice re-estimates c-203: 2d → ~10d · Sat 22:41</span></div><div class="trow"><span class="t-id">m-2841</span><span class="t-kind">update</span><span class="t-body">conf 0.81 → <span class="hot">0.54</span> · falsifier fired · Sun 03:12</span></div><div class="trow"><span class="t-id">r-cust-acme</span><span class="t-kind">health</span><span class="t-body">Healthy → <span class="hot">Warning</span> · r-a-r $487K</span></div>',
          },
        ],
        verbs: [
          {
            id: "full-reasoning",
            label: "Full reasoning",
            primary: true,
            query_template: "Walk me through the full reasoning on Acme.",
          },
          {
            id: "implications",
            label: "Implications for Thursday",
            primary: false,
            query_template:
              "What this means for Thursday's board update.",
          },
          {
            id: "draft-monica",
            label: "Draft Monica brief",
            primary: false,
            query_template: "Draft a brief for Monica.",
          },
          {
            id: "sit",
            label: "Sit with it",
            primary: false,
            query_template: "",
          },
        ],
      },
      cached_at: NOW_ISO,
    },
    {
      id: "dec-1",
      kind: "decision",
      tag_color: "warm",
      tag_label: "Decision · on you",
      meta: "drafts for both paths ready",
      body_html:
        '<div class="card-content"><p class="dec-text">Re-scope the Acme deliverable, or <em>extend the renewal window</em>.</p><div class="dec-chips"><span class="dec-chip hot">decide by <b>Thu 24 Apr</b></span><span class="dec-chip">at stake <b>$487K</b></span></div></div>',
      expanded: {
        reasoning_html:
          '<div class="path"><div class="p-top"><span class="p-name">Path A · re-scope the deliverable</span><span class="p-tag">fits Monica&rsquo;s pattern</span></div><p>Drop the dashboard handoff from contracted scope; keep the rate-limiter on SLA. Preserves the renewal conversation, costs ~2 weeks engineering credit, resolves by end of week. Requires Monica on a call Wednesday.</p></div><div class="path"><div class="p-top"><span class="p-name">Path B · extend renewal 30 days</span><span class="p-tag">cleaner, commercially harder</span></div><p>Drops revenue-at-risk to $0 immediately. Signals a pattern to other customers; Northwind&rsquo;s expansion Model would absorb <b>−0.06</b> on expansion probability.</p></div><p><span class="note">No preference. The call depends on how you&rsquo;re weighing Northwind&rsquo;s next renewal.</span></p>',
        evidence: [],
        verbs: [
          {
            id: "path-a",
            label: "See Path A email",
            primary: true,
            query_template: "Show me the drafted email for Path A.",
          },
          {
            id: "path-b",
            label: "See Path B email",
            primary: false,
            query_template: "Show me the drafted email for Path B.",
          },
          {
            id: "defer",
            label: "Defer with a reason",
            primary: false,
            query_template: "",
          },
        ],
      },
      cached_at: NOW_ISO,
    },
    {
      id: "q1",
      kind: "question",
      tag_color: "soft",
      tag_label: "Open question · standing 41 days",
      meta: "only you can resolve",
      body_html:
        '<p class="q-text">Is the DePIN goal a real bet, or is it there because letting it go would feel like giving up on Nepal?</p><p class="q-sub">Six weeks, 0.3 FTE, no commitments, no Model movement. I can tell you&rsquo;re visiting it; I can&rsquo;t tell you what visiting means.</p>',
      expanded: {
        reasoning_html:
          '<p>I&rsquo;m asking because of a pattern &mdash; not the DePIN documents themselves.</p><p>Every Observation tied to <span class="cite">g-42</span> in the last six weeks has been an <span class="note">inspection</span> event. No Commitments created, no contributing Models moved, no Resources deployed. In your company&rsquo;s history, that pattern precedes either a funding decision or a quiet closure.</p><p>Your 12 February Atlas journal called Nepal&rsquo;s hydropower <span class="note">&ldquo;the one thing I&rsquo;d feel proudest of.&rdquo;</span> You haven&rsquo;t written about it since. That combination &mdash; present intention, absent action, strong emotional anchor &mdash; is why I don&rsquo;t know which way you&rsquo;re leaning.</p><div class="q-options"><button class="q-option">Real bet · fund it this quarter</button><button class="q-option">It&rsquo;s over · kill it</button><button class="q-option">Something in between</button><button class="q-option">Not ready</button></div>',
        evidence: [],
        verbs: [
          {
            id: "pattern",
            label: "Show me the pattern",
            primary: false,
            query_template: "Show me the Nepal pattern in full.",
          },
          {
            id: "not-now",
            label: "Not now",
            primary: false,
            query_template: "",
          },
        ],
      },
      cached_at: NOW_ISO,
    },
  ],
  close_line: {
    body: "That's the signal. You can go.",
    metadata: {
      signal_count: 14206,
      external_moves: 3,
      calibration_pct: 73,
    },
  },
  status: {
    substrate_alive: true,
    calibration_pct: 73,
    needs_you_count: 1,
  },
};

const DEFAULT_VERBS: TurnVerb[] = [
  { id: "followup", label: "Follow up" },
  { id: "save", label: "Save" },
  { id: "done", label: "Done" },
];

export function mockAsk(query: string): AskResponse {
  const lower = query.toLowerCase();
  let html: string;
  if (
    lower.includes("acme") &&
    (lower.includes("why") ||
      lower.includes("unsafe") ||
      lower.includes("reasoning"))
  ) {
    html =
      '<p>Model <span class="cite">m-2841</span> carried a falsifier: <em>two or more contracted deliverables slip past 15 April</em>. It fired Saturday when <span class="cite">c-187</span> transitioned to Blocked and Alice re-estimated <span class="cite">c-203</span> from two days to ten.</p><p>Neither transition reached the CEO channel. Monica&rsquo;s last customer touchpoint was 04 April; she has not been contested on her &ldquo;rock solid&rdquo; read.</p><p>Current confidence: <b>0.54</b>. Revenue at risk, computed structurally through the customer_commitments spine: <b>$487,000</b>.</p>';
  } else if (lower.includes("thursday") || lower.includes("board")) {
    html =
      '<p>If Thursday opens with &ldquo;Acme rock solid&rdquo; and the slip surfaces later through Monica or Acme, your calibration with investors takes a real hit. My confidence this surfaces within 10 days regardless: <b>0.72</b>.</p><p>Two framings that hold up: <em>&ldquo;Acme is our anchor Q3 renewal; we identified a dependency we&rsquo;re actively scoping with them&rdquo;</em> or <em>&ldquo;We have one commercial call open on Acme&rsquo;s renewal structure, closing by end of next week&rdquo;</em>.</p>';
  } else if (lower.includes("monica")) {
    html =
      '<p>Drafted in your Slack voice with her:</p><p><em>Hey &mdash; seeing the Acme Q2 slip pattern on eng channel since Fri. Renewal confidence dropped to 0.54. Want to sync today before Thursday? 15 min would do it.</em></p><p>Non-alarming, gives her context, offers the handle.</p>';
  } else if (lower.includes("path a")) {
    html =
      '<p>Drafted in your tone with Acme:</p><p><em>Hi Robert &mdash; quick note before Thursday. As the team&rsquo;s dug into the Q2 release we&rsquo;ve found a dependency we want to scope precisely with you rather than rush.</em></p>';
  } else if (lower.includes("path b")) {
    html =
      '<p>Drafted &mdash; commercially harder, because it asks Acme to flex:</p><p><em>Hi Robert &mdash; we&rsquo;d like to propose shifting the renewal window 30 days to give both sides room to finalize Q2 cleanly.</em></p>';
  } else if (
    lower.includes("nepal") ||
    lower.includes("depin") ||
    lower.includes("g-42") ||
    lower.includes("pattern")
  ) {
    html =
      '<p>The pattern: every Observation tied to <span class="cite">g-42</span> in the last six weeks has been an <em>inspection</em> event. No Commitments created. No contributing Models moved. No Resources deployed.</p>';
  } else if (lower.includes("miss") || lower.includes("yesterday")) {
    html =
      '<p>Three things from yesterday you haven&rsquo;t engaged with:</p><p><b>1.</b> Alice re-estimated c-203 Saturday night.</p><p><b>2.</b> A cryptographer published a critique of Nexus attestation pattern on X, 19:40 yesterday.</p><p><b>3.</b> Vertex Labs hit 21 days with zero touchpoints.</p>';
  } else if (lower.includes("believ") || lower.includes("least")) {
    html =
      '<p>Beliefs the substrate considers least supported:</p><p><b>1. &ldquo;Atlas beta is on track.&rdquo;</b> Substrate confidence: <b>0.48</b>.</p><p><b>2. &ldquo;Acme is rock solid.&rdquo;</b> See today&rsquo;s signal.</p><p><b>3. &ldquo;We&rsquo;ll ship EU compliance in time.&rdquo;</b> Substrate confidence: <b>0.26</b>.</p>';
  } else if (lower.includes("silent") || lower.includes("quiet")) {
    html =
      '<p>Two silences the substrate reads as meaningful:</p><p><b>Revenue vs engineering on Acme.</b> 11 engineering mentions, 0 revenue mentions.</p><p><b>Vertex Labs, 3 weeks.</b> Expansion probability drifted 0.71 → 0.48.</p>';
  } else {
    html =
      "<p>I don&rsquo;t have a grounded answer to that yet. Want me to pull from the Acme thread, the Nepal pattern, or the EU compliance region?</p>";
  }

  return {
    turn_id: `turn-${Math.random().toString(36).slice(2, 10)}`,
    query_echo: query,
    response_html: html,
    verbs: DEFAULT_VERBS,
    computed_at: new Date().toISOString(),
    latency_ms: 120,
  };
}
