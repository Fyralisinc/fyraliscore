// Sample data for the My Mind page. Anchored on 2026-04-30 (today).
// See DRIFTWOOD_MY_MIND_SPEC.md Part 22 for canonical samples.

import type { Loop, Note, Reminder } from "./types";

const ANCHOR = new Date("2026-04-30T10:00:00Z");
const day = 24 * 60 * 60 * 1000;
function isoAt(daysOffset: number, hour = 10, minute = 0): string {
  const d = new Date(ANCHOR.getTime() + daysOffset * day);
  d.setUTCHours(hour, minute, 0, 0);
  return d.toISOString();
}

export const SAMPLE_LOOPS: Loop[] = [
  {
    id: "loop-q3-pipeline",
    category: "loop",
    kind: "concern",
    headline: "Q3 pipeline looking tight.",
    created: isoAt(-17, 9, 42),
    updated: isoAt(-12, 14, 30),
    state: "open",
    from_today: false,
    person: null,
    substrate_evidence:
      "Pipeline coverage is currently 2.3x of Q3 target, down from 2.8x in May. Three deals slipped from Q2.",
    substrate_stance: "This concern matches my own observations.",
    user_notes: [
      {
        date: isoAt(-12, 14, 30).slice(0, 10),
        text: "Talked to Sarah. She thinks two of the three slipped deals are still recoverable.",
      },
    ],
  },
  {
    id: "loop-audit-logs",
    category: "loop",
    kind: "action",
    headline:
      "Worth thinking about audit logs — three enterprise customers asked in the last two weeks.",
    created: isoAt(-9, 10, 30),
    updated: isoAt(-5, 8, 0),
    state: "open",
    from_today: true,
    today_card_id: "today-audit-logs-2026-04-21",
    substrate_evidence:
      "Original Today context: 87% on pattern, 67% on action, $770K at stake. 3 evidence: Acme Apr 12, Meridian Apr 18, Bay Group Apr 24.",
    substrate_stance:
      "I notice this has been on hold while the underlying pattern strengthened. Two more customers asked since then.",
    user_notes: [],
  },
  {
    id: "loop-sarah-feedback",
    category: "loop",
    kind: "action",
    headline: "Owe Sarah feedback on her promotion case.",
    created: isoAt(-5, 16, 0),
    updated: isoAt(-5, 16, 0),
    state: "open",
    from_today: false,
    person: "Sarah",
    user_notes: [],
  },
  {
    id: "loop-head-of-marketing",
    category: "loop",
    kind: "question",
    headline: "Should we hire a head of marketing this year?",
    created: isoAt(-3, 11, 12),
    updated: isoAt(-3, 11, 12),
    state: "open",
    from_today: false,
    person: null,
    substrate_stance:
      "I don't have enough signal to weigh in yet — pipeline is mixed, brand recognition is steady.",
    user_notes: [],
  },
  {
    id: "loop-old-redis",
    category: "loop",
    kind: "concern",
    headline: "Are we still over-investing in the Redis migration?",
    created: isoAt(-41, 14, 0),
    updated: isoAt(-15, 9, 0),
    state: "open",
    from_today: false,
    person: null,
    substrate_stance:
      "This has been here 41 days. The pattern strengthened — the team booked another two weeks since.",
    user_notes: [],
  },
];

export const SAMPLE_NOTES: Note[] = [
  {
    id: "note-david-partnership",
    category: "note",
    headline:
      "David at the board mentioned thinking about a partnership with the financial-services consortium.",
    created: isoAt(-12, 16, 0),
    state: "captured",
    source: "David · board call",
    substrate_stance:
      "I notice this connects to the Acme + Meridian + Bay Group audit logs pattern surfaced today — they're all in financial services. Worth promoting to a Loop?",
  },
  {
    id: "note-org-design-book",
    category: "note",
    headline: "Read that book Marcus sent on org design.",
    created: isoAt(-6, 9, 0),
    state: "captured",
    source: "Marcus",
  },
  {
    id: "note-churn-patterns",
    category: "note",
    headline: "Look into customer churn patterns by segment.",
    created: isoAt(-1, 17, 30),
    state: "captured",
  },
  {
    id: "note-allhands-capacity",
    category: "note",
    headline: "Two people in all-hands mentioned engineering capacity.",
    created: isoAt(-2, 12, 0),
    state: "captured",
    substrate_stance:
      "This is the third time the capacity theme has come up in your notes this quarter.",
  },
];

export const SAMPLE_REMINDERS: Reminder[] = [
  {
    id: "rem-deck-friday",
    category: "reminder",
    trigger_type: "time",
    headline: "Send the deck Friday morning.",
    created: isoAt(-8, 11, 15),
    remind_at: isoAt(-5, 8, 0),
    state: "fired",
    fired_at: isoAt(-5, 8, 0),
  },
  {
    id: "rem-acme-anniversary",
    category: "reminder",
    trigger_type: "time",
    headline: "Acme contract anniversary on May 15.",
    created: isoAt(-15, 10, 0),
    remind_at: isoAt(15, 8, 0),
    state: "pending",
  },
  {
    id: "rem-watch-bob",
    category: "reminder",
    trigger_type: "condition",
    headline: "Watching Bob's trajectory.",
    created: isoAt(-22, 14, 0),
    state: "pending",
    condition: "bob signals",
    signals: [],
  },
  {
    id: "rem-watch-acme",
    category: "reminder",
    trigger_type: "condition",
    headline: "Watching Acme renewal.",
    created: isoAt(-22, 14, 0),
    state: "pending",
    condition: "acme renewal-related activity",
    signals: [
      {
        date: isoAt(-18, 9, 0).slice(0, 10),
        description: "Acme renewal call (sentiment positive)",
      },
      {
        date: isoAt(-13, 11, 0).slice(0, 10),
        description: "Acme requested audit logs (added to feature ask)",
      },
      {
        date: isoAt(-7, 14, 0).slice(0, 10),
        description: "Acme contract anniversary (May 15)",
      },
    ],
  },
];
