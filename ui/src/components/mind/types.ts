// Driftwood — My Mind page types.
// See DRIFTWOOD_MY_MIND_SPEC.md for canonical shapes.

export type MindLayerId = "all" | "loops" | "notes" | "reminders";

export type LoopKind = "action" | "concern" | "question";
export type LoopState = "open" | "resolved" | "promoted-today" | "promoted-decision";
export type NoteState = "captured" | "promoted" | "removed";
export type ReminderTriggerType = "time" | "condition";
export type ReminderState = "pending" | "fired" | "acknowledged";

export type MindRefKind = "loop" | "note" | "reminder" | "person" | "customer" | "decision";
export type ShapeRef = { type: MindRefKind; id: string; text: string };
export type ShapeToken =
  | { kind: "text"; text: string }
  | { kind: "ref"; ref: ShapeRef };

export type UserNote = {
  date: string;       // ISO date
  text: string;
};

export type WatchingSignal = {
  date: string;        // ISO date
  description: string;
};

export type Loop = {
  id: string;
  category: "loop";
  kind: LoopKind;
  headline: string;
  created: string;     // ISO datetime
  updated: string;     // ISO datetime
  state: LoopState;
  from_today: boolean;
  today_card_id?: string;
  person?: string | null;
  substrate_evidence?: string;
  substrate_stance?: string;
  user_notes: UserNote[];
};

export type Note = {
  id: string;
  category: "note";
  headline: string;
  created: string;     // ISO datetime
  state: NoteState;
  source?: string;
  substrate_stance?: string;
};

export type Reminder = {
  id: string;
  category: "reminder";
  trigger_type: ReminderTriggerType;
  headline: string;
  created: string;     // ISO datetime
  state: ReminderState;
  // Time trigger only
  remind_at?: string;
  fired_at?: string;
  // Condition trigger only
  condition?: string;
  signals?: WatchingSignal[];
};

export type MindItem = Loop | Note | Reminder;

export type LayerStripCounts = {
  all: { items: number; due: number };
  loops: { count: number; aging: number };
  notes: { count: number; today: number };
  reminders: { count: number; pending: number };
};

export type MindFilters = {
  categories: Set<"loop" | "note" | "reminder">;
  age: "all" | "aging" | "recent";
  person: string | null;
  search: string;
};

export type ParsedItem =
  | {
      kind: "loop";
      loop_kind: LoopKind;
      headline: string;
      person?: string | null;
    }
  | {
      kind: "note";
      headline: string;
      source?: string;
    }
  | {
      kind: "reminder";
      trigger_type: ReminderTriggerType;
      headline: string;
      remind_at?: string;
      condition?: string;
    };
