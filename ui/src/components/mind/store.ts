// Local My Mind store — persists user-captured items to localStorage
// so the demo flow survives reloads. The seed data is the canonical
// sample from DRIFTWOOD_MY_MIND_SPEC.md Part 22; user actions append
// to and mutate this baseline.

import { SAMPLE_LOOPS, SAMPLE_NOTES, SAMPLE_REMINDERS } from "./sample-data";
import type { Loop, MindItem, Note, Reminder } from "./types";

const STORAGE_KEY = "driftwood.mind.v1";

type StoreShape = {
  loops: Loop[];
  notes: Note[];
  reminders: Reminder[];
};

function defaultStore(): StoreShape {
  return {
    loops: structuredClone(SAMPLE_LOOPS),
    notes: structuredClone(SAMPLE_NOTES),
    reminders: structuredClone(SAMPLE_REMINDERS),
  };
}

function readRaw(): StoreShape {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return defaultStore();
    const parsed = JSON.parse(raw) as Partial<StoreShape>;
    return {
      loops: parsed.loops ?? [],
      notes: parsed.notes ?? [],
      reminders: parsed.reminders ?? [],
    };
  } catch {
    return defaultStore();
  }
}

function writeRaw(s: StoreShape) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(s));
    // Notify listeners in this tab — storage event only fires across tabs.
    window.dispatchEvent(new CustomEvent("mind:changed"));
  } catch {
    // ignore storage failures
  }
}

export const MindStore = {
  read(): StoreShape {
    return readRaw();
  },

  reset() {
    writeRaw(defaultStore());
  },

  addLoop(loop: Loop) {
    const s = readRaw();
    s.loops = [loop, ...s.loops.filter((l) => l.id !== loop.id)];
    writeRaw(s);
  },

  addNote(note: Note) {
    const s = readRaw();
    s.notes = [note, ...s.notes.filter((n) => n.id !== note.id)];
    writeRaw(s);
  },

  addReminder(rem: Reminder) {
    const s = readRaw();
    s.reminders = [rem, ...s.reminders.filter((r) => r.id !== rem.id)];
    writeRaw(s);
  },

  updateItem(id: string, patch: Partial<MindItem>) {
    const s = readRaw();
    s.loops = s.loops.map((l) => (l.id === id ? ({ ...l, ...patch } as Loop) : l));
    s.notes = s.notes.map((n) => (n.id === id ? ({ ...n, ...patch } as Note) : n));
    s.reminders = s.reminders.map((r) =>
      r.id === id ? ({ ...r, ...patch } as Reminder) : r
    );
    writeRaw(s);
  },

  removeItem(id: string) {
    const s = readRaw();
    s.loops = s.loops.filter((l) => l.id !== id);
    s.notes = s.notes.filter((n) => n.id !== id);
    s.reminders = s.reminders.filter((r) => r.id !== id);
    writeRaw(s);
  },

  resolveLoop(id: string) {
    const s = readRaw();
    s.loops = s.loops.map((l) =>
      l.id === id
        ? { ...l, state: "resolved", updated: new Date().toISOString() }
        : l
    );
    writeRaw(s);
  },

  appendUserNote(loopId: string, text: string) {
    const s = readRaw();
    s.loops = s.loops.map((l) =>
      l.id === loopId
        ? {
            ...l,
            updated: new Date().toISOString(),
            user_notes: [
              { date: new Date().toISOString().slice(0, 10), text },
              ...l.user_notes,
            ],
          }
        : l
    );
    writeRaw(s);
  },
};

export function makeId(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
}

export function subscribe(fn: () => void): () => void {
  const handler = () => fn();
  window.addEventListener("mind:changed", handler);
  window.addEventListener("storage", handler);
  return () => {
    window.removeEventListener("mind:changed", handler);
    window.removeEventListener("storage", handler);
  };
}
