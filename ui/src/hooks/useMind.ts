import { useCallback, useEffect, useMemo, useState } from "react";
import {
  MindStore,
  makeId,
  subscribe,
} from "@/components/mind/store";
import { parseInput } from "@/components/mind/parser";
import type {
  LayerStripCounts,
  Loop,
  MindItem,
  Note,
  Reminder,
} from "@/components/mind/types";

const NOW = () => new Date();
const DAY_MS = 24 * 60 * 60 * 1000;

export function isAging(loop: Loop, now: Date = NOW()): boolean {
  if (loop.state !== "open") return false;
  return now.getTime() - new Date(loop.created).getTime() > 30 * DAY_MS;
}

export function ageDays(iso: string, now: Date = NOW()): number {
  return Math.floor((now.getTime() - new Date(iso).getTime()) / DAY_MS);
}

export function useMind() {
  const [tick, setTick] = useState(0);
  useEffect(() => subscribe(() => setTick((t) => t + 1)), []);

  // Re-read whenever tick changes.
  const data = useMemo(() => MindStore.read(), [tick]);

  const counts = useMemo<LayerStripCounts>(() => {
    const now = NOW();
    const today = now.toISOString().slice(0, 10);
    const openLoops = data.loops.filter((l) => l.state === "open");
    const aging = openLoops.filter((l) => isAging(l, now)).length;
    const fired = data.reminders.filter((r) => r.state === "fired").length;
    const pending = data.reminders.filter((r) => r.state === "pending").length;
    const notesToday = data.notes.filter(
      (n) => n.created.slice(0, 10) === today
    ).length;
    const all =
      openLoops.length +
      data.notes.filter((n) => n.state === "captured").length +
      data.reminders.filter(
        (r) => r.state === "pending" || r.state === "fired"
      ).length;
    return {
      all: { items: all, due: fired + aging },
      loops: { count: openLoops.length, aging },
      notes: { count: data.notes.length, today: notesToday },
      reminders: { count: pending + fired, pending },
    };
  }, [data]);

  const addFromInput = useCallback((raw: string): MindItem | null => {
    const text = raw.trim();
    if (!text) return null;
    const parsed = parseInput(text);
    const nowIso = new Date().toISOString();
    if (parsed.kind === "loop") {
      const loop: Loop = {
        id: makeId("loop"),
        category: "loop",
        kind: parsed.loop_kind,
        headline: parsed.headline,
        created: nowIso,
        updated: nowIso,
        state: "open",
        from_today: false,
        person: parsed.person ?? null,
        user_notes: [],
      };
      MindStore.addLoop(loop);
      return loop;
    }
    if (parsed.kind === "note") {
      const note: Note = {
        id: makeId("note"),
        category: "note",
        headline: parsed.headline,
        created: nowIso,
        state: "captured",
        source: parsed.source,
      };
      MindStore.addNote(note);
      return note;
    }
    const rem: Reminder = {
      id: makeId("rem"),
      category: "reminder",
      trigger_type: parsed.trigger_type,
      headline: parsed.headline,
      created: nowIso,
      state: "pending",
      remind_at: parsed.remind_at,
      condition: parsed.condition,
      signals: parsed.trigger_type === "condition" ? [] : undefined,
    };
    MindStore.addReminder(rem);
    return rem;
  }, []);

  const changeCategory = useCallback(
    (id: string, target: "loop" | "note" | "reminder") => {
      const all: MindItem[] = [...data.loops, ...data.notes, ...data.reminders];
      const item = all.find((x) => x.id === id);
      if (!item) return;
      MindStore.removeItem(id);
      const nowIso = new Date().toISOString();
      if (target === "loop" && item.category !== "loop") {
        const loop: Loop = {
          id: makeId("loop"),
          category: "loop",
          kind: "action",
          headline: item.headline,
          created: nowIso,
          updated: nowIso,
          state: "open",
          from_today: false,
          person: null,
          user_notes: [],
        };
        MindStore.addLoop(loop);
      } else if (target === "note" && item.category !== "note") {
        const note: Note = {
          id: makeId("note"),
          category: "note",
          headline: item.headline,
          created: nowIso,
          state: "captured",
        };
        MindStore.addNote(note);
      } else if (target === "reminder" && item.category !== "reminder") {
        const rem: Reminder = {
          id: makeId("rem"),
          category: "reminder",
          trigger_type: "time",
          headline: item.headline,
          created: nowIso,
          state: "pending",
        };
        MindStore.addReminder(rem);
      }
    },
    [data]
  );

  const resolveLoop = useCallback((id: string) => {
    MindStore.resolveLoop(id);
  }, []);

  const removeItem = useCallback((id: string) => {
    MindStore.removeItem(id);
  }, []);

  const acknowledgeReminder = useCallback((id: string) => {
    MindStore.updateItem(id, { state: "acknowledged" });
  }, []);

  const snoozeReminder = useCallback(
    (id: string, until: Date) => {
      MindStore.updateItem(id, {
        state: "pending",
        remind_at: until.toISOString(),
      } as Partial<Reminder>);
    },
    []
  );

  const promoteToToday = useCallback((id: string) => {
    MindStore.updateItem(id, { state: "promoted-today" } as Partial<Loop>);
  }, []);

  const addUserNote = useCallback((loopId: string, text: string) => {
    MindStore.appendUserNote(loopId, text);
  }, []);

  const promoteNoteTo = useCallback(
    (
      noteId: string,
      target: "loop" | "reminder",
      extras?: {
        loop_kind?: Loop["kind"];
        person?: string | null;
        thinking?: string;
        trigger_type?: Reminder["trigger_type"];
        remind_at?: string;
        condition?: string;
      }
    ) => {
      const note = data.notes.find((n) => n.id === noteId);
      if (!note) return;
      const nowIso = new Date().toISOString();
      MindStore.removeItem(noteId);
      if (target === "loop") {
        const loop: Loop = {
          id: makeId("loop"),
          category: "loop",
          kind: extras?.loop_kind ?? "action",
          headline: note.headline,
          created: nowIso,
          updated: nowIso,
          state: "open",
          from_today: false,
          person: extras?.person ?? null,
          substrate_stance: note.substrate_stance,
          user_notes: extras?.thinking
            ? [{ date: nowIso.slice(0, 10), text: extras.thinking }]
            : [],
        };
        MindStore.addLoop(loop);
      } else {
        const rem: Reminder = {
          id: makeId("rem"),
          category: "reminder",
          trigger_type: extras?.trigger_type ?? "time",
          headline: note.headline,
          created: nowIso,
          state: "pending",
          remind_at: extras?.remind_at,
          condition: extras?.condition,
          signals: extras?.trigger_type === "condition" ? [] : undefined,
        };
        MindStore.addReminder(rem);
      }
    },
    [data]
  );

  return {
    loops: data.loops,
    notes: data.notes,
    reminders: data.reminders,
    counts,
    addFromInput,
    changeCategory,
    resolveLoop,
    removeItem,
    acknowledgeReminder,
    snoozeReminder,
    promoteToToday,
    addUserNote,
    promoteNoteTo,
  };
}
