// Lightweight client-side substrate parser for My Mind input.
// Mirrors the substrate-as-parser model in DRIFTWOOD_MY_MIND_SPEC.md
// Part 5.3. The real backend may replace this with a ML/LLM-driven
// parser; this file gives us deterministic mock behavior for the demo.

import type { ParsedItem, LoopKind, ReminderTriggerType } from "./types";

const PEOPLE = ["sarah", "marcus", "david", "bob", "alex", "jen", "priya"];

function detectPerson(text: string): string | null {
  const lower = text.toLowerCase();
  for (const p of PEOPLE) {
    const re = new RegExp(`\\b${p}\\b`, "i");
    if (re.test(lower)) return p[0].toUpperCase() + p.slice(1);
  }
  return null;
}

function nextDow(weekday: number): Date {
  // weekday: 0=Sun .. 6=Sat
  const now = new Date();
  const diff = (weekday + 7 - now.getDay()) % 7 || 7;
  const d = new Date(now);
  d.setDate(now.getDate() + diff);
  d.setHours(8, 0, 0, 0);
  return d;
}

function detectTime(text: string): string | null {
  const lower = text.toLowerCase();
  const dows: Record<string, number> = {
    sunday: 0, sun: 0,
    monday: 1, mon: 1,
    tuesday: 2, tue: 2, tues: 2,
    wednesday: 3, wed: 3,
    thursday: 4, thu: 4, thurs: 4,
    friday: 5, fri: 5,
    saturday: 6, sat: 6,
  };
  for (const word of Object.keys(dows)) {
    const re = new RegExp(`\\b${word}\\b`);
    if (re.test(lower)) {
      const d = nextDow(dows[word]);
      return d.toISOString();
    }
  }
  if (/\btomorrow\b/.test(lower)) {
    const d = new Date();
    d.setDate(d.getDate() + 1);
    d.setHours(8, 0, 0, 0);
    return d.toISOString();
  }
  if (/\bnext week\b/.test(lower)) {
    return nextDow(1).toISOString();
  }
  // Date pattern: "May 15", "Apr 30"
  const months: Record<string, number> = {
    jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
    jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
  };
  const m = lower.match(/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})\b/);
  if (m) {
    const mo = months[m[1]];
    const day = parseInt(m[2], 10);
    const now = new Date();
    let year = now.getFullYear();
    if (mo < now.getMonth() || (mo === now.getMonth() && day < now.getDate())) {
      year += 1;
    }
    const d = new Date(year, mo, day, 8, 0, 0);
    return d.toISOString();
  }
  return null;
}

function classifyLoopKind(text: string): LoopKind {
  const lower = text.toLowerCase();
  if (
    /\b(should|whether|do we|can we|when do|when should|why)\b/.test(lower) ||
    /\?$/.test(lower)
  ) {
    return "question";
  }
  if (/\b(concern|worried|tight|risk|slipping|losing|drift)\b/.test(lower)) {
    return "concern";
  }
  return "action";
}

export function parseInput(raw: string): ParsedItem {
  const text = raw.trim();
  const lower = text.toLowerCase();

  // Watching → condition reminder
  if (/^watching\b/.test(lower) || /\b(watch out for|keep an eye on)\b/.test(lower)) {
    const condition = text.replace(/^watching\s+/i, "").trim();
    return {
      kind: "reminder",
      trigger_type: "condition" as ReminderTriggerType,
      headline: text.endsWith(".") ? text : `${text}.`,
      condition,
    };
  }

  // Explicit time-trigger phrasing
  const remindMatch = /^(remind me|reminder)/i.test(lower);
  const time = detectTime(text);

  if (remindMatch || /(send|email|ping|follow up|call|meet)/i.test(lower)) {
    if (time) {
      return {
        kind: "reminder",
        trigger_type: "time",
        headline: text.endsWith(".") ? text : `${text}.`,
        remind_at: time,
      };
    }
  }

  // Notes — passive captures from observations
  if (/\b(read|look into|dig into|book|article|saw|heard|mentioned at)\b/i.test(lower)) {
    const person = detectPerson(text);
    return {
      kind: "note",
      headline: text.endsWith(".") ? text : `${text}.`,
      source: person ?? undefined,
    };
  }

  // Default: it's a loop
  const person = detectPerson(text);
  return {
    kind: "loop",
    loop_kind: classifyLoopKind(text),
    headline: text.endsWith(".") || text.endsWith("?") ? text : `${text}.`,
    person,
  };
}
