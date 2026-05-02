import { useMemo } from "react";
import { isAging } from "@/hooks/useMind";
import type {
  Loop,
  MindLayerId,
  Note,
  Reminder,
  ShapeToken,
} from "./types";

// Spec Part 4 — narrative band, single zone, statement spans full width.
// Statement is generated from the current state of the mind store. References
// to specific items are emitted as `<ref>` tokens that filter the list.

type Props = {
  layer: MindLayerId;
  loops: Loop[];
  notes: Note[];
  reminders: Reminder[];
  onRef: (id: string) => void;
};

export function MindNarrativeBand({
  layer,
  loops,
  notes,
  reminders,
  onRef,
}: Props) {
  const tokens = useMemo<ShapeToken[]>(
    () => buildStatement(layer, loops, notes, reminders),
    [layer, loops, notes, reminders]
  );

  return (
    <section className="narrative-band mind-narrative-band" aria-label="Mind state summary">
      <div className="shape-statement mind-shape-statement">
        <p className="shape-statement-text">
          {tokens.map((tok, i) =>
            tok.kind === "text" ? (
              <span key={i}>{tok.text}</span>
            ) : (
              <button
                key={i}
                type="button"
                className="ref"
                data-ref-type={tok.ref.type}
                onClick={() => onRef(tok.ref.id)}
              >
                {tok.ref.text}
              </button>
            )
          )}
        </p>
      </div>
    </section>
  );
}

function t(text: string): ShapeToken {
  return { kind: "text", text };
}

function ref(
  type: "loop" | "note" | "reminder",
  id: string,
  text: string
): ShapeToken {
  return { kind: "ref", ref: { type, id, text } };
}

function buildStatement(
  layer: MindLayerId,
  loops: Loop[],
  notes: Note[],
  reminders: Reminder[]
): ShapeToken[] {
  const now = new Date();
  const openLoops = loops.filter((l) => l.state === "open");
  const aging = openLoops.filter((l) => isAging(l, now));
  const fired = reminders.filter((r) => r.state === "fired");
  const pending = reminders.filter((r) => r.state === "pending");
  const total =
    openLoops.length +
    notes.filter((n) => n.state === "captured").length +
    pending.length +
    fired.length;

  if (total === 0) {
    return [
      t(
        "Nothing in your mind right now. Type below to capture whatever you're carrying."
      ),
    ];
  }

  if (layer === "loops") {
    if (openLoops.length === 0) {
      return [
        t(
          "No active loops. When you put something on your mind that needs tracking, it'll appear here."
        ),
      ];
    }
    const oldest = [...openLoops].sort(
      (a, b) =>
        new Date(a.created).getTime() - new Date(b.created).getTime()
    )[0];
    const oldestDays = Math.floor(
      (now.getTime() - new Date(oldest.created).getTime()) / 86_400_000
    );
    const tokens: ShapeToken[] = [
      t(`${openLoops.length} active loops. `),
    ];
    if (aging.length > 0) {
      tokens.push(t(`${aging.length} ${aging.length === 1 ? "has" : "have"} been here over 30 days. `));
    }
    tokens.push(t(`The "`));
    tokens.push(ref("loop", oldest.id, oldest.headline.replace(/\.$/, "")));
    tokens.push(t(`" loop has been here longest at ${oldestDays} days.`));
    return tokens;
  }

  if (layer === "notes") {
    const captured = notes.filter((n) => n.state === "captured");
    if (captured.length === 0) {
      return [
        t(
          "No notes captured. Use Notes to externalize things you've heard or want to come back to."
        ),
      ];
    }
    const recent = captured[0];
    return [
      t(`${captured.length} ${captured.length === 1 ? "note" : "notes"} captured. `),
      t(`Most recent: "`),
      ref("note", recent.id, recent.headline.replace(/\.$/, "")),
      t(`".`),
    ];
  }

  if (layer === "reminders") {
    if (pending.length === 0 && fired.length === 0) {
      return [
        t(
          "No reminders set. Reminders fire when conditions are met — either a time you specify, or activity the substrate detects."
        ),
      ];
    }
    const tokens: ShapeToken[] = [
      t(`${pending.length + fired.length} ${pending.length + fired.length === 1 ? "reminder" : "reminders"}. `),
    ];
    if (fired.length > 0) {
      tokens.push(
        t(
          `${fired.length} ${fired.length === 1 ? "fired and is" : "fired and are"} awaiting acknowledgement. `
        )
      );
    }
    const watching = pending.filter((r) => r.trigger_type === "condition");
    const withSignals = watching.filter(
      (r) => (r.signals?.length ?? 0) > 0
    );
    if (withSignals.length > 0) {
      tokens.push(
        t(
          `Watching list has ${withSignals.length} ${withSignals.length === 1 ? "item" : "items"} with new signals.`
        )
      );
    } else if (watching.length > 0) {
      tokens.push(t(`Your watching list has been quiet.`));
    }
    return tokens;
  }

  // ALL view
  const tokens: ShapeToken[] = [
    t(`You're carrying ${total} ${total === 1 ? "item" : "items"} in your mind.`),
  ];
  if (fired.length > 0) {
    tokens.push(t(` ${fired.length} ${fired.length === 1 ? "reminder is" : "reminders are"} due.`));
  }
  if (aging.length > 0) {
    if (aging.length === 1) {
      tokens.push(t(` One loop has been here over a month — `));
      tokens.push(ref("loop", aging[0].id, aging[0].headline.replace(/\.$/, "")));
      tokens.push(t(`.`));
    } else {
      tokens.push(t(` ${aging.length} loops have been here over a month — ready to close?`));
    }
  } else if (total <= 6) {
    tokens.push(t(` Your mind is quiet.`));
  }
  return tokens;
}
