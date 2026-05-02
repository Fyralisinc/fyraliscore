import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Sidebar } from "@/components/Sidebar";
import { ShortcutsOverlay } from "@/components/ShortcutsOverlay";
import { MindLayerStrip } from "@/components/mind/MindLayerStrip";
import { MindNarrativeBand } from "@/components/mind/MindNarrativeBand";
import { MindInputRow } from "@/components/mind/MindInputRow";
import { MindList } from "@/components/mind/MindList";
import { LoopCard } from "@/components/mind/LoopCard";
import { NoteCard } from "@/components/mind/NoteCard";
import { ReminderCard } from "@/components/mind/ReminderCard";
import { PromoteModal } from "@/components/mind/PromoteModal";
import { FilterPanel } from "@/components/mind/FilterPanel";
import { useMind, ageDays, isAging } from "@/hooks/useMind";
import type {
  Loop,
  MindFilters,
  MindLayerId,
  Note,
  Reminder,
} from "@/components/mind/types";

// Driftwood — My Mind page (DRIFTWOOD_MY_MIND_SPEC.md).
// Four layers: All (default), Loops, Notes, Reminders.
export default function MyMind() {
  const navigate = useNavigate();
  const mind = useMind();
  const [layer, setLayer] = useState<MindLayerId>("all");
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [filterOpen, setFilterOpen] = useState(false);
  const [filters, setFilters] = useState<MindFilters>(() => ({
    categories: new Set<"loop" | "note" | "reminder">(["loop", "note", "reminder"]),
    age: "all",
    person: null,
    search: "",
  }));
  const [recentlyCreated, setRecentlyCreated] = useState<
    Map<string, number>
  >(() => new Map());
  const [promote, setPromote] = useState<{
    note: Note;
    target: "loop" | "reminder";
  } | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  // Track recently created items so we can show the parse chip for 60s.
  const tagCreated = useCallback((id: string) => {
    setRecentlyCreated((prev) => {
      const next = new Map(prev);
      next.set(id, Date.now());
      return next;
    });
    window.setTimeout(() => {
      setRecentlyCreated((prev) => {
        const next = new Map(prev);
        next.delete(id);
        return next;
      });
    }, 60_000);
  }, []);

  const onSubmitInput = useCallback(
    (text: string) => {
      const item = mind.addFromInput(text);
      if (item) tagCreated(item.id);
    },
    [mind, tagCreated]
  );

  // Keyboard shortcuts.
  useEffect(() => {
    function isInput(el: Element | null): boolean {
      if (!el) return false;
      const tag = (el as HTMLElement).tagName;
      return (
        tag === "INPUT" ||
        tag === "TEXTAREA" ||
        (el as HTMLElement).isContentEditable
      );
    }
    function onKey(e: KeyboardEvent) {
      const active = document.activeElement;
      if (e.key === "Escape") {
        if (filterOpen) {
          setFilterOpen(false);
          e.preventDefault();
          return;
        }
        if (promote) {
          setPromote(null);
          e.preventDefault();
          return;
        }
        if (shortcutsOpen) {
          setShortcutsOpen(false);
          e.preventDefault();
          return;
        }
        if (isInput(active)) {
          (active as HTMLElement).blur();
          e.preventDefault();
          return;
        }
        return;
      }
      if (isInput(active)) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      switch (e.key) {
        case "n":
        case "N":
          e.preventDefault();
          inputRef.current?.focus();
          break;
        case "/":
          e.preventDefault();
          (document.querySelector(".mind-search") as HTMLInputElement | null)?.focus();
          break;
        case "?":
          e.preventDefault();
          setShortcutsOpen(true);
          break;
        case "1":
          e.preventDefault();
          setLayer("all");
          break;
        case "2":
          e.preventDefault();
          setLayer("loops");
          break;
        case "3":
          e.preventDefault();
          setLayer("notes");
          break;
        case "4":
          e.preventDefault();
          setLayer("reminders");
          break;
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [filterOpen, promote, shortcutsOpen]);

  const nav = useMemo(
    () => [
      {
        id: "primary",
        label: "Surfaces",
        items: [
          { id: "today", label: "Today", active: false },
          { id: "structure", label: "Structure", active: false },
          { id: "history", label: "History", active: false },
          { id: "mind", label: "My Mind", active: true, shortcut: "M" },
          {
            id: "communicate",
            label: "Communicate",
            disabled: true,
            badge: "soon",
          },
        ],
      },
    ],
    []
  );

  // Apply filters and search.
  const visible = useMemo(() => {
    const q = filters.search.trim().toLowerCase();
    function matchesSearch(text: string): boolean {
      if (!q) return true;
      return text.toLowerCase().includes(q);
    }
    function matchesAge(created: string): boolean {
      if (filters.age === "all") return true;
      const days = ageDays(created);
      if (filters.age === "aging") return days >= 30;
      return days < 30;
    }

    const loops = mind.loops.filter((l) => {
      if (!filters.categories.has("loop")) return false;
      const text = `${l.headline} ${l.user_notes
        .map((n) => n.text)
        .join(" ")} ${l.substrate_evidence ?? ""} ${l.substrate_stance ?? ""}`;
      if (!matchesSearch(text)) return false;
      if (!matchesAge(l.created)) return false;
      if (filters.person && l.person !== filters.person) return false;
      return true;
    });
    const notes = mind.notes.filter((n) => {
      if (!filters.categories.has("note")) return false;
      const text = `${n.headline} ${n.source ?? ""} ${n.substrate_stance ?? ""}`;
      if (!matchesSearch(text)) return false;
      if (!matchesAge(n.created)) return false;
      return true;
    });
    const reminders = mind.reminders.filter((r) => {
      if (!filters.categories.has("reminder")) return false;
      const text = `${r.headline} ${(r.signals ?? [])
        .map((s) => s.description)
        .join(" ")} ${r.condition ?? ""}`;
      if (!matchesSearch(text)) return false;
      if (!matchesAge(r.created)) return false;
      return true;
    });
    return { loops, notes, reminders };
  }, [filters, mind.loops, mind.notes, mind.reminders]);

  // Distinct people for the filter dropdown.
  const people = useMemo(() => {
    const set = new Set<string>();
    for (const l of mind.loops) if (l.person) set.add(l.person);
    return Array.from(set).sort();
  }, [mind.loops]);

  // Per-layer rendered output.
  const content = useMemo(
    () => renderContent({ layer, visible, mind, recentlyCreated, setPromote }),
    [layer, visible, mind, recentlyCreated]
  );

  function resetFilters() {
    setFilters({
      categories: new Set<"loop" | "note" | "reminder">(["loop", "note", "reminder"]),
      age: "all",
      person: null,
      search: "",
    });
  }

  return (
    <>
      <div className="cockpit">
        <Sidebar
          brand={{ name: "Driftwood", mark: "D", pulse_day: 4 }}
          nav={nav}
          vitals={[]}
          onNavigate={(_s, item) => {
            if (item === "today") navigate("/");
            else if (item === "structure") navigate("/structure");
            else if (item === "history") navigate("/history");
            else if (item === "mind") navigate("/mind");
          }}
        />

        <main className="structure-main mind-main">
          <MindLayerStrip
            active={layer}
            counts={mind.counts}
            onSwitch={setLayer}
            onShortcuts={() => setShortcutsOpen(true)}
          />

          <MindNarrativeBand
            layer={layer}
            loops={mind.loops}
            notes={mind.notes}
            reminders={mind.reminders}
            onRef={(id) => {
              setFilters({ ...filters, search: id });
            }}
          />

          <div className="mind-controls-wrap">
            <MindInputRow
              ref={inputRef}
              onSubmit={onSubmitInput}
              onOpenFilter={() => setFilterOpen((v) => !v)}
              onSearchChange={(s) => setFilters({ ...filters, search: s })}
              searchValue={filters.search}
              filterActive={filterOpen}
            />
            {filterOpen ? (
              <FilterPanel
                filters={filters}
                people={people}
                onChange={setFilters}
                onClose={() => setFilterOpen(false)}
                onReset={resetFilters}
              />
            ) : null}
          </div>

          <div className="mind-layer-content">{content}</div>
        </main>
      </div>

      {promote ? (
        <PromoteModal
          note={promote.note}
          target={promote.target}
          onCancel={() => setPromote(null)}
          onConfirm={(extras) => {
            mind.promoteNoteTo(promote.note.id, promote.target, extras);
            setPromote(null);
          }}
        />
      ) : null}

      {shortcutsOpen ? (
        <ShortcutsOverlay onClose={() => setShortcutsOpen(false)} />
      ) : null}
    </>
  );
}

type RenderArgs = {
  layer: MindLayerId;
  visible: { loops: Loop[]; notes: Note[]; reminders: Reminder[] };
  mind: ReturnType<typeof useMind>;
  recentlyCreated: Map<string, number>;
  setPromote: (p: { note: Note; target: "loop" | "reminder" } | null) => void;
};

function renderContent({
  layer,
  visible,
  mind,
  recentlyCreated,
  setPromote,
}: RenderArgs) {
  const wasRecent = (id: string) => recentlyCreated.has(id);
  const stillRecent = (id: string) => recentlyCreated.has(id);

  const renderLoop = (l: Loop) => (
    <LoopCard
      key={l.id}
      loop={l}
      justCreated={wasRecent(l.id)}
      showParseChip={stillRecent(l.id)}
      onResolve={() => mind.resolveLoop(l.id)}
      onSendToToday={() => mind.promoteToToday(l.id)}
      onAddNote={(t) => mind.addUserNote(l.id, t)}
      onChangeCategory={(target) => mind.changeCategory(l.id, target)}
    />
  );
  const renderNote = (n: Note) => (
    <NoteCard
      key={n.id}
      note={n}
      justCreated={wasRecent(n.id)}
      showParseChip={stillRecent(n.id)}
      onPromoteToLoop={() => setPromote({ note: n, target: "loop" })}
      onPromoteToReminder={() => setPromote({ note: n, target: "reminder" })}
      onRemove={() => mind.removeItem(n.id)}
      onChangeCategory={(target) => mind.changeCategory(n.id, target)}
    />
  );
  const renderReminder = (r: Reminder) => (
    <ReminderCard
      key={r.id}
      reminder={r}
      justCreated={wasRecent(r.id)}
      showParseChip={stillRecent(r.id)}
      onMarkDone={() => mind.acknowledgeReminder(r.id)}
      onSnooze={(d) => mind.snoozeReminder(r.id, d)}
      onStopWatching={() => mind.removeItem(r.id)}
      onChangeCategory={(target) => mind.changeCategory(r.id, target)}
    />
  );

  if (layer === "all") {
    const openLoops = visible.loops.filter((l) => l.state === "open");
    const aging = openLoops.filter((l) => isAging(l));
    const fresh = openLoops.filter((l) => !isAging(l));
    const fired = visible.reminders.filter((r) => r.state === "fired");
    const pending = visible.reminders.filter((r) => r.state === "pending");

    const demanding = [...fired, ...aging];
    const sections: { id: string; title: string; count: number; children: React.ReactNode[] }[] = [];
    if (demanding.length > 0) {
      sections.push({
        id: "demanding",
        title: "DEMANDING ATTENTION",
        count: demanding.length,
        children: demanding.map((it) =>
          it.category === "loop" ? renderLoop(it as Loop) : renderReminder(it as Reminder)
        ),
      });
    }
    if (fresh.length > 0) {
      sections.push({
        id: "loops",
        title: "LOOPS",
        count: fresh.length,
        children: [...fresh]
          .sort(
            (a, b) =>
              new Date(b.created).getTime() - new Date(a.created).getTime()
          )
          .map(renderLoop),
      });
    }
    if (visible.notes.length > 0) {
      sections.push({
        id: "notes",
        title: "NOTES",
        count: visible.notes.length,
        children: [...visible.notes]
          .sort(
            (a, b) =>
              new Date(b.created).getTime() - new Date(a.created).getTime()
          )
          .map(renderNote),
      });
    }
    if (pending.length > 0) {
      sections.push({
        id: "reminders",
        title: "REMINDERS",
        count: pending.length,
        children: [...pending]
          .sort((a, b) => sortReminder(a, b))
          .map(renderReminder),
      });
    }

    return (
      <MindList
        sections={sections}
        emptyState={renderEmpty(visible)}
      />
    );
  }

  if (layer === "loops") {
    const open = visible.loops.filter((l) => l.state === "open");
    const aging = open.filter((l) => isAging(l));
    const active = open.filter((l) => !isAging(l));
    const recentlyResolved = visible.loops
      .filter((l) => l.state === "resolved")
      .filter((l) => ageDays(l.updated) < 7);
    const sections: { id: string; title: string; count: number; children: React.ReactNode[] }[] = [];
    if (aging.length > 0) {
      sections.push({
        id: "aging",
        title: "AGING",
        count: aging.length,
        children: aging
          .sort((a, b) => new Date(a.created).getTime() - new Date(b.created).getTime())
          .map(renderLoop),
      });
    }
    if (active.length > 0) {
      sections.push({
        id: "active",
        title: "ACTIVE",
        count: active.length,
        children: active
          .sort((a, b) => new Date(b.created).getTime() - new Date(a.created).getTime())
          .map(renderLoop),
      });
    }
    if (recentlyResolved.length > 0) {
      sections.push({
        id: "resolved",
        title: "RECENTLY RESOLVED",
        count: recentlyResolved.length,
        children: recentlyResolved.map(renderLoop),
      });
    }
    return (
      <MindList
        sections={sections}
        emptyState={
          <p className="empty-state-text">
            No active loops. When you put something on your mind that needs
            tracking, it'll appear here.
          </p>
        }
      />
    );
  }

  if (layer === "notes") {
    const buckets = bucketNotes(visible.notes);
    return (
      <MindList
        sections={buckets.map((b) => ({
          id: b.id,
          title: b.title,
          count: b.items.length,
          children: b.items.map(renderNote),
        }))}
        emptyState={
          <p className="empty-state-text">
            No notes captured. Use Notes to externalize things you've heard or
            want to come back to.
          </p>
        }
      />
    );
  }

  // reminders
  const fired = visible.reminders.filter((r) => r.state === "fired");
  const pendingTime = visible.reminders.filter(
    (r) => r.state === "pending" && r.trigger_type === "time"
  );
  const pendingWatch = visible.reminders.filter(
    (r) => r.state === "pending" && r.trigger_type === "condition"
  );
  const recentlyDone = visible.reminders.filter(
    (r) => r.state === "acknowledged"
  );
  const sections: { id: string; title: string; count: number; children: React.ReactNode[] }[] = [];
  if (fired.length > 0) {
    sections.push({
      id: "fired",
      title: "FIRED",
      count: fired.length,
      children: fired.map(renderReminder),
    });
  }
  if (pendingTime.length > 0) {
    sections.push({
      id: "pending-time",
      title: "PENDING — TIME",
      count: pendingTime.length,
      children: [...pendingTime].sort(sortReminder).map(renderReminder),
    });
  }
  if (pendingWatch.length > 0) {
    sections.push({
      id: "pending-watch",
      title: "PENDING — WATCHING",
      count: pendingWatch.length,
      children: [...pendingWatch]
        .sort((a, b) => (b.signals?.length ?? 0) - (a.signals?.length ?? 0))
        .map(renderReminder),
    });
  }
  if (recentlyDone.length > 0) {
    sections.push({
      id: "recent-done",
      title: "RECENTLY COMPLETED",
      count: recentlyDone.length,
      children: recentlyDone.map(renderReminder),
    });
  }
  return (
    <MindList
      sections={sections}
      emptyState={
        <p className="empty-state-text">
          No reminders set. Reminders fire when conditions are met — either a
          time you specify, or activity the substrate detects.
        </p>
      }
    />
  );
}

function renderEmpty(v: { loops: Loop[]; notes: Note[]; reminders: Reminder[] }) {
  if (v.loops.length === 0 && v.notes.length === 0 && v.reminders.length === 0) {
    return (
      <div className="empty-state-overlay">
        <p className="empty-state-text">
          Nothing in your mind right now. Type below to capture whatever you're
          carrying.
        </p>
        <p className="empty-state-attribution">— Driftwood</p>
      </div>
    );
  }
  return (
    <div className="filter-zero">
      <p>No items match your filters.</p>
    </div>
  );
}

function bucketNotes(notes: Note[]): { id: string; title: string; items: Note[] }[] {
  const today: Note[] = [];
  const week: Note[] = [];
  const month: Note[] = [];
  const older: Note[] = [];
  const now = new Date();
  for (const n of notes) {
    const days = ageDays(n.created, now);
    if (days < 1) today.push(n);
    else if (days < 7) week.push(n);
    else if (days < 30) month.push(n);
    else older.push(n);
  }
  const sortDesc = (a: Note, b: Note) =>
    new Date(b.created).getTime() - new Date(a.created).getTime();
  const out: { id: string; title: string; items: Note[] }[] = [];
  if (today.length) out.push({ id: "today", title: "TODAY", items: today.sort(sortDesc) });
  if (week.length) out.push({ id: "week", title: "THIS WEEK", items: week.sort(sortDesc) });
  if (month.length) out.push({ id: "month", title: "THIS MONTH", items: month.sort(sortDesc) });
  if (older.length) out.push({ id: "older", title: "OLDER", items: older.sort(sortDesc) });
  return out;
}

function sortReminder(a: Reminder, b: Reminder): number {
  const at = a.remind_at ? new Date(a.remind_at).getTime() : 0;
  const bt = b.remind_at ? new Date(b.remind_at).getTime() : 0;
  return at - bt;
}
