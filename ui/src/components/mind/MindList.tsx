import { ReactNode } from "react";

// Spec Part 9 — All view section structure with bucket headers.
type Props = {
  sections: { id: string; title: string; count: number; children: ReactNode }[];
  emptyState?: ReactNode;
};

export function MindList({ sections, emptyState }: Props) {
  const visible = sections.filter(
    (s) => Array.isArray(s.children) ? (s.children as unknown[]).length > 0 : !!s.children
  );

  if (visible.length === 0 && emptyState) {
    return <div className="mind-list">{emptyState}</div>;
  }

  return (
    <div className="mind-list" aria-label="Items in your mind">
      {visible.map((s) => (
        <section key={s.id} className="mind-section">
          <header className="section-header">
            <span className="section-title">{s.title}</span>
            <span className="section-count">({s.count})</span>
            <span className="section-rule" />
          </header>
          <div className="section-items">{s.children}</div>
        </section>
      ))}
    </div>
  );
}
