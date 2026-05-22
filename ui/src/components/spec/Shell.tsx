import type { ReactNode } from "react";

interface SpecShellProps {
  sidebar: ReactNode;
  main: ReactNode;
  inspector?: ReactNode;
}

// Top-level spec layout. Sidebar | main column | optional inspector.
// Matches §15.1: 248px sidebar, fluid main, 360–420px inspector.
export function SpecShell({ sidebar, main, inspector }: SpecShellProps) {
  return (
    <div className="fx-app" data-inspector={inspector ? "open" : "closed"}>
      <aside className="fx-app__sidebar" aria-label="Primary navigation">
        {sidebar}
      </aside>
      <main className="fx-app__main">{main}</main>
      {inspector ? (
        <aside className="fx-app__inspector" aria-label="Inspector">
          {inspector}
        </aside>
      ) : null}
    </div>
  );
}
