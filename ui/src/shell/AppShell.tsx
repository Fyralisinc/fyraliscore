import type { ReactNode } from "react";

export type SidebarMode = "expanded" | "collapsed";

export interface AppShellProps {
  sidebar: ReactNode;
  main: ReactNode;
  inspector?: ReactNode;
  // "collapsed" shrinks the sidebar flex item to an icon rail. The
  // sidebar itself decides what to render at each width.
  sidebarMode?: SidebarMode;
}

export function AppShell({
  sidebar,
  main,
  inspector,
  sidebarMode = "expanded",
}: AppShellProps) {
  return (
    <div className="fy-shell" data-sidebar-mode={sidebarMode}>
      <aside
        className={`fy-shell__sidebar fy-shell__sidebar--${sidebarMode}`}
        aria-label="Primary navigation"
      >
        {sidebar}
      </aside>
      <main className="fy-shell__main">{main}</main>
      {inspector ? (
        <aside className="fy-shell__inspector" aria-label="Inspector">
          {inspector}
        </aside>
      ) : null}
    </div>
  );
}

export default AppShell;
