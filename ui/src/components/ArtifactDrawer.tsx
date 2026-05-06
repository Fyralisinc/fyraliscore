import { useCallback, useEffect, useState } from "react";
import { getArtifact } from "@/api/today-client";
import type {
  ArtifactDetail,
  ArtifactKind,
  ArtifactLink,
  ArtifactSection,
} from "@/api/today-types";

type Target = { kind: ArtifactKind; id: string };
type Props = {
  target: Target | null;
  onClose: () => void;
};

const KIND_LABEL: Record<ArtifactKind, string> = {
  actor: "Actor",
  commitment: "Commitment",
  goal: "Goal",
  decision: "Decision",
  resource: "Resource",
  observation: "Evidence",
  model: "Belief",
};

// Right-side drawer with an internal navigation stack so clicking a
// link inside the drawer drills into that artifact. Esc / overlay /
// close-button pop the whole stack and close the drawer; the Back
// button pops one frame.
export function ArtifactDrawer({ target, onClose }: Props) {
  const [stack, setStack] = useState<Target[]>([]);
  const [detail, setDetail] = useState<ArtifactDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // External target prop drives a stack reset.
  useEffect(() => {
    setStack(target ? [target] : []);
  }, [target]);

  const current = stack[stack.length - 1] ?? null;

  useEffect(() => {
    if (!current) {
      setDetail(null);
      setError(null);
      return;
    }
    const ctrl = new AbortController();
    setLoading(true);
    setError(null);
    setDetail(null);
    getArtifact(current.kind, current.id, ctrl.signal)
      .then((d) => setDetail(d))
      .catch((e) => {
        if (ctrl.signal.aborted) return;
        setError(e instanceof Error ? e.message : "Failed to load");
      })
      .finally(() => {
        if (!ctrl.signal.aborted) setLoading(false);
      });
    return () => ctrl.abort();
  }, [current?.kind, current?.id]);

  useEffect(() => {
    if (!current) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    }
    document.addEventListener("keydown", onKey, true);
    return () => document.removeEventListener("keydown", onKey, true);
  }, [current, onClose]);

  const onPushLink = useCallback((link: ArtifactLink) => {
    setStack((s) => [...s, { kind: link.type, id: link.id }]);
  }, []);

  const onBack = useCallback(() => {
    setStack((s) => (s.length > 1 ? s.slice(0, -1) : s));
  }, []);

  if (!current) return null;
  return (
    <>
      <div className="artifact-drawer-overlay" onClick={onClose} />
      <aside
        className="artifact-drawer"
        role="dialog"
        aria-label="Artifact details"
      >
        <header className="artifact-drawer-head">
          <div className="artifact-drawer-head-left">
            {stack.length > 1 ? (
              <button
                type="button"
                className="artifact-drawer-back"
                onClick={onBack}
                aria-label="Back"
                title="Back"
              >
                ←
              </button>
            ) : null}
            <span className="artifact-drawer-kind">
              {KIND_LABEL[current.kind] ?? current.kind}
            </span>
          </div>
          <button
            type="button"
            className="artifact-drawer-close"
            onClick={onClose}
            aria-label="Close"
          >
            ×
          </button>
        </header>
        <div className="artifact-drawer-body">
          {loading ? <p className="artifact-drawer-loading">Loading…</p> : null}
          {error ? <p className="artifact-drawer-error">{error}</p> : null}
          {detail ? (
            <>
              <h3 className="artifact-drawer-title">{detail.title}</h3>
              <p className="artifact-drawer-subtitle">{detail.subtitle}</p>
              {detail.summary ? (
                <p
                  className="artifact-drawer-summary"
                  dangerouslySetInnerHTML={{ __html: detail.summary }}
                />
              ) : null}
              {detail.sections.map((sec, i) => (
                <DrawerSection key={i} section={sec} onLink={onPushLink} />
              ))}
            </>
          ) : null}
        </div>
      </aside>
    </>
  );
}

function DrawerSection({
  section,
  onLink,
}: {
  section: ArtifactSection;
  onLink: (link: ArtifactLink) => void;
}) {
  if (section.kind === "fields") {
    return (
      <section className="drawer-section drawer-section-fields">
        {section.title ? (
          <h4 className="drawer-section-title">{section.title}</h4>
        ) : null}
        <dl className="artifact-drawer-fields">
          {section.rows.map((f, i) => (
            <div key={i} className="artifact-drawer-field">
              <dt>{f.label}</dt>
              <dd>{f.value}</dd>
            </div>
          ))}
        </dl>
      </section>
    );
  }
  if (section.kind === "narrative") {
    return (
      <section className="drawer-section drawer-section-narrative">
        <h4 className="drawer-section-title">{section.title}</h4>
        <p className="artifact-drawer-text">{section.body}</p>
      </section>
    );
  }
  // links
  return (
    <section className="drawer-section drawer-section-links">
      <h4 className="drawer-section-title">{section.title}</h4>
      {section.items.length === 0 ? (
        <p className="drawer-section-empty">{section.empty_text ?? "—"}</p>
      ) : (
        <ul className="drawer-link-list">
          {section.items.map((it, i) => (
            <li key={i}>
              <button
                type="button"
                className="drawer-link"
                onClick={() => onLink(it)}
              >
                <span className="drawer-link-primary">{it.primary}</span>
                <span className="drawer-link-row">
                  {it.secondary ? (
                    <span className="drawer-link-secondary">{it.secondary}</span>
                  ) : null}
                  {it.meta ? (
                    <span className="drawer-link-meta">{it.meta}</span>
                  ) : null}
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
