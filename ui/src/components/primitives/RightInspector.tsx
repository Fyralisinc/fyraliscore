import type { ReactNode } from "react";

export interface RightInspectorProps {
  title: ReactNode;
  classification?: ReactNode;
  body: ReactNode;
  footerActions?: ReactNode;
  onBack?: () => void;
  onForward?: () => void;
  onClose?: () => void;
  canBack?: boolean;
  canForward?: boolean;
}

export function RightInspector({
  title,
  classification,
  body,
  footerActions,
  onBack,
  onForward,
  onClose,
  canBack = true,
  canForward = true,
}: RightInspectorProps) {
  return (
    <div className="fy-inspector">
      <header className="fy-inspector__header">
        <button
          type="button"
          className="fy-inspector__nav-btn"
          aria-label="Back"
          disabled={!canBack || !onBack}
          onClick={onBack}
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
            <path d="M10 3 5 8l5 5" />
          </svg>
        </button>
        <button
          type="button"
          className="fy-inspector__nav-btn"
          aria-label="Forward"
          disabled={!canForward || !onForward}
          onClick={onForward}
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
            <path d="M6 3 11 8l-5 5" />
          </svg>
        </button>
        <button
          type="button"
          className="fy-inspector__nav-btn fy-inspector__close"
          aria-label="Close"
          onClick={onClose}
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
            <path d="M4 4l8 8M12 4l-8 8" />
          </svg>
        </button>
      </header>

      {classification ? (
        <div className="fy-inspector__classification">{classification}</div>
      ) : null}

      <h2 className="fy-inspector__title">{title}</h2>

      <div className="fy-inspector__body">{body}</div>

      {footerActions ? (
        <footer className="fy-inspector__footer">{footerActions}</footer>
      ) : null}
    </div>
  );
}

export default RightInspector;
