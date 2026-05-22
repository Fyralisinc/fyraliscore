// Delegation sheet — spec §7.2. Modal sheet that lets the user assign
// responsibility for a Proposed Change without personally accepting it.

import { useEffect, useState } from "react";

import type { DecisionDelta, DelegateBody } from "@/api/today-page-types";

interface Props {
  delta: DecisionDelta;
  onCancel: () => void;
  onSubmit: (body: DelegateBody) => Promise<void> | void;
}

export function DelegationSheet({ delta, onCancel, onSubmit }: Props) {
  // Inferred default owner: from impact.delegation.owner_id when
  // present, else delta.targetNodeKind-derived role.
  const defaultOwner = delta.annotations?.delegation?.owner_id ?? "";
  const [ownerId, setOwnerId] = useState(defaultOwner);
  const [message, setMessage] = useState(
    `Fyralis flagged this change. Please confirm ownership or propose an alternate owner.`,
  );
  const [dueAt, setDueAt] = useState("");
  const [notifyNow, setNotifyNow] = useState(true);
  const [monitorConfirmation, setMonitorConfirmation] = useState(true);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onCancel();
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onCancel]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!ownerId.trim()) return;
    setSubmitting(true);
    try {
      await onSubmit({
        delegateToActorId: ownerId.trim(),
        message: message.trim() || undefined,
        dueAt: dueAt || undefined,
        notifyNow,
        monitorConfirmation,
      });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="tdv2-sheet-backdrop" role="dialog" aria-modal="true">
      <form
        className="tdv2-sheet"
        onSubmit={handleSubmit}
        data-testid="delegation-sheet"
      >
        <header className="tdv2-drawer__head">
          <h2 className="tdv2-drawer__title">Delegate change</h2>
          <button
            type="button"
            className="tdv2-drawer__close"
            onClick={onCancel}
            aria-label="Cancel"
          >
            ×
          </button>
        </header>
        <div className="tdv2-drawer__body">
          <div className="tdv2-field">
            <label className="tdv2-field__label" htmlFor="del-owner">Delegate to</label>
            <input
              id="del-owner"
              className="tdv2-input"
              type="text"
              placeholder="Actor ID (UUID)"
              value={ownerId}
              onChange={(e) => setOwnerId(e.target.value)}
              autoFocus
              required
            />
          </div>
          <div className="tdv2-field">
            <label className="tdv2-field__label" htmlFor="del-due">Due date</label>
            <input
              id="del-due"
              className="tdv2-input"
              type="datetime-local"
              value={dueAt}
              onChange={(e) => setDueAt(e.target.value)}
            />
          </div>
          <div className="tdv2-field">
            <label className="tdv2-field__label" htmlFor="del-message">Message / context</label>
            <textarea
              id="del-message"
              className="tdv2-textarea"
              value={message}
              onChange={(e) => setMessage(e.target.value)}
            />
          </div>
          <div className="tdv2-field">
            <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "13px" }}>
              <input
                type="checkbox"
                checked={notifyNow}
                onChange={(e) => setNotifyNow(e.target.checked)}
              />
              Notify now
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "13px" }}>
              <input
                type="checkbox"
                checked={monitorConfirmation}
                onChange={(e) => setMonitorConfirmation(e.target.checked)}
              />
              Allow Fyralis to monitor confirmation
            </label>
          </div>
        </div>
        <footer className="tdv2-drawer__foot">
          <button
            type="button"
            className="tdv2-btn tdv2-btn--secondary"
            onClick={onCancel}
          >
            Cancel
          </button>
          <button
            type="submit"
            className="tdv2-btn tdv2-btn--primary"
            disabled={submitting || !ownerId.trim()}
            data-testid="delegate-submit"
          >
            {submitting ? "Delegating..." : "Delegate change"}
          </button>
        </footer>
      </form>
    </div>
  );
}
