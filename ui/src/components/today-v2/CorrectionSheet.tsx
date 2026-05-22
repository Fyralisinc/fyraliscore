// Correction sheet — spec §7.3. Modal sheet for "Report correction".
// Uses non-adversarial copy ("Report correction" not "Contest").

import { useEffect, useState } from "react";

import type { CorrectionBody, CorrectionType } from "@/api/today-page-types";

const TYPES: { value: CorrectionType; label: string; hint: string }[] = [
  { value: "wrong_conclusion",  label: "Wrong conclusion",       hint: "The proposal is incorrect." },
  { value: "wrong_owner",       label: "Wrong owner",            hint: "Different actor is accountable." },
  { value: "already_handled",   label: "Already handled",        hint: "This was addressed outside Fyralis." },
  { value: "missing_context",   label: "Missing context",        hint: "We're missing important context." },
  { value: "not_important",     label: "Not important enough",   hint: "Below the bar for judgment." },
  { value: "misleading_source", label: "Source is misleading",   hint: "Underlying signal is unreliable." },
  { value: "other",             label: "Other",                  hint: "Something else." },
];

interface Props {
  onCancel: () => void;
  onSubmit: (body: CorrectionBody) => Promise<void> | void;
}

export function CorrectionSheet({ onCancel, onSubmit }: Props) {
  const [ctype, setCtype] = useState<CorrectionType>("wrong_conclusion");
  const [explanation, setExplanation] = useState("");
  const [supportingLink, setSupportingLink] = useState("");
  const [applyRelated, setApplyRelated] = useState(false);
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
    if (!explanation.trim()) return;
    setSubmitting(true);
    try {
      await onSubmit({
        correctionType: ctype,
        explanation: explanation.trim(),
        supportingLink: supportingLink.trim() || undefined,
        applyToRelatedItems: applyRelated,
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
        data-testid="correction-sheet"
      >
        <header className="tdv2-drawer__head">
          <h2 className="tdv2-drawer__title">Report correction</h2>
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
            <span className="tdv2-field__label">Correction type</span>
            <div className="tdv2-correction-types">
              {TYPES.map((t) => (
                <button
                  key={t.value}
                  type="button"
                  className={`tdv2-correction-type${
                    ctype === t.value ? " tdv2-correction-type--selected" : ""
                  }`}
                  onClick={() => setCtype(t.value)}
                  data-testid={`correction-type-${t.value}`}
                >
                  <div style={{ fontWeight: 500 }}>{t.label}</div>
                  <div style={{ fontSize: "11px", color: "var(--text-muted)", marginTop: "2px" }}>
                    {t.hint}
                  </div>
                </button>
              ))}
            </div>
          </div>
          <div className="tdv2-field">
            <label className="tdv2-field__label" htmlFor="cor-explanation">Explanation</label>
            <textarea
              id="cor-explanation"
              className="tdv2-textarea"
              value={explanation}
              onChange={(e) => setExplanation(e.target.value)}
              placeholder="What's wrong, and what should Fyralis update?"
              autoFocus
              required
            />
          </div>
          <div className="tdv2-field">
            <label className="tdv2-field__label" htmlFor="cor-link">Optional supporting link</label>
            <input
              id="cor-link"
              className="tdv2-input"
              type="url"
              placeholder="https://"
              value={supportingLink}
              onChange={(e) => setSupportingLink(e.target.value)}
            />
          </div>
          <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "13px" }}>
            <input
              type="checkbox"
              checked={applyRelated}
              onChange={(e) => setApplyRelated(e.target.checked)}
            />
            Apply this correction to related items
          </label>
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
            className="tdv2-btn tdv2-btn--correction"
            disabled={submitting || !explanation.trim()}
            data-testid="correction-submit"
          >
            {submitting ? "Submitting..." : "Submit correction"}
          </button>
        </footer>
      </form>
    </div>
  );
}
