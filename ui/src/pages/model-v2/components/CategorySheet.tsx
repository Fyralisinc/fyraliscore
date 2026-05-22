// Category focus drawer — right-side overlay with a blurred backdrop.
//
// Replaces the in-canvas Category Zoom expansion: clicking a category
// in the overview map now slides this panel in from the right while
// the overview lattice remains visible (but blurred) behind it. The
// user keeps spatial context without having the canvas itself reflow.
//
// Item / bundle clicks inside the sheet close the sheet and push the
// corresponding deeper state (nodeZoom / relationshipZoom) so the main
// canvas can take over.

import { useEffect } from "react";
import type { CategoryFocus, CategoryId, RelationshipBundle } from "../types";
import {
  CategoryIcon,
  ModelItemMicroCard,
  StatusBeads,
} from "./primitives";

export function CategorySheet({
  focus,
  onClose,
  onItemClick,
  onBundleClick,
  onRelatedCategoryClick,
}: {
  focus: CategoryFocus;
  onClose: () => void;
  onItemClick: (id: string) => void;
  onBundleClick: (id: string) => void;
  onRelatedCategoryClick: (id: CategoryId) => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        onClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const { category, topItems, relationshipBundles, relatedCategories } = focus;
  const inbound = relationshipBundles.filter(
    (b) => b.targetCategoryId === category.id,
  );
  const outbound = relationshipBundles.filter(
    (b) => b.sourceCategoryId === category.id,
  );
  const relatedActive = relatedCategories.filter(
    (c) => (c as { isRelated?: boolean }).isRelated,
  );

  return (
    <div
      className="fm-sheet"
      role="dialog"
      aria-modal="true"
      aria-label={`${category.label} focus`}
      data-testid="category-sheet"
    >
      <div className="fm-sheet__shade" onClick={onClose} aria-hidden="true" />
      <aside
        className={`fm-sheet__panel fm-sheet__panel--${category.colorToken}`}
      >
        <header className="fm-sheet__head">
          <div className="fm-sheet__head-row">
            <span className="fm-sheet__eyebrow">Category</span>
            <button
              type="button"
              className="fm-sheet__close"
              onClick={onClose}
              aria-label="Close"
            >
              ×
            </button>
          </div>
          <div className="fm-sheet__titleblock">
            <CategoryIcon id={category.id} />
            <h2 className="fm-sheet__title">{category.label}</h2>
          </div>
          <p className="fm-sheet__desc">{category.description}</p>
          <div className="fm-sheet__counts">
            <span className="fm-sheet__bignum">
              {category.itemCount.toLocaleString()}
            </span>
            <span className="fm-sheet__bignumlabel">
              active
              {category.blockedCount ? ` · ${category.blockedCount} blocked` : ""}
              {category.atRiskCount ? ` · ${category.atRiskCount} at risk` : ""}
            </span>
          </div>
          <StatusBeads distribution={category.statusDistribution} />
        </header>
        <div className="fm-sheet__body">
          <section className="fm-sheet__section">
            <h3>
              Top items
              <span className="fm-sheet__count">
                {topItems.length} / {category.itemCount.toLocaleString()}
              </span>
            </h3>
            {topItems.length === 0 ? (
              <p className="fm-sheet__empty">
                No items in this category yet.
              </p>
            ) : (
              <ul className="fm-sheet__items">
                {topItems.map((it) => (
                  <li key={it.id}>
                    <ModelItemMicroCard
                      item={it}
                      onClick={() => onItemClick(it.id)}
                    />
                  </li>
                ))}
              </ul>
            )}
          </section>

          {outbound.length > 0 ? (
            <section className="fm-sheet__section">
              <h3>Outbound relationships</h3>
              <ul className="fm-sheet__bundles">
                {outbound.map((b) => (
                  <BundleRow
                    key={b.id}
                    bundle={b}
                    side="target"
                    onClick={() => onBundleClick(b.id)}
                  />
                ))}
              </ul>
            </section>
          ) : null}

          {inbound.length > 0 ? (
            <section className="fm-sheet__section">
              <h3>Inbound relationships</h3>
              <ul className="fm-sheet__bundles">
                {inbound.map((b) => (
                  <BundleRow
                    key={b.id}
                    bundle={b}
                    side="source"
                    onClick={() => onBundleClick(b.id)}
                  />
                ))}
              </ul>
            </section>
          ) : null}

          {relatedActive.length > 0 ? (
            <section className="fm-sheet__section">
              <h3>Related categories</h3>
              <div className="fm-sheet__chips">
                {relatedActive.map((c) => (
                  <button
                    key={c.id}
                    type="button"
                    className={`fm-sheet__chip fm-sheet__chip--${c.colorToken}`}
                    onClick={() => onRelatedCategoryClick(c.id)}
                  >
                    <CategoryIcon id={c.id} />
                    <span>{c.label}</span>
                    <span className="fm-sheet__chip-count">
                      {c.itemCount}
                    </span>
                  </button>
                ))}
              </div>
            </section>
          ) : null}
        </div>
      </aside>
    </div>
  );
}

function BundleRow({
  bundle,
  side,
  onClick,
}: {
  bundle: RelationshipBundle;
  side: "source" | "target";
  onClick: () => void;
}) {
  // For outbound rows we show "→ target"; for inbound, "source →".
  const otherId =
    side === "target" ? bundle.targetCategoryId : bundle.sourceCategoryId;
  return (
    <li>
      <button
        type="button"
        className={`fm-sheet__bundle fm-sheet__bundle--${bundle.visual.colorToken}${
          bundle.synthesized ? " fm-sheet__bundle--synth" : ""
        }`}
        onClick={onClick}
      >
        <span className="fm-sheet__bundle-verb">{bundle.verb}</span>
        <span className="fm-sheet__bundle-other">
          {side === "target" ? "→ " : ""}
          {otherId}
          {side === "source" ? " →" : ""}
        </span>
        <span className="fm-sheet__bundle-count">
          {bundle.synthesized ? "~" : ""}
          {bundle.instanceCount}
        </span>
      </button>
    </li>
  );
}
