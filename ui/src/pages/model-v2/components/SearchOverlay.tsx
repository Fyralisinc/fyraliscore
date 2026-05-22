// SearchFocus state (spec §12). The overlay is a layered result list:
// suggested answer, matching model items, matching relationships,
// matching categories, suggested actions.

import { useEffect, useMemo, useRef } from "react";
import type {
  CategoryId,
  ModelCategory,
  ModelItemSummary,
  RelationshipBundle,
} from "../types";
import { StatusChip } from "./primitives";

export function SearchOverlay({
  query,
  onQueryChange,
  onClose,
  categories,
  bundles,
  items,
  onCategoryPick,
  onBundlePick,
  onItemPick,
}: {
  query: string;
  onQueryChange: (q: string) => void;
  onClose: () => void;
  categories: ModelCategory[];
  bundles: RelationshipBundle[];
  items: ModelItemSummary[];
  onCategoryPick: (id: CategoryId) => void;
  onBundlePick: (id: string) => void;
  onItemPick: (id: string) => void;
}) {
  const ref = useRef<HTMLInputElement | null>(null);
  useEffect(() => {
    ref.current?.focus();
  }, []);
  const q = query.trim().toLowerCase();

  const { catHits, bundleHits, itemHits } = useMemo(() => {
    if (!q) return { catHits: categories.slice(0, 4), bundleHits: bundles.slice(0, 4), itemHits: items.slice(0, 6) };
    return {
      catHits: categories.filter((c) => c.label.toLowerCase().includes(q)),
      bundleHits: bundles.filter(
        (b) =>
          b.verb.toLowerCase().includes(q) ||
          b.label.toLowerCase().includes(q) ||
          b.sourceCategoryId.toLowerCase().includes(q) ||
          b.targetCategoryId.toLowerCase().includes(q),
      ),
      itemHits: items.filter(
        (i) =>
          i.assertion.toLowerCase().includes(q) ||
          i.shortLabel.toLowerCase().includes(q) ||
          (i.relationshipHint ?? "").toLowerCase().includes(q) ||
          (i.owner ?? "").toLowerCase().includes(q),
      ),
    };
  }, [q, categories, bundles, items]);

  // Layered suggested-answer header: pick the strongest signal — most
  // specific result wins (item > bundle > category).
  const answer = useMemo(() => {
    if (!q) return "Browse the model by category, relationship, or item.";
    if (itemHits.length > 0) {
      return `${itemHits.length} model item${
        itemHits.length === 1 ? "" : "s"
      } match this query.`;
    }
    if (bundleHits.length > 0) {
      return `${bundleHits.length} relationship${
        bundleHits.length === 1 ? "" : "s"
      } match this query.`;
    }
    if (catHits.length > 0) return `${catHits.length} categories match.`;
    return "No matches yet — try a relationship verb (blocks, owns, exposes).";
  }, [q, itemHits, bundleHits, catHits]);

  return (
    <div className="fm-search-overlay" role="dialog" aria-modal="true" aria-label="Search">
      <div className="fm-search-overlay__shade" onClick={onClose} aria-hidden="true" />
      <div className="fm-search-overlay__panel" data-testid="search-overlay">
        <header className="fm-search-overlay__head">
          <input
            ref={ref}
            type="search"
            value={query}
            placeholder="Ask the model — try 'commitments blocked by pricing'"
            onChange={(e) => onQueryChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") onClose();
            }}
            data-testid="search-overlay-input"
          />
          <button type="button" className="fm-search-overlay__close" onClick={onClose}>
            Close
          </button>
        </header>
        <p className="fm-search-overlay__answer">{answer}</p>
        {itemHits.length > 0 ? (
          <section className="fm-search-overlay__section">
            <h3>Model items</h3>
            <ul>
              {itemHits.slice(0, 6).map((i) => (
                <li key={i.id}>
                  <button type="button" onClick={() => onItemPick(i.id)}>
                    <span>{i.shortLabel}</span>
                    <StatusChip status={i.status} />
                  </button>
                </li>
              ))}
            </ul>
          </section>
        ) : null}
        {bundleHits.length > 0 ? (
          <section className="fm-search-overlay__section">
            <h3>Relationships</h3>
            <ul>
              {bundleHits.slice(0, 6).map((b) => (
                <li key={b.id}>
                  <button type="button" onClick={() => onBundlePick(b.id)}>
                    <span>
                      {b.sourceCategoryId} → {b.verb} → {b.targetCategoryId}
                    </span>
                    <span className="fm-search-overlay__count">{b.label}</span>
                  </button>
                </li>
              ))}
            </ul>
          </section>
        ) : null}
        {catHits.length > 0 ? (
          <section className="fm-search-overlay__section">
            <h3>Categories</h3>
            <ul>
              {catHits.slice(0, 8).map((c) => (
                <li key={c.id}>
                  <button type="button" onClick={() => onCategoryPick(c.id)}>
                    <span>{c.label}</span>
                    <span className="fm-search-overlay__count">{c.itemCount} items</span>
                  </button>
                </li>
              ))}
            </ul>
          </section>
        ) : null}
      </div>
    </div>
  );
}
