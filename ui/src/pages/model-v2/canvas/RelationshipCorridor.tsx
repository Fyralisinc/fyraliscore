// RelationshipZoom state (spec §9).
//
// The selected arrow becomes a corridor between source category and
// target category, with each relationship instance rendered as a
// strand showing source → target items. Resolution opportunity chips
// optionally appear at the bottom.

import type { CategoryId, RelationshipFocus } from "../types";
import { CategoryIcon, ModelItemMicroCard, StatusChip } from "../components/primitives";

export function RelationshipCorridor({
  focus,
  onItemClick,
  onCategoryClick,
  onResolutionClick,
}: {
  focus: RelationshipFocus;
  onItemClick: (id: string) => void;
  onCategoryClick: (id: CategoryId) => void;
  onResolutionClick?: (id: string) => void;
}) {
  const { bundle, sourceCategory, targetCategory, instances, resolutionOpportunities } = focus;
  return (
    <div className="fm-corridor" data-testid="relationshipzoom-canvas">
      <header className="fm-corridor__head">
        <div className="fm-corridor__caption">
          <span className="fm-corridor__verb">{bundle.verb}</span>
          <span className="fm-corridor__counts">
            {bundle.instanceCount}{" "}
            {bundle.instanceCount === 1 ? "relationship" : "relationships"}
          </span>
          {bundle.impactLabel ? (
            <span className="fm-corridor__impact">{bundle.impactLabel}</span>
          ) : null}
        </div>
      </header>
      <div className="fm-corridor__columns">
        <button
          type="button"
          className={`fm-corridor__cat fm-corridor__cat--${sourceCategory.colorToken}`}
          onClick={() => onCategoryClick(sourceCategory.id)}
          aria-label={`${sourceCategory.label} side of relationship`}
        >
          <CategoryIcon id={sourceCategory.id} />
          <span>{sourceCategory.label}</span>
        </button>
        <div className="fm-corridor__spacer" aria-hidden="true" />
        <button
          type="button"
          className={`fm-corridor__cat fm-corridor__cat--${targetCategory.colorToken}`}
          onClick={() => onCategoryClick(targetCategory.id)}
          aria-label={`${targetCategory.label} side of relationship`}
        >
          <CategoryIcon id={targetCategory.id} />
          <span>{targetCategory.label}</span>
        </button>
      </div>
      <ul className="fm-corridor__instances">
        {instances.map((ri) => (
          <li key={ri.id} className="fm-corridor__row">
            <div className="fm-corridor__side">
              <ModelItemMicroCard
                item={ri.sourceItem}
                onClick={() => onItemClick(ri.sourceItem.id)}
                size="compact"
              />
            </div>
            <div
              className={`fm-corridor__strand fm-corridor__strand--${bundle.visual.colorToken}`}
              aria-hidden="true"
            >
              <span className="fm-corridor__verb-floater">{ri.verb}</span>
              <svg className="fm-corridor__strand-svg" viewBox="0 0 240 24" preserveAspectRatio="none">
                <path
                  d="M0 12 L228 12"
                  fill="none"
                  className="fm-corridor__strand-path"
                />
                <path d="M228 6 L240 12 L228 18 Z" className="fm-corridor__strand-arrow" />
              </svg>
              {ri.impactLabel ? (
                <span className="fm-corridor__row-impact">{ri.impactLabel}</span>
              ) : null}
            </div>
            <div className="fm-corridor__side">
              <ModelItemMicroCard
                item={ri.targetItem}
                onClick={() => onItemClick(ri.targetItem.id)}
                size="compact"
              />
            </div>
          </li>
        ))}
        {instances.length === 0 ? (
          <li className="fm-corridor__empty">
            <strong>No relationship instances available.</strong>
            <p>
              Fyralis has the category-level bundle but does not have inspectable
              instances yet. Connect more sources or wait for evidence to accumulate.
            </p>
          </li>
        ) : null}
      </ul>
      {resolutionOpportunities && resolutionOpportunities.length > 0 ? (
        <footer className="fm-corridor__footer" aria-label="Resolution opportunities">
          <span className="fm-corridor__footer-label">Resolution opportunities</span>
          <div className="fm-corridor__chips">
            {resolutionOpportunities.map((r) => (
              <button
                key={r.id}
                type="button"
                className="fm-corridor__chip"
                onClick={() => onResolutionClick?.(r.id)}
              >
                {r.label}
              </button>
            ))}
          </div>
        </footer>
      ) : null}
    </div>
  );
}

// Re-export to keep the canvas barrel consistent without separate imports.
export { StatusChip };
