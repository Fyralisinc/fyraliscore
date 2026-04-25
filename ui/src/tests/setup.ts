import "@testing-library/jest-dom/vitest";

// jsdom doesn't implement scrollTo on elements; App uses it for the
// card-expand scroll-into-view. Patch as no-op so tests don't throw.
Element.prototype.scrollTo = function () {
  /* no-op */
};
// Some components may call scrollIntoView.
if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = function () {
    /* no-op */
  };
}
