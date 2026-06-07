## Documentation

Internal engineering + AI-agent docs live in `/docs` and are published with
**MkDocs (Material)**. This is internal coordination/understanding — how the
system works and why — not a public API/SDK reference.

- **Preview locally:** `pip install -e ".[docs]"` (or `uv pip install -e ".[docs]"`),
  then `mkdocs serve`. Strict build: `mkdocs build --strict`.
- **Structure** (`mkdocs.yml` `nav`):
  - `docs/index.md` — start-here system overview + diagram.
  - `docs/architecture/` — one page per layer (`app`, `ingest`, `reasoning`,
    `domain`, `product`, `platform`, `workers`, `lib`, `data-plane`); each has a
    wiring diagram and responsibilities.
  - `docs/services.md` — the single coordination table of every service.
  - `docs/glossary.md` — proprietary domain vocabulary.
  - `docs/adr/` — Architecture Decision Records (template + index; add new
    decisions here).
- **Rules for contributors (human and agent):**
  - **Edit the docs in the same PR as the code they describe.** A change to a
    subsystem updates that subsystem's architecture page.
  - **Never fabricate rationale.** If the *why* isn't in the code, leave a visible
    `> **TODO(human):** …` callout rather than guessing. Label inferred technical
    claims as inferences.
  - Diagrams use Mermaid via Material's superfences (a ```` ```mermaid ```` fence) —
    do not add a separate mermaid plugin.
  - The pre-existing `docs/ingestion/`, `docs/github-intelligence/`, `docs/testing/`,
    and `docs/history/` trees are kept on disk but `exclude_docs`-ed from the build
    (they link straight to source files). Don't add site links to them.
  - The deeper narrative decision record is `CODEBASE-MANAGEMENT.md`; the
    module-level reference is `CODEBASE-ARCHITECTURE.md`; the comprehensive
    end-to-end reference is `FYRALIS.md`. All three now live in the internal docs
    under `docs/reference/` (the *Codebase reference* nav section), not the repo root.
