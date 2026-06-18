-- =====================================================================
-- 0143_extension_marketplace.sql — the curated extension registry (E4)
-- =====================================================================
-- ADR-0004 E4 / roadmap M8. A submitted extension version moves through:
--   submitted → (automated gate: manifest lint + scope justification + callback
--   domain) → approved → (public: manual review + signing) → published.
-- A tenant installs a PUBLISHED listing; install verifies the signature (public)
-- then records the consent grant via lifecycle.install.
--
-- Review rigor scales with blast radius, not code trust (ADR): private listings
-- self-approve through the automated gate; public listings additionally require a
-- recorded human reviewer + a host signature.
--
-- Host-managed (no RLS): the catalog is cross-tenant + operator-owned.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS extension_listings (
  listing_id     UUID PRIMARY KEY,
  extension_id   TEXT NOT NULL,
  version        TEXT NOT NULL,
  publisher      TEXT NOT NULL,
  trust_tier     TEXT NOT NULL DEFAULT 'third_party',
  visibility     TEXT NOT NULL DEFAULT 'private'
                 CHECK (visibility IN ('private', 'public')),
  status         TEXT NOT NULL DEFAULT 'submitted'
                 CHECK (status IN ('submitted', 'approved', 'published', 'rejected')),
  manifest       JSONB NOT NULL,
  capabilities   JSONB NOT NULL DEFAULT '{}'::jsonb,
  gate_report    JSONB NOT NULL DEFAULT '{}'::jsonb,
  signature      TEXT,                 -- host signature over the manifest (public listings)
  signed_by      TEXT,
  review_notes   TEXT,
  submitted_by   TEXT NOT NULL,
  submitted_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_by    TEXT,
  reviewed_at    TIMESTAMPTZ,
  published_at   TIMESTAMPTZ,
  UNIQUE (extension_id, version)
);

CREATE INDEX IF NOT EXISTS extension_listings_published_idx
  ON extension_listings (extension_id) WHERE status = 'published';

COMMIT;
