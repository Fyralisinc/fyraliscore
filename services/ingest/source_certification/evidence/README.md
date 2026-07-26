# Provider evidence packs

There is exactly one JSON pack per canonical observation source. These files
record the official/observed inputs needed to certify the API surface Fyralis
actually uses.

The checked-in packs intentionally start unlocked:

- `verified_at` remains `null` until a reviewer confirms the referenced
  behavior.
- `used_api_surface.schema_sha256` remains `null` until the sanitized schema or
  golden-fixture bundle is pinned.
- Provider limits that are unpublished or entitlement-dependent must be copied
  from response headers, provider consoles, or low-rate canaries. They must not
  be guessed.

`python -m services.ingest.source_certification inventory --require-ready`
must continue to fail until all 27 packs have genuine evidence.
