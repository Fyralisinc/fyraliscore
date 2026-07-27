# Provider evidence packs

There is exactly one JSON pack per canonical observation source. These files
record the official/observed inputs needed to certify the API surface Fyralis
actually uses.

Each `used_api_surface.schema_sha256` pins the exact bytes of the matching
checked-in `../surfaces/<source>.json` bundle. Those sanitized bundles are
generated from the source/provider contract, referenced implementation-module
hashes, strict Provider Lab routes, and the source-owned deterministic golden
fixture:

```bash
COMPANY_OS_ENV=test python scripts/generate_source_certification_surfaces.py
COMPANY_OS_ENV=test python scripts/generate_source_certification_surfaces.py --check
```

The generated checksum locks the local surface; it does not claim provider
verification. The checked-in packs remain blocked until:

- `verified_at` remains `null` until a reviewer confirms the referenced
  behavior.
- Provider limits that are unpublished or entitlement-dependent must be copied
  from response headers, provider consoles, or low-rate canaries. They must not
  be guessed.

`python -m services.ingest.source_certification inventory --require-ready`
must continue to fail until all 27 packs have genuine evidence.
