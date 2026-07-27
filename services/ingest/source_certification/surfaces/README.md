# Generated source certification surfaces

The 27 JSON files in this directory are generated local evidence. Each bundle
pins:

- the complete `SourceDefinition` and its `ProviderDefinition`;
- hashes of contract-bound and source-integration implementation modules;
- the strict Provider Lab routes used by the production client; and
- one sanitized, deterministic golden fixture.

Regenerate and verify them with:

```bash
COMPANY_OS_ENV=test python scripts/generate_source_certification_surfaces.py
COMPANY_OS_ENV=test python scripts/generate_source_certification_surfaces.py --check
```

These files do not replace provider documentation review, observed quota
evidence, throughput runs, or real-provider canaries.
