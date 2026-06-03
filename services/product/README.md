# services/product

Product layer — CEO-facing surfaces composed from substrate + reasoning.

This directory is one **architectural layer** of the `services/` package. It is
a PEP 420 namespace package (no `__init__.py`) and groups the domain packages
that share this role. See [CODEBASE-ARCHITECTURE.md](../../CODEBASE-ARCHITECTURE.md)
for the full layer map and [CONTRIBUTING.md](../../CONTRIBUTING.md) for the
import rules enforced by `lint-imports`.

## Packages

```
__pycache__ conversations decision_deltas demo forecasts greeting history model_trace query recommendations rendering today
```

## Import direction

Import *downward* freely (toward `domain`, `platform`, and `lib`). Avoid new
*upward* imports into higher layers — boundaries are enforced by import-linter
(`lint-imports`). Add a new package here only if its role matches this layer;
otherwise pick the layer that matches and update the layer map.
