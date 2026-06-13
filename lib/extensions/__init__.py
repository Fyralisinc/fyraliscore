"""lib/extensions — the Fyralis interface/extension host surface (ADR-0004).

This package is the *stable, internals-hiding* boundary an extension binds to:

  - ``lib.extensions.host_api`` — the SemVer-pinned host API (the contract a
    manifest's ``engines.fyralis_host_api`` range targets).
  - ``lib.extensions.manifest`` — the declarative ``ExtensionManifest`` + the
    ``company_os.interfaces`` discovery loader.

It lives under ``lib`` (the dependency floor) so the import-linter "lib is
independent of services" contract enforces *for free* that the host API never
leaks ``services.*`` internals. The concrete attach seams live in the layers
they wire into (e.g. the draft-enricher registry in
``services.ingest.ingestion.enrichers``); they import the contract *types* from
here.
"""
from __future__ import annotations
