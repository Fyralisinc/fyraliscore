"""Bridge progress contract.

Per ingestion LLD §6 (Bridge contract):
  - `events.py` — Pydantic models for every `onboarding.progress` event.
  - `publisher.py` — `publish_progress_event(producer, event)` thin
    wrapper that owns topic name + per-tenant key derivation.
"""
