"""Fireflies integration (meeting-transcript source).

Fireflies.ai is a meeting-notetaker API that records, transcribes, and
summarizes calls. It is authenticated with a long-lived API token (HTTP
Bearer) and exposes a workspace's meeting transcripts (plus per-transcript
summaries / action items), with webhooks on transcription-complete events. The
ingestion source key is ``fireflies`` and the single channel is
``fireflies:transcript``.

This package clones the Brex Bearer-token archetype (itself a Mercury clone).
Several external-API details are UNVERIFIED (pagination scheme, webhook
signature scheme, exact host + read endpoints/scopes); see the
``TODO(human): confirm …`` markers in ``client.py``,
``../../ingestion/fetchers/fireflies.py``, and
``../../../app/webhooks/signatures/fireflies.py``.
"""
