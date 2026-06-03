"""Standalone local HTTP mock servers for sandbox / e2e ingestion testing.

Unlike services/ingest/synthetic/mock_clients/ (in-memory Python fakes injected at the
client seam), these are REAL HTTP servers. They exercise the full outbound path
— endpoint resolver, httpx client, and (for Google sources) the DWD token
mint — against a controllable fake, with no real provider credentials.
"""
