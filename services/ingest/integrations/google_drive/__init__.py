"""services/ingest/integrations/google_drive/ — Google Drive ingestion source (IN-16).

Like Google Calendar (IN-15), Drive is a Google Workspace API that reuses the
SHARED Gmail Domain-Wide-Delegation substrate (services/ingest/integrations/gmail/
dwd.py + GoogleHttpClient) rather than the provider_installations OAuth-bot
path. Three sub-modules mirror the Calendar package:

  - client.py     : Drive v3 request shapes over the shared GoogleHttpClient,
                    including document text export.
  - onboarding.py : resolve My-Drive + Shared-Drive targets and finalize the
                    install (UPSERT rows + emit the M6 onboarding trigger).
  - metrics.py    : provision + fetch counters.

The ingestion-side planner / fetcher / handler / reconciler live under
services/ingest/ingestion/{planners,fetchers,handlers,reconcilers}/google_drive.py.
"""
