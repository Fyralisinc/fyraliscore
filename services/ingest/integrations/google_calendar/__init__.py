"""Google Calendar integration package (IN-15).

Google Calendar is a Google Workspace API and reuses the EXISTING Gmail
Domain-Wide-Delegation auth substrate (services/ingest/integrations/gmail/dwd.py:
get_minter() + GoogleHttpClient + DirectoryClient). This package adds only
the Calendar-specific surface:

  - client.py     : GoogleCalendarClient over the shared GoogleHttpClient
  - onboarding.py : resolve workspace users -> calendars; finalize install
  - metrics.py    : provision / fetch counters
"""
