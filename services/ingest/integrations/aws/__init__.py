"""AWS ingestion integration (IN-AWS).

AWS is an IAM/SigV4-auth source on the Grafana time-window-backfill shape, but
its LIVE edge is a POLL (SQS / EventBridge) rather than a webhook:

  - client.py      — outbound AWS API client (CloudTrail LookupEvents /
                     CloudWatch alarm-history). Real auth is IAM SigV4
                     (botocore); the synthetic gate uses a mock, so the real
                     signing is left as a TODO-stubbed seam.
  - credentials.py — IAM credential resolution helper (assume-role / static
                     keys). TODO-stubbed for the synthetic gate.
  - onboarding.py  — finalize_install (aws_installations + onboarding trigger).
  - live_poll.py   — the PRODUCTION poll live edge: handle_polled_event maps one
                     polled CloudTrail-shaped event into a canonical record and
                     shadow-writes it (ingress_kind="poll"), mirroring the
                     telegram gateway/dispatch direct-dispatch pattern.

Backfill walks a per-account/region TIME WINDOW of management events
(CloudTrail), bounded below by a 90-day floor (clone of the Grafana annotations
walk). There is NO webhook signature file: the live edge is a poll, so the trust
boundary is the IAM-authenticated poll itself (as with Telegram's gateway /
Gmail's Pub/Sub), not an HMAC header.
"""
