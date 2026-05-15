# IN-13 Webhook Smoke Test — Rerun

Second pass through the full ingest path after fixing the
`installation_audit_log.action` CHECK widening (migration 0044).

This run targets `installation_id=132572250` (the reinstalled App) in
`all-repositories` mode. Every event from any repo in `Fyralisinc/*`
that the App has access to should land as an Observation.

| Event source | Expected observation |
|---|---|
| push to this branch | `external_id = Fyralisinc/fyraliscore@<sha>`, authoritative |
| issues.opened | `external_id = <issue node_id>`, authoritative |
| pull_request.opened | `external_id = <PR node_id>`, inferential |
| issue_comment.created (×2) | `external_id = <comment node_id>`, inferential |

Safe to delete after verification.
