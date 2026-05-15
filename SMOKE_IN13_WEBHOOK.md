# IN-13 Webhook Smoke Test

This file exists to drive a real signed webhook delivery from
`Fyralisinc/fyraliscore` through ngrok into the locally-running
Fyralis gateway. It is **safe to delete** after IN-13 is verified.

The branch is `smoke/IN-13-webhook-test`. The PR that opens against
`main` should produce, at minimum, these Observations:

| Event | Expected `external_id` | trust_tier | kind |
|---|---|---|---|
| `push` | `Fyralisinc/fyraliscore@<head_sha>` | authoritative | signal |
| `pull_request.opened` | `<PR node_id>` | inferential | signal |
| `issues.opened` | `<issue node_id>` | authoritative | signal |
| `issue_comment.created` | `<comment node_id>` | inferential | signal |

Verification:

```sql
SELECT external_id, content_text, trust_tier, kind, occurred_at
  FROM observations
 WHERE source_channel='github:webhook'
 ORDER BY occurred_at DESC LIMIT 10;
```

Spec: `specs/IN-13-github-integration/spec.md`.
