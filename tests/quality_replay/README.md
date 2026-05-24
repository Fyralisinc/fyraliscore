# Think Quality Replay

Promoted cases in `tests/quality_replay/cases/*.json` come from
`/debug/think-quality/cases` via:

```bash
DATABASE_URL=postgres://... \
  .venv/bin/python scripts/promote_think_quality_cases.py \
  --tenant-id <tenant_uuid> --limit 10
```

Freshly promoted misses should usually keep `expectation.mode` set to
`known_failure`; the replay test then preserves the failure as a
tracked regression target without making CI red for an acknowledged
miss. After the prompt/retrieval behavior is fixed and the case is
refreshed, switch the fixture to `must_pass` and tune the expectation
thresholds.
