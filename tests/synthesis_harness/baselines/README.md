# Calibration baseline

`calibration.json` is the snapshot of the most recent harness
calibration run that we accept as "this is the level we expect the
synthesis layer to be at." Run

```
python -m tests.synthesis_harness --calibration
```

and the harness compares the new ECE against the baseline. A drift
of more than `REGRESSION_THRESHOLD_ECE` (`0.05`, see
`tests/synthesis_harness/calibration.py`) is flagged as a regression
and the harness exits non-zero.

## When to update the baseline

* After a deliberate change that improves calibration (lower ECE):
  always — drift the baseline down so we hold ourselves to the new
  level.
* After a deliberate change that worsens calibration: only with an
  explicit reason in the commit message. "We tightened the falsifier
  rules so the engine's confidence on previously-overconfident state
  Models dropped" is acceptable; "ECE went up and we don't know why"
  is not.
* After adding more labeled cases that shift the bucket distribution:
  yes, because the population changed. Note this in the commit.

## What this is NOT

Not a quality certificate. Calibration ECE is only as meaningful as
the human-labeled `ground_truth_correctness` values that fed it,
which are noisy and few. See `calibration.py` module docstring and
`REPORT.md` §10 for the full caveat.

The baseline's job is to detect *drift* between two harness runs —
not to claim the engine is calibrated to any specific number.
