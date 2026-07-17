# P0 Hard-Gate Ownership Matrix

The machine-readable source is
[hard-gate-ownership-matrix.json](hard-gate-ownership-matrix.json).

Every HG-01 through HG-15 gate now has:

- a named current owner surface;
- a declared enforcement seam;
- a declared independent test seam; and
- a current evidence source.

This is ownership completeness, not system correctness. Most truth and
observability gates are deliberately red or unmeasured at the P0 baseline.
Later phases may change a gate result only with member-level evidence from the
registered test seam.

## Current disposition

| Result | Meaning |
| --- | --- |
| `fail` | Current code/artifacts reproduce a constitutional violation or missing invariant. |
| `convention_not_physical` | Intended rule exists but bypass prevention is not mechanically proven. |
| `bounded_only` | The property passed a small registered population but lacks integrated/open-population proof. |
| `unmeasured` | No current evidence answers the gate. |
| `historical_fail_current_contract_added` | Historical evidence failed; P0 added a contract/test seam but has not run an integrated repair. |

No topology, retrieval, activity, or aggregate score can compensate for a red
hard gate.
