# P0-C Truth-State Inventory

The JSON companion defines fifteen illegal state classes and four legal
controls. Each class names its global hard gate and future P2 owner.

## Current finding

The schema prevents some malformed scalar values, but it does not make the
important cross-table epistemic invariants unrepresentable. In particular:

- `active` does not prove admission;
- accepted Models and relations can lack sufficient lineage;
- Model scope JSON can be untyped or batch-propagated;
- proposition and natural text have no shared semantic-version digest;
- lifecycle closure is distributed across several writers and readers;
- relation status and participant binding status can disagree;
- accepted relations need not have a complete participant set;
- accepted binary edges need not originate in relation instances; and
- read activity mutates counters on the canonical Model row.

These are characterization findings. P0 deliberately does not repair them.

## Test meaning

The focused test is expected to pass because it proves that every registered
historical illegal class is explicitly reproduced in the inventory and mapped
to an enforcement owner. It does **not** assert that production rejects the
state. `currently_representable=true` is the failing system behavior captured
for P2.

Future P2 tests must invert that field only after all canonical writers and
default readers enforce the invariant. Removing a fixture to obtain green is
not permitted.
