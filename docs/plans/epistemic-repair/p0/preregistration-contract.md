# P0 Preregistration Contract

Implementation:
`lib/evaluation/epistemic_repair/preregistration.py`

## Required sealed inputs

Before an evaluation execution begins, one manifest binds:

- scenario ID, schema version, and SHA-256;
- gold ID, schema version, and SHA-256;
- evaluation-policy ID, schema version, and SHA-256;
- every runtime-source artifact and digest;
- provider/model/configuration digest;
- exact repository commit and worktree-overlay digest;
- logical-call, physical-attempt, whole-operation time, and token budgets;
- all random seeds;
- required hard gates;
- proof boundaries;
- allowed execution count; and
- prior execution count.

The receipt is sealed with an aware timestamp and the canonical manifest
SHA-256. Reopening reparses the full immutable contract and rejects any field
drift. A receipt whose allowed execution count is already exhausted cannot be
created.

## Evidence rule

The contract proves input identity and preregistration integrity. It does not
prove that an execution happened, that the provider honored a request, or that
semantic results were correct. Those require separate attempt, execution,
member-level outcome, and evaluator receipts tied back to this manifest digest.

## P0 validation

The focused test suite proves:

- deterministic serialization/digest round-trip;
- tamper rejection;
- unique artifact identities;
- execution-allowance enforcement; and
- one whole-operation attempt budget that includes failed physical attempts.
