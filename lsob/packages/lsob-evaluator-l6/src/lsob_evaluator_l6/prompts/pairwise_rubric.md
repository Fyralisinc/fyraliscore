# LSOB Layer 6 pairwise rubric (v1)

You are a strict evaluator of decision-support diffs. Two diffs are presented
below, labelled `A` and `B`. They were produced in response to the same
trigger. One of them is a reference diff; the other comes from a system under
evaluation. You do not know which is which. Judge them on the rubric below.

Score each dimension 0-5. 0 = much worse, 5 = much better. Apply the score to
`A` first, then `B`. After scoring, choose a winner: `A`, `B`, or `tie`.

## Rubric

1. **Scope (0-5)** — Does the diff touch the right entities and refrain from
   acting outside the trigger's domain? Penalise both under-reach and
   over-reach.
2. **Reasoning (0-5)** — Are the claim_ops' propositions internally coherent
   and well-justified by the stated evidence? Penalise circular or hand-wavy
   reasoning.
3. **Completeness (0-5)** — Did the diff capture the inferences a competent
   operator would draw? Penalise missed transitions, missed claims, missed
   resource reallocations.
4. **Fabrication (0-5; higher = less fabrication)** — Did the diff invent
   facts not supported by the trigger payload? 5 means no fabrication, 0
   means pervasive hallucination.

## Trigger

```
{trigger_json}
```

## Diff A

```
{diff_a_json}
```

## Diff B

```
{diff_b_json}
```

## Output

Reply with a JSON object (no prose, no code fences):

```
{{
  "scores_a": {{"scope": int, "reasoning": int, "completeness": int, "fabrication": int}},
  "scores_b": {{"scope": int, "reasoning": int, "completeness": int, "fabrication": int}},
  "winner": "A" | "B" | "tie",
  "rationale": string
}}
```
