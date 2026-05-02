# Generate customer Resources for {{ company_name }}

{{ company_name }} — {{ tagline }}.

{{ description }}

## What to produce

A list of **{{ customer_count }} customer Resources**. The total ARR
across them should be approximately **${{ customer_arr_total }}** USD
(distribution will be skewed: a few large accounts, a long tail of
mid-market and smaller). Add roughly {{ prospect_count }} prospects too,
flagged via segment.

## Schema

Per customer:

- `id` — UUIDv4 string.
- `company_name` — fictional but plausible (no real-customer leakage).
- `arr_usd` — number, USD per year.
- `segment` — one of: `enterprise`, `mid_market`, `smb`, `design_partner`,
  `prospect`.
- `current_health` — one of: `healthy`, `watching`, `at_risk`, `escalating`.
- `primary_contacts` — list of 1-3 strings (e.g., "Maria Lopez, VP Eng").

Return JSON `{ "items": [...] }` matching `CustomerBatch`.

## Validation rules

1. Sum of `arr_usd` for non-prospect customers within ±10% of
   `${{ customer_arr_total }}`.
2. At most one customer flagged `escalating` (the headline tension
   account); leave it absent for healthy companies.
3. Customer names diverse — geographic + sector spread.
