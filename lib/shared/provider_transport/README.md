# Provider transport request policy

`ProviderTransport.execute(context, policy, call)` is the only retry owner for
an outbound provider attempt. Source clients classify provider responses into
the typed transport errors; the transport applies quota, concurrency, timeout,
retry, cooldown, and durable `RetryLater` behavior.

## Retry safety

`RequestPolicy.retry_safety` has three modes:

- `idempotent`: repeating the operation is intrinsically safe.
- `idempotency_key`: retry is allowed only when
  `RequestContext.idempotency_key` is present. The source adapter is
  responsible for sending that exact key to the provider.
- `unsafe`: the transport never repeats the provider call and does not turn
  an ambiguous post-call failure into `RetryLater`. It raises the
  non-recoverable `ProviderRetryForbiddenError`, preventing outer workflow
  retry machinery from bypassing the policy. Provider quota denial before the
  first call can still be scheduled safely.

Production clients resolve an exact source-owned operation policy when no
test override is injected. They do not construct local fallback policies.

`retryable_status_codes` and `retryable_error_codes` are optional allowlists.
`None` preserves legacy typed-error behavior. Once an allowlist is declared,
an observed discriminator must be included or the original error is returned
after one attempt as `ProviderRetryForbiddenError`. The original error code and
HTTP status remain in its structured context. `rate_limit_header_parser_id`
records which source-client parser produced the typed rate-limit error;
parsing remains at the provider adapter boundary because providers do not
share one header format. When a parser identity is declared, it must match the
identity carried by `ProviderRateLimited` or automatic retry is forbidden.
The shared `rate_limited_from_headers` helper identifies itself as
`http.retry_after`.

## Verified quota declarations

Production quota rules in `FYRALIS_PROVIDER_QUOTAS_JSON` must include:

```json
{
  "scope": "workspace",
  "identity": "workspace",
  "capacity": "<verified integer>",
  "refill_per_second": "<verified number>",
  "cost": "<verified integer>",
  "evidence_ref": "evidence://source/quota/version",
  "verified_on": "2025-01-01"
}
```

The placeholders above deliberately avoid suggesting provider limits.
Deployments must use values supported by their evidence pack. Startup fails
when a required runtime has missing, partial, invalid, or future-dated evidence.
Optional local runtimes continue accepting the legacy shape for Provider Lab
and migration compatibility.

## Distributed circuit semantics

Circuit state is keyed from each concrete quota `bucket_key`; no source-wide
or provider-wide fallback breaker exists. Redis atomically checks every
required circuit and every token bucket before charging quota or admitting an
upstream call.

- Three consecutive retryable failures open each participating bucket for
  30 seconds.
- A successful or provider-reachable rate-limit response resets a closed
  bucket's consecutive-failure count.
- After the open window, exactly one replica claims a 10-second half-open
  probe lease. Other replicas receive durable `RetryLater` with
  `reason=circuit_open` and never call upstream.
- A successful probe closes only its concrete buckets. A failed probe reopens
  them for a full window. A crashed probe can be replaced after its lease.
- The internal circuit bounds above are resilience settings, not provider
  quota claims. Provider capacities and costs still come only from verified
  quota declarations.
- Redis gate or outcome-write failures fail closed as
  `reason=quota_backend`. If an unsafe upstream operation already completed,
  the transport instead requires manual reconciliation so it cannot schedule
  a duplicate write.
