# IN-01 — Body size + JSON check ahead of body read

## 1. Summary
Reject oversized or malformed ingest requests **before** the gateway reads the
request body into memory, eliminating a trivial OOM amplification vector on the
`POST /ingest/{channel}` route.

## 2. Problem statement
[services/gateway/main.py:573-650](../../../services/gateway/main.py#L573-L650)
currently performs:

```python
raw = await request.body()
if len(raw) > MAX_PAYLOAD_BYTES:
    return JSONResponse({"error": "payload_too_large", ...}, status_code=413)
```

`await request.body()` buffers the entire payload before the size check fires.
A malicious caller advertising `Content-Length: 5_000_000_000` (or streaming a
multi-gigabyte chunked body) can exhaust process memory long before the
guard rejects the request. There is also no defensive cap on streamed reads,
and `json.JSONDecodeError` is caught after the fact but with no schema applied
to the error response.

The current ingest limit is `MAX_PAYLOAD_BYTES = 1 MiB` (defined at
[services/ingestion/core.py:68](../../../services/ingestion/core.py#L68)).

## 3. Goals
- **G1.** Reject requests whose `Content-Length` header exceeds
  `MAX_PAYLOAD_BYTES` with HTTP `413` **before** any body byte is read.
- **G2.** Reject `Transfer-Encoding: chunked` ingest requests with HTTP `413`
  (no streaming-body support in Wave 2).
- **G3.** When `Content-Length` is absent or trusted, bound the body read with
  a byte counter that aborts at `MAX_PAYLOAD_BYTES + 1` (defense in depth).
- **G4.** Malformed JSON returns a **structured** `400 {"error":"invalid_json",
  "detail": "<reason>"}` — never a stack trace.
- **G5.** Slack signature verification continues to receive the **raw** body
  bytes unchanged (HMAC must be over the original payload).

## 4. Non-goals
- Streaming/chunked ingest support (deferred; tracked separately).
- Per-tenant payload limits.
- Changing `MAX_PAYLOAD_BYTES` itself.
- Touching non-ingest routes (`/observations`, `/health`, etc.).
- Rewriting the auth/rate-limit middleware stack.

## 5. User stories
- **U1.** *As an operator*, when a buggy or malicious client posts a 100 MB
  body to `/ingest/...`, my gateway returns 413 in <5 ms and allocates no
  more than the request headers — so a flood of bad requests can't OOM the
  process.
- **U2.** *As an integration engineer*, when my JSON payload is malformed, I
  receive a structured error I can log and react to programmatically.
- **U3.** *As the Slack integration*, my signed payloads keep validating
  because the HMAC still sees the exact bytes Slack sent.

## 6. Acceptance criteria

| ID  | Scenario | Expected |
|-----|----------|----------|
| A1  | `POST /ingest/slack:message` with `Content-Length: 104857601` (100 MB+1) and a valid bearer | `413 {"error":"payload_too_large","max_bytes":1048576}`, body **not** read |
| A2  | Same as A1 but no body sent (header lies) | Still `413`, no read attempted |
| A3  | `POST /ingest/...` with `Transfer-Encoding: chunked` | `413 {"error":"payload_too_large","reason":"chunked_unsupported"}` |
| A4  | `POST /ingest/...` with no `Content-Length` and a body whose actual size exceeds the limit | `413` once the streaming counter trips at `MAX_PAYLOAD_BYTES + 1` |
| A5  | `POST /ingest/slack:message` with a valid 500 KB JSON payload and a valid Slack signature | `200`/`201` as today; signature check sees identical raw bytes |
| A6  | `POST /ingest/...` with `Content-Type: application/json` and body `{not json}` | `400 {"error":"invalid_json","detail":"..."}` |
| A7  | `POST /ingest/...` with valid 1 KB JSON | unchanged 200/201 happy path |
| A8  | Authentication still runs **before** size precheck (so unauth callers don't get a size error) | `401 missing_bearer` even when oversize header set |

## 7. Out-of-scope behaviour clarifications
- A `Content-Length` smaller than the actual streamed bytes is treated as
  client error: the streaming counter catches the discrepancy at G3.
- We do **not** verify `Content-Length` matches the body exactly — Starlette
  /uvicorn already terminate the connection in that case.
- HEAD/OPTIONS/GET on `/ingest/*` are unaffected (only POST is guarded).

## 8. Risks & mitigations
| Risk | Mitigation |
|------|------------|
| Slack signature failure after body-read refactor | A5 covers this; signature path keeps using the same `raw` bytes returned by the bounded reader. |
| Middleware ordering breaks auth | Precheck runs **inside** the route handler (or as a route-scoped dependency), after `auth` middleware — preserves A8. |
| Future streaming use-case blocked | Chunked rejection is gated on path prefix `/ingest/`, not global. |

## 9. Open questions
None blocking. The implementation can decide between (a) putting the precheck
inline at the top of `post_ingest` vs. (b) a small FastAPI dependency — both
satisfy the spec. `plan.md` picks one.

## 10. Success metric
- Unit/integration tests in `services/gateway/tests/test_ingest_endpoint.py`
  cover A1–A8 and pass on CI.
- Manual smoke: `curl` with `-H "Content-Length: 999999999"` against a local
  gateway returns 413 immediately and `docker stats` shows no memory spike.
