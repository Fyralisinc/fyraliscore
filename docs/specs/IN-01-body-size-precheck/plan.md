# IN-01 — Implementation Plan

Spec: [./spec.md](./spec.md)

## 1. Approach
Add a **route-scoped guard** at the top of `post_ingest` in
[services/gateway/main.py:573](../../../services/gateway/main.py#L573) that runs
**after** auth (preserving A8) but **before** `await request.body()`. The guard
does three things in order:

1. Reject `Transfer-Encoding: chunked` → `413` with
   `reason="chunked_unsupported"`.
2. Parse `Content-Length`; if present and `> MAX_PAYLOAD_BYTES` → `413` with
   `max_bytes`.
3. Replace the unbounded `await request.body()` with a small helper
   `_read_bounded_body(request, limit)` that streams via `request.stream()`,
   accumulates into a `bytearray`, and aborts once the count exceeds
   `limit`.

Slack signature verification continues to receive the assembled `bytes` —
identical contract to today.

### Why route-scoped, not ASGI middleware?
- Need auth context available first (A8); ASGI middleware would have to
  duplicate the auth/path matching logic.
- Limit is per-route (`/ingest/*`); other routes shouldn't pay the cost or
  be constrained.
- Keeps the change diff small and easy to test.

## 2. Files touched
| File | Change |
|------|--------|
| [services/gateway/main.py](../../../services/gateway/main.py) | Add `_precheck_ingest_size()` + `_read_bounded_body()` helpers; call them at the top of `post_ingest`. Replace `await request.body()`. |
| [services/gateway/tests/test_ingest_endpoint.py](../../../services/gateway/tests/test_ingest_endpoint.py) | Add tests for A1–A4, A6, A8. Existing oversize test (A1 variant) stays. |

No DB migration. No new dependency. No config changes.

## 3. Detailed design

### 3.1 `_precheck_ingest_size(request) -> JSONResponse | None`
Returns a `JSONResponse` to short-circuit, or `None` to continue.

```python
def _precheck_ingest_size(request: Request) -> JSONResponse | None:
    te = request.headers.get("transfer-encoding", "").lower()
    if "chunked" in te:
        return JSONResponse(
            {"error": "payload_too_large", "reason": "chunked_unsupported"},
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
        )
    cl_raw = request.headers.get("content-length")
    if cl_raw is not None:
        try:
            cl = int(cl_raw)
        except ValueError:
            return JSONResponse(
                {"error": "invalid_content_length"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if cl < 0 or cl > MAX_PAYLOAD_BYTES:
            return JSONResponse(
                {"error": "payload_too_large", "max_bytes": MAX_PAYLOAD_BYTES},
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            )
    return None
```

### 3.2 `_read_bounded_body(request, limit) -> bytes | JSONResponse`
Defense in depth: even with `Content-Length` honest, abort if the actual
stream exceeds the limit.

```python
async def _read_bounded_body(
    request: Request, limit: int
) -> bytes | JSONResponse:
    buf = bytearray()
    async for chunk in request.stream():
        buf.extend(chunk)
        if len(buf) > limit:
            return JSONResponse(
                {"error": "payload_too_large", "max_bytes": limit},
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            )
    return bytes(buf)
```

### 3.3 Call sites in `post_ingest`
```python
if (early := _precheck_ingest_size(request)) is not None:
    return early
maybe = await _read_bounded_body(request, MAX_PAYLOAD_BYTES)
if isinstance(maybe, JSONResponse):
    return maybe
raw = maybe
```
The rest of the handler (Slack sig, `json.loads`, `ingest(...)`) is unchanged.

### 3.4 Structured JSON-decode error (G4)
Wrap the existing `json.loads(raw)` to include a `detail` field with the
exception's `msg` (no traceback):

```python
try:
    payload = json.loads(raw)
except json.JSONDecodeError as e:
    return JSONResponse(
        {"error": "invalid_json", "detail": e.msg},
        status_code=400,
    )
```

## 4. Test strategy
Test file: `services/gateway/tests/test_ingest_endpoint.py`. Pattern follows
the existing `test_ingest_oversized_payload_returns_413`.

| Test name | Maps to | Notes |
|-----------|---------|-------|
| `test_ingest_rejects_oversize_content_length` | A1, A2 | Send `Content-Length` header above limit with an empty body; assert 413 and that handler did not call `ingest()`. |
| `test_ingest_rejects_chunked_transfer_encoding` | A3 | Set `Transfer-Encoding: chunked` header; assert 413 with `reason=chunked_unsupported`. |
| `test_ingest_streamed_body_exceeds_limit` | A4 | POST without `Content-Length` (httpx forces it; use a generator content) with > limit bytes; assert 413. |
| `test_ingest_invalid_json_returns_structured_400` | A6 | Body `b"{not json"`; assert `{"error":"invalid_json","detail":...}`. |
| `test_ingest_oversize_before_auth_still_401` | A8 | No bearer + oversize header; assert 401. |
| existing `test_ingest_oversized_payload_returns_413` | A1 (legacy) | Keep — still passes. |
| `test_slack_signature_still_validates_after_bounded_read` | A5 | Valid signature + 500 KB JSON; assert 200/201. |

## 5. Performance / safety
- Bounded reader allocates at most `limit + max_chunk_size` bytes
  (uvicorn default chunk ~64 KiB) → safe.
- Header-only rejection path: no allocation beyond the JSONResponse body
  (~80 bytes). Well within the <5 ms target.

## 6. Rollout
- Single PR onto `demo-deploy`.
- No feature flag — the change strictly tightens behaviour that callers
  shouldn't be relying on (oversize/chunked already failed, just later).
- No migration, no env var.

## 7. Verification checklist before merge
- `pytest services/gateway/tests/test_ingest_endpoint.py -q` green.
- `pytest services/ingestion/tests/` still green (no regression in core).
- Manual `curl -H "Content-Length: 999999999" -H "Authorization: Bearer ..."
  http://localhost:8000/ingest/slack:message` returns 413 instantly.
- `git diff --stat` shows only the two files in §2.
