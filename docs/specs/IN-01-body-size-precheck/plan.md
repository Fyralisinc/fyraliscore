# IN-01 — Implementation Plan

Spec: [./spec.md](./spec.md)

## 1. Approach — FastAPI dependency (option b)
Introduce a single FastAPI dependency, `ingest_body_bytes`, that the
`post_ingest` route consumes via `Depends(...)`. The dependency runs
**after** middleware (so `auth` already ran — preserves A8) and **before**
the route handler body, and it does:

1. Reject `Transfer-Encoding: chunked` → raise `IngestSizeError(413,
   {"error":"payload_too_large","reason":"chunked_unsupported"})`.
2. Parse `Content-Length`; reject if `> MAX_PAYLOAD_BYTES`.
3. Stream the body via `request.stream()`, accumulating into a `bytearray`
   and aborting at `MAX_PAYLOAD_BYTES + 1` (defense in depth).
4. Return the validated `bytes` to the handler.

Custom exception + handler pair (`IngestSizeError` →
`ingest_size_error_handler`) keeps response shape **flat** (i.e.
`{"error":"...","max_bytes":...}`), matching today's `JSONResponse` body
rather than FastAPI's default `{"detail":{...}}` wrapping.

Slack signature verification and `json.loads` continue to use the bytes
returned by the dependency — contract unchanged.

### Why a dependency, not inline / not ASGI middleware?
- **vs. ASGI middleware:** middleware would need to re-derive `/ingest/`
  path matching and could trip on demo bypass paths; a dependency is
  surgical to the one route.
- **vs. inline helpers:** dependencies are testable in isolation,
  composable with future routes (e.g. webhook ingest), and naturally
  short-circuit the handler via the exception handler — no `if isinstance(
  ret, JSONResponse): return ret` boilerplate inside the route.
- Auth still runs first because it's middleware, executed before any
  endpoint dependency.

## 2. Files touched
| File | Change |
|------|--------|
| [services/gateway/main.py](../../../services/gateway/main.py) | Add `IngestSizeError`, `ingest_body_bytes` dependency, register exception handler in `build_app`. Replace `await request.body()` in `post_ingest` with `raw: bytes = Depends(ingest_body_bytes)`. Structured JSON error includes `detail`. |
| [services/gateway/tests/test_ingest_endpoint.py](../../../services/gateway/tests/test_ingest_endpoint.py) | Add tests for A1–A4, A6, A8. Existing oversize test (A1 variant) stays. |

No DB migration. No new dependency. No config changes.

## 3. Detailed design

### 3.1 `IngestSizeError` + handler
```python
class IngestSizeError(Exception):
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self.payload = payload

async def ingest_size_error_handler(
    request: Request, exc: IngestSizeError
) -> JSONResponse:
    return JSONResponse(exc.payload, status_code=exc.status_code)
```
Registered in `build_app` via `app.add_exception_handler(IngestSizeError,
ingest_size_error_handler)`.

### 3.2 `ingest_body_bytes` dependency
```python
async def ingest_body_bytes(request: Request) -> bytes:
    te = request.headers.get("transfer-encoding", "").lower()
    if "chunked" in te:
        raise IngestSizeError(
            status.HTTP_413_CONTENT_TOO_LARGE,
            {"error": "payload_too_large", "reason": "chunked_unsupported"},
        )
    cl_raw = request.headers.get("content-length")
    if cl_raw is not None:
        try:
            cl = int(cl_raw)
        except ValueError:
            raise IngestSizeError(
                status.HTTP_400_BAD_REQUEST,
                {"error": "invalid_content_length"},
            )
        if cl < 0 or cl > MAX_PAYLOAD_BYTES:
            raise IngestSizeError(
                status.HTTP_413_CONTENT_TOO_LARGE,
                {"error": "payload_too_large",
                 "max_bytes": MAX_PAYLOAD_BYTES},
            )
    buf = bytearray()
    async for chunk in request.stream():
        buf.extend(chunk)
        if len(buf) > MAX_PAYLOAD_BYTES:
            raise IngestSizeError(
                status.HTTP_413_CONTENT_TOO_LARGE,
                {"error": "payload_too_large",
                 "max_bytes": MAX_PAYLOAD_BYTES},
            )
    return bytes(buf)
```

### 3.3 Updated route signature
```python
@app.post("/ingest/{channel:path}")
async def post_ingest(
    channel: str,
    request: Request,
    raw: bytes = Depends(ingest_body_bytes),
) -> JSONResponse:
    ...
```
The `raw = await request.body()` line and the old length check are removed;
everything downstream (Slack sig verification, `json.loads`, `ingest(...)`)
continues to use `raw`.

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
