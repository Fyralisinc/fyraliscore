# IN-01 — Task Breakdown

Spec: [./spec.md](./spec.md) · Plan: [./plan.md](./plan.md)

Branch: `feat/in-01-body-size-precheck` (off `demo-deploy`).

Tasks are ordered. Each one is small enough to land + verify before moving on.

---

## T1 — Add `_precheck_ingest_size` helper
**File:** [services/gateway/main.py](../../../services/gateway/main.py)
**Where:** near the top of `create_app()` or just above `post_ingest` (alongside `_deps`/`_unauth`).

- Implement the helper per [plan.md §3.1](./plan.md).
- Handles three cases: chunked TE → 413, bad `Content-Length` → 400, oversize CL → 413.
- Returns `JSONResponse | None`.

**Done when:** function exists, imports resolve, `python -c "from services.gateway.main import create_app"` still works.

---

## T2 — Add `_read_bounded_body` helper
**File:** [services/gateway/main.py](../../../services/gateway/main.py)

- Implement per [plan.md §3.2](./plan.md).
- Uses `request.stream()`, accumulates into `bytearray`, aborts at `limit + 1`.
- Returns `bytes | JSONResponse`.

**Done when:** helper compiles; unit test (T6 below) drives it.

---

## T3 — Wire helpers into `post_ingest`
**File:** [services/gateway/main.py:573-650](../../../services/gateway/main.py#L573-L650)

- After the existing `auth` check (preserves A8), insert the precheck call.
- Replace `raw = await request.body()` with the bounded read.
- Keep Slack signature verification using the resulting `raw` bytes (A5).
- Wrap `json.loads(raw)` to return `{"error":"invalid_json","detail": e.msg}` (G4).
- Delete the now-redundant `if len(raw) > MAX_PAYLOAD_BYTES:` block.

**Done when:** the handler matches [plan.md §3.3](./plan.md); existing happy-path test still passes locally.

---

## T4 — Test: oversize `Content-Length` rejected pre-read (A1, A2)
**File:** [services/gateway/tests/test_ingest_endpoint.py](../../../services/gateway/tests/test_ingest_endpoint.py)

- Add `test_ingest_rejects_oversize_content_length`.
- POST with `Content-Length: <MAX+1>` and **empty body** + valid bearer.
- Assert `resp.status_code == 413` and `resp.json()["error"] == "payload_too_large"`.

**Done when:** test passes; without the precheck it would either hang or 400 due to length mismatch — current code would have read the (empty) body and missed the attack vector.

---

## T5 — Test: chunked transfer-encoding rejected (A3)
**File:** same as T4.

- Add `test_ingest_rejects_chunked_transfer_encoding`.
- Send `Transfer-Encoding: chunked` header with a small body.
- Assert `413` and `reason == "chunked_unsupported"`.

**Done when:** test passes.

---

## T6 — Test: streamed body exceeds limit (A4)
**File:** same as T4.

- Add `test_ingest_streamed_body_exceeds_limit`.
- Use httpx with a generator body (no Content-Length) producing > `MAX_PAYLOAD_BYTES` total.
- Assert `413`.

**Done when:** test passes — proves the bounded reader trips even when the header is absent.

---

## T7 — Test: invalid JSON → structured 400 (A6)
**File:** same as T4.

- Add `test_ingest_invalid_json_returns_structured_400`.
- Body: `b"{not json"`, valid bearer, valid `Content-Length`.
- Assert `400`, body `{"error":"invalid_json","detail": <non-empty string>}`.

**Done when:** test passes; response is JSON (no traceback string).

---

## T8 — Test: oversize before auth still returns 401 (A8)
**File:** same as T4.

- Add `test_ingest_oversize_before_auth_still_401`.
- POST with oversize `Content-Length`, **no** `Authorization` header.
- Assert `401` and `error == "missing_bearer"`.

**Done when:** test passes — confirms middleware ordering.

---

## T9 — Test: Slack signature still validates with bounded reader (A5)
**File:** same as T4.

- Add (or extend existing slack test) `test_slack_signature_still_validates_after_bounded_read`.
- Build a 500 KB valid JSON body, compute a real Slack signature against it, POST.
- Assert `200` or `201`.

**Done when:** test passes — confirms HMAC sees identical bytes.

---

## T10 — Run full gateway + ingestion suites
```bash
pytest services/gateway/tests -q
pytest services/ingestion/tests -q
```
**Done when:** both green.

---

## T11 — Manual smoke
```bash
# Start gateway (docker compose or uvicorn) then:
curl -i -X POST http://localhost:8000/ingest/slack:message \
  -H "Authorization: Bearer <valid-token>" \
  -H "Content-Length: 999999999" \
  --data-binary ""
```
**Done when:** response is `413` within tens of milliseconds and `docker stats` shows no memory spike.

---

## T12 — Commit + PR
- Single commit on `feat/in-01-body-size-precheck`.
- Commit message references IN-01 and the spec doc.
- Open PR against `demo-deploy`; PR body links to [spec.md](./spec.md) and lists A1–A8 as the test matrix.

---

## Definition of done (rolls up)
- All acceptance criteria A1–A8 from [spec.md §6](./spec.md#6-acceptance-criteria) have a matching test.
- `git diff --stat` touches only the two files in [plan.md §2](./plan.md#2-files-touched).
- Smoke test (T11) confirms no memory blow-up on a header-only oversize POST.
- PR opened against `demo-deploy`.
