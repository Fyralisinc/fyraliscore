# WS-AUDIT — append-only hash-chained audit log + break-glass access (FR-G / I5)

This component is the control plane's **tamper-evident record of who did what**, plus the
**break-glass** emergency-access workflow. It implements **FR-G** and **invariant I5**:

> **I5 — break-glass access is _customer-granted, scoped, time-boxed, and audit-logged_.**

Two pieces, one shared trail:

| File | Role |
|---|---|
| `audit_log.py` | the **append-only, hash-chained** audit log — `append()` + `verify_chain()` |
| `breakglass.py` | the **break-glass** workflow — `request_grant` / `approve_grant` / `check_access` |
| `cli.py` | a small **admin CLI** over both (`audit ...` and `breakglass ...`) |
| `selftest.py` | the end-to-end proof (append → verify → tamper-detect → 1s grant in/out of window) |
| `Dockerfile` / `service.compose.yml` | containerized on-demand CLI + integrate-step fragment |

It is built on the control plane's existing primitives — it does **not** re-implement crypto:

| Reused from | What for |
|---|---|
| `control-plane/signing` (`signing_lib`) | ed25519 sign + verify of the chain-head **checkpoint**, canonical JSON, sha256, RFC-3339 time (C2 / I6) |

---

## 1. The audit log: append-only + hash-chained

`audit_log.AuditLog` is a JSON-Lines file where **every entry carries the hash of the previous
entry**, so tampering with ANY past entry breaks the chain.

```python
from audit_log import AuditLog
log = AuditLog("/data/audit.log.jsonl")          # created on first append; never truncated
log.append(actor="ops@fyralis", action="config.apply", target="acme", metadata={"version": 7})
log.append(actor="ops@fyralis", action="cert.revoke", target="bossco", metadata={"reason": "churn"})

res = log.verify_chain()
assert res.ok                                     # chain intact
```

### Entry shape (one JSONL line)

```json
{"seq":1,"ts":"2026-06-24T00:00:00Z","actor":"ops@fyralis","action":"config.apply",
 "target":"acme","metadata":{"version":7},"prev_hash":"<hex>","entry_hash":"<hex>"}
```

* `entry_hash` = `sha256( canonical_json( {seq, ts, actor, action, target, metadata, prev_hash} ) )`.
* `prev_hash` = the previous entry's `entry_hash`; the **genesis** entry's `prev_hash` is the fixed
  sentinel `"GENESIS"`.
* `canonical_json` is the **same** compact, sorted-key serialization the rest of the control plane
  signs with (from `signing_lib`), so a hash means exactly one thing everywhere.

### Why it's tamper-EVIDENT (two layers)

1. **Hash chain (intra-file).** Edit any field of a past entry → its `entry_hash` no longer matches
   its body, and the *next* entry's `prev_hash` no longer matches it. `verify_chain()` walks the
   chain and returns `ok=False` with `bad_seq` pinpointing the first broken entry.
2. **Signed checkpoint (whole-file).** A determined attacker could rewrite the *entire* file,
   recomputing every hash forward so the chain re-links. To defeat that, every `append()` also
   rewrites a **detached ed25519 signature over the current chain-head hash** (`<log>.checkpoint.json`),
   signed with the CP signing key (C2 / I6). An attacker **without the private signing key cannot
   re-sign** the new head, so `verify_chain(check_signature=True)` (the default) rejects the chain
   when its head doesn't match a valid signed checkpoint.

> Append-only is enforced **by construction**: `append()` only ever `O_APPEND`-writes one line and
> `fsync`s; it never seeks, truncates, or rewrites the log. (A determined root can always touch a
> file on disk — that is precisely what the hash chain + signed checkpoint **detect**. "Append-only"
> here is the access discipline; "tamper-evident" is the guarantee.)

`verify_chain()` returns a `ChainVerification(ok, reason, count, bad_seq, head_hash, signature_ok)`.
`signature_ok` is `True` (valid signed checkpoint), `False` (present but wrong/forged), or `None`
(no signing key on this host → chain-only mode; the hash chain still holds).

---

## 2. Break-glass: customer-granted, scoped, time-boxed, audit-logged (I5)

`breakglass.BreakGlass` is a small state machine over the audit log. Every transition is written to
the **hash-chained** log, so the break-glass trail is itself tamper-evident.

```python
from audit_log import AuditLog
from breakglass import BreakGlass

mgr = BreakGlass(AuditLog("/data/audit.log.jsonl"))

# 1. an operator REQUESTS a scoped, time-boxed grant (INERT until approved)
g = mgr.request_grant(actor="sre@fyralis", scope="tenant:acme/logs:read", ttl=900, reason="inc-4127")

# 2. the CUSTOMER approves it (this starts the time-box) — "customer-granted"
mgr.approve_grant(g.grant_id, approved_by="acme-admin@acme.com")

# 3. access is honored ONLY while approved + unexpired + in-scope — each use is audited
d = mgr.check_access(actor="sre@fyralis", scope="tenant:acme/logs:read")
assert d.allowed                                  # within the 900s window

# ... 900s later (or after mgr.revoke_grant(...)) ...
d = mgr.check_access(actor="sre@fyralis", scope="tenant:acme/logs:read")
assert not d.allowed                              # time-boxed: DENIED after expiry
```

Each invariant clause maps to a concrete mechanism:

| I5 clause | How |
|---|---|
| **customer-granted** | a `request_grant` is **inert** until `approve_grant(...)` by a **customer** principal (`approved_by`, distinct from the requesting operator). An unapproved/denied grant authorizes nothing. |
| **scoped** | a grant authorizes exactly its `scope` string; `check_access` matches it (with an explicit opt-in `tenant:acme/*` wildcard for sub-scopes). It never authorizes a sibling/broader scope. |
| **time-boxed** | `ttl` seconds, counted **from approval**. After `expires_at`, access is DENIED and the grant auto-transitions to `expired` (lazily, on the next check or sweep). |
| **audit-logged** | every transition — `request`, `approve`, `deny`, **`use`** (each exercised access), `expire`, `revoke`, plus denied checks — is appended to the hash-chained log. |

Grant lifecycle: `requested → (approve)→ approved → (ttl elapses / revoke)→ expired / revoked`;
or `requested → (deny)→ denied`. State persists to a JSON projection (`breakglass_grants.json`)
beside the log; **the audit log is the authoritative event history** — the store is a fast index.

Expiry is **lazy + idempotent**: `check_access` (and the explicit `sweep_expirations()`, which a
cron/daemon can call) expires any elapsed grant and emits **exactly one** `breakglass.expire` event
per grant, never re-emitting on later checks.

---

## 3. Admin CLI

```bash
PY=/path/to/.venv/bin/python      # any venv with `cryptography`

# --- audit trail ---
$PY cli.py audit append --actor ops@fyralis --action config.apply --target acme --meta '{"v":7}'
$PY cli.py audit verify                       # exit 0 = chain OK, 1 = tampered (prints the bad seq)
$PY cli.py audit list --limit 20

# --- break-glass ---
$PY cli.py breakglass request --actor sre@fyralis --scope tenant:acme/logs:read --ttl 900 --reason inc-1
$PY cli.py breakglass approve --grant-id bg-xxxxxxxxxxxx --approved-by acme-admin@acme.com
$PY cli.py breakglass check   --actor sre@fyralis --scope tenant:acme/logs:read   # exit 0 ALLOW / 1 DENY
$PY cli.py breakglass revoke  --grant-id bg-xxxxxxxxxxxx --revoked-by ops@fyralis  # kill before TTL
$PY cli.py breakglass sweep                    # expire elapsed grants now (audits each)
$PY cli.py breakglass list
```

Use `--log` / `--store` to point at a specific path (defaults live in this dir; the container
defaults to the `/data` volume). The CLI auto-loads the active signing key from
`control-plane/signing` when present (to sign the checkpoint); without one the log still
hash-chains and the checkpoint is left unsigned.

---

## 4. Self-test

```bash
$PY selftest.py        # exit 0 = all green
```

Proves end-to-end, through the **real** signing lib (against an isolated throwaway trust root under
a temp dir, so it never touches the repo's audit log or signing state):

* **A.** append several entries → `verify_chain()` **OK** (hash chain + signed checkpoint valid).
* **B.** flip a field in ONE past entry → `verify_chain()` **DETECTS** it and pinpoints the seq; a
  whole-file rewrite (every hash recomputed) re-links the chain but **fails the signed checkpoint**.
* **C.** request → customer-approve a **1-second** grant → `check_access` **ALLOWED** within the
  window, **DENIED** after expiry (and the grant auto-`expired`); a different scope is DENIED.
* **D.** assert each lifecycle event (`request`, `approve`, **`use`**, `expire`) produced an audit
  entry, expiry is audited exactly once, and the trail stays chain-valid throughout.

Current status: **23/23 checks pass.**

---

## 5. Container / integrate

`service.compose.yml` is the standalone fragment the integrate step merges into the master
`docker-compose.control-plane.yml`. It builds an **on-demand CLI** image (not a daemon): build
context is the control-plane **root** so the image includes `signing/`; the private signing key is
mounted **read-only** (never baked in); the audit log + grant store persist on the `audit-data`
volume so the chain survives restarts.

```bash
docker compose -f docker-compose.control-plane.yml run --rm audit audit verify
docker compose -f docker-compose.control-plane.yml run --rm audit \
    breakglass request --actor sre@fyralis --scope tenant:acme/logs:read --ttl 900
```

---

## 6. Caveats / non-goals

* **Tamper-EVIDENT, not tamper-PROOF.** A root user can always edit/delete the file on disk; the
  guarantee is *detection* (the chain + signed checkpoint), not *prevention*. For prevention,
  ship the log to append-only/WORM storage or a remote sink — out of scope here.
* **Signed checkpoint needs the signing key.** Whole-file tamper-evidence requires the active CP
  private key (mounted read-only) to sign each new head. Without it the log still **hash-chains**
  (intra-file evidence), but a full rewrite would go undetected — run the audit CLI on a host that
  has the signing key (or a co-signing sidecar) for the strong guarantee.
* **Checkpoint covers the head only.** The signature attests the current `(seq, head_hash)`, not
  every historical head. Truncating the log to a *prior* validly-signed head would verify (it is a
  consistent prefix); detecting truncation-to-an-earlier-checkpoint needs a monotonic external
  witness (e.g. periodically anchoring the head elsewhere) — a deliberate v2 item.
* **Single-writer assumption.** `append()` is process-safe (a thread lock serializes
  read-head → write-line → re-checkpoint), but the JSONL file is **not** designed for many
  concurrent OS processes appending to the same file; run one writer (the CLI/service) or front it
  with a queue. The hash chain would *detect* an interleaved corruption, but avoid it by design.
* **Wall-clock expiry.** The time-box is wall-clock; a host with a badly skewed clock can mis-judge
  expiry. There is no clock-skew grace here (break-glass is short-lived and high-stakes — fail
  toward *expiring sooner*, never later).
* **Grant store is a projection.** `breakglass_grants.json` is a fast-lookup index rebuilt from
  state; the **audit log is the source of truth** for the history. Deleting the store loses
  in-flight grant *state* but not the audited record of what happened.
* **Approval is a workflow gate, not an identity system.** `approved_by` records *who* approved;
  authenticating that principal (operator SSO / customer console auth) is the console's job, not
  this component's. The CLI trusts its caller (it sits behind the auth-proxy on `cp-net`).
* **No external DB.** State is files (JSONL log + JSON store/checkpoint) so the component is
  self-contained and the chain is auditable with `cat` + this verifier; a DB-backed sink is a
  scaling concern, not a correctness one.
