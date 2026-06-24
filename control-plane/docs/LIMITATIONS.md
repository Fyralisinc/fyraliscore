# Fyralis BYOC Control Plane — Known Limitations & Next-Sprint

> One triaged register of every known caveat for the BYOC control-plane MVP, consolidating
> the per-phase `BUILD_LOG.md` entries, the component `README.md` caveats, and the Phase-7
> CTO-handoff review (security / completeness-vs-design / operability lenses). Read this
> alongside [`architecture.md`](./architecture.md) (trust model + invariants I1–I6),
> [`reference.md`](./reference.md) (per-component config), and [`operations.md`](./operations.md)
> (runbooks).

**How to read this.** Each item is tagged with a **status** (`done` / `partial` / `next-sprint`)
and a **severity** (`critical` / `high` / `medium` / `low` / `n/a-by-design`). The two HIGH
items in §1 are the only things flagged as *fix-before-CTO-handoff*; everything else is either
complete or deliberately scoped out for a later sprint.

---

## 0. TL;DR — what is solid vs. what to fix

**Solid / complete (`done`).** The cryptographic trust core is genuinely strong and verified:
the cert→tenant→`X-Scope-OrgID` path (`auth-proxy/proxy.py` + `tenant_resolver.py`) is
fail-closed at every step, identity comes only from the verified SPIFFE SAN (never a header),
revocation is re-read per request (unknown == revoked), the SSRF guard in `_safe_upstream_path`
is sound, verify-before-apply (`signing/verify_bundle.py` + `agent/config_pull.py`) rejects
bad-sig/unknown-key/retired-key/wrong-kind, and the audit hash-chain + signed checkpoint +
break-glass mechanics are well-built. The golden-12 SLIs are all implemented; agents are
outbound-only (I2 proven 3 ways); no secrets are committed (keys / trust-root / `_runtime/`
are gitignored + bootstrap-minted). `make smoke` is 52/0/3 and reproducible.

**Fix before CTO handoff (`partial`, HIGH).** Two network-exposure / unauth-plane gaps in §1
(L-1, L-2). None is a wire-level isolation breach today (the metrics isolation plane I4 verified
clean), but the *shipped one-command compose* publishes trust-bypassing ports to the host and the
heartbeat/registry plane is unauthenticated. The third HIGH (L-3, the bring-up consistency trap)
is now **closed** — `bootstrap.sh` reconciles the persistent registry + bundle + runtime + `.env`
as one unit so a re-run always converges to a single consistent onboarded tenant.

**Intentionally next-sprint (`next-sprint`).** Live cloud / AWS deploy, formal SOC-2 audit,
P2 PrivateLink / air-gap / multi-cloud, RLS (only if the CP itself goes multi-tenant in one DB),
operator-Grafana authn hardening, and object-storage backends for Mimir/Loki. See §4.

---

## 1. HIGH — fix before CTO handoff

### L-1 (HIGH, partial) — Compose publishes trust-bypassing ports to the host (C5/I4)
**Where:** `docker-compose.control-plane.yml` — `mimir` (9009:9009), `loki` (3100:3100),
worker metrics / demo-dataplane (9300:9300), console (8080:8080).
**Issue:** The whole multi-tenant isolation guarantee (C5) is *"Mimir/Loki trust
`X-Scope-OrgID` only because it arrives from inside `cp-net` behind the proxy."* Publishing
these ports to `0.0.0.0` on the host breaks that premise: anyone who can reach the host on
`:9009` can send `X-Scope-OrgID: <victim-tenant>` straight to Mimir and read any tenant's
metrics, fully bypassing the auth-proxy, mTLS, cert verification and revocation. The shipped
"one-command bring-up" *is* the vulnerable config; the inline comment admits "in PRODUCTION do
NOT publish this" but the file ships it published and unguarded. (Source: SEC review CRITICAL;
COMPLETENESS notes it is network-scoped on `cp-net` + documented, so must-fix-before-exposure
rather than an active wire-level breach in the demo.)
**Fix now:** bind these to `127.0.0.1` (e.g. `"127.0.0.1:9009:9009"`) or remove the host
publish entirely and gate any debug publish behind an opt-in compose profile
(`profiles: [debug]`). All external access must traverse `auth-proxy:8443`. Add a
CI/selftest assertion that no service other than `auth-proxy` publishes a host port on
`0.0.0.0`.

### L-2 (HIGH, partial) — Console fleet-registry write API is unauthenticated; tenant_id is caller-asserted (FR-H / C4 / I4)
**Where:** `console/app.py` — `POST /api/v1/register`, `POST /api/v1/heartbeat`,
`DELETE /api/v1/deployments/{id}`; `agent/agent.py` POSTs heartbeats over plain
`http://console:8080`; compose publishes `8080:8080`.
**Issue:** These endpoints take `tenant_id` and the full `DeploymentRecord` straight from the
request body with no auth middleware and no client-cert binding. Unlike the (well-secured)
metrics plane, tenant identity on the heartbeat/registry plane is caller-**asserted**, not
cert-derived — an I4 inconsistency. Any caller on the network can spoof any tenant's heartbeat,
flip a deployment green/red, overwrite a real deployment's `tenant_id`/`license_expiry`/version,
or delete it from the operator's source-of-truth fleet view. `console/README.md` and
`BUILD_LOG.md` self-report this ("No auth on the console API … do not publish :8080") yet the
shipped compose publishes it and routes the agent at it in cleartext.
**Fix now (minimum):** stop publishing `:8080` to the host this sprint (matches the team's own
caveat); document that the console must never bind to an untrusted interface.
**Next sprint:** route heartbeats through the same mTLS auth-proxy and derive `tenant_id` from
the cert SAN (reject a record whose body `tenant_id` != cert tenant), or require a signed
heartbeat / per-deployment bearer token, or front the console with operator SSO.

### L-3 (HIGH, done) — Bootstrap idempotency ignores the persistent tenant registry → silent 403-everything (Path-B bring-up)
**Where:** `bootstrap.sh` onboard idempotency guard previously keyed off `_runtime/` material
(`license.json` + `client.crt` + `.env`) and never re-checked the **tracked** persistent file
`ca/tenant_registry.json` that the auth-proxy bind-mounts read-only.
**Issue:** Reproduced live: `./bootstrap.sh --no-docker` printed "demo tenant acme already
onboarded" and exited 0 while `ca/tenant_registry.json` was `{}`. Because the registry is a
tracked file committed as `{}`, any `git stash` / `git checkout -- .` / `git pull` / interrupted
`make clean` that resets it while `_runtime/` survives leaves the tree inconsistent. The stack
then comes up with an empty registry: the boundary cert resolves to "unknown", the proxy
fail-closes to **403 on every push**, and the entire metric-flow + isolation demo silently
produces zero series.
**FIXED.** The onboard step now drives a reconciler (`onboarding/reconcile.py`, `make reconcile`)
that inspects all four durable artifact groups **together** — the ACTIVE row(s) in
`ca/tenant_registry.json`, the on-disk bundle (`onboarding/bundles/<id>/`), the staged `_runtime/`
material, and the `.env` deployment binding — and converges:
* **consistent** (one active row whose fingerprint == one complete bundle, runtime + `.env` agree)
  → skip onboarding (true idempotency);
* **absent** → onboard fresh;
* **partial** (any fragment missing / mismatched / **duplicated**, incl. the `{}`-registry trap and
  orphan/duplicate bundles) → cleanly offboard *every* fragment (revoke + delete the registry
  row(s), rmtree the bundle dir(s), clear the staged `_runtime`, drop the stale
  `AGENT_DEPLOYMENT_ID`) then re-onboard, so a second `make bootstrap` always converges to exactly
  one consistent onboarded tenant.
`--no-docker` mode is preserved. Covered by `tests/test_bootstrap_idempotency.py` (runs the real
`bootstrap.sh --no-docker` twice in a /tmp copy + simulates registry-reset / runtime-wiped /
duplicate-bundle partial states and asserts convergence to exactly one active row + one bundle +
one staged runtime + one `.env` binding).

---

## 2. MEDIUM — next-sprint hardening (no isolation hole today)

### L-4 (MEDIUM, next-sprint) — Signing covers the artifact but not the manifest → undetected version downgrade (C2/I6)
**Where:** `signing/signing_lib.py` / `verify_bundle.py`. The ed25519 signature covers only the
canonical artifact bytes; the manifest (`key_id`, `version`, `artifact`, `signed_at`, `sha256`)
is not signed. The agent trusts `res.version` (from the manifest) for config/canary/rollback
decisions, so a MITM or compromised artifact server can keep a validly-signed older artifact
while rewriting the manifest `version` to mount an undetected downgrade (pin a tenant to a
known-vulnerable config/release). `key_id` swap and artifact-kind confusion mostly fail closed;
version-binding is genuinely absent.
**Next sprint:** sign over `canonical_json_bytes(manifest_minus_sig)` (have `verify_bundle`
re-derive the artifact digest from the signed manifest), or embed `version`+`artifact` into the
signed bytes; add an anti-rollback check in the agent (refuse a config/release whose signed
version is lower than the currently-applied one).

### L-5 (MEDIUM, partial) — T1 label redaction is a denylist, doc claims an allowlist (I1)
**Where:** `boundary/otel-collector-config.yaml` `transform/redact-labels` is an exact-key
denylist (`delete_key(attributes,"<key>")` per banned key); `redaction_allowlist.md` frames
Gate 2 as an enum allowlist and its worked example shows `user_email` being dropped — which the
denylist config would NOT drop. OTTL `delete_key` is exact-match, so unlisted keys like
`customer_email` / `actor_email` / `contact_email` on an allowlisted family would survive.
**Mitigation today:** Gate 1 (family allowlist) is the primary I1 gate and is real default-deny,
and the data-plane metrics lib forbids unbounded labels — so exposure requires an allowlisted
family to carry an unlisted PII-ish label. `selftest.py` only checks the keys it already lists,
so the gap is invisible to the test, and the doc overstates the guarantee.
**Next sprint:** make Gate 2 a true allowlist (OTTL `keep_keys` of the bounded-enum label set
after family filtering); until then fix the doc/worked-example to match the denylist reality and
add a `*_email` / `*_id` substring scrub.

### L-6 (MEDIUM, next-sprint) — `__fleet__` cross-tenant federation not enabled → fleet SLIs silently empty
**Where:** `fleet-sli/README.md` + `grafana` `datasources.yaml` say `__fleet__` reads across
tenants via Mimir tenant-federation, but `mimir.yaml` contains NO `tenant_federation` config.
Without `ruler.tenant_federation`, the `__fleet__` ruler evaluates only over series stored under
the (empty) `__fleet__` tenant, so every `fleet:*` / `fyralis:*` recording rule and all golden-12
fleet SLIs/alerts/SLO burn-rates silently produce nothing. Security-adjacent: once enabled this
is the *sole* documented cross-tenant read and must be scoped to the operator deliberately, not
improvised via the "point `__fleet__` at acme|globex" fallback.
**Next sprint:** enable and pin `ruler.tenant_federation` (+ query-frontend `tenant_federation`
for the fleet datasource) in `mimir.yaml`; document `__fleet__` as the sole intended cross-tenant
reader; add an integration assertion that a `fleet:*` series is non-empty after a tenant
remote-writes.

### L-7 (MEDIUM, next-sprint) — No auth-proxy route to Loki → T2 log path has no cert→tenant injection (C3/I4)
**Where:** the auth-proxy upstream is a single hardcoded URL (`http://mimir:9009`); there is no
path-based routing to `loki:3100`. The boundary collector is T1-only today (logs/traces pipelines
exist only in `tier_policy.yaml`, not wired), so nothing leaks now — but the design says T2
redacted logs egress through the same mTLS boundary with cert→tenant injection, and no such
authenticated ingress exists for Loki.
**Next sprint:** add Loki to the proxy's routing (path-prefix or second listener) so `/loki/*` is
reverse-proxied with the same cert-derived `X-Scope-OrgID` injection, **before** T2 is offered to
any customer. Until then, document T2 as not-yet-wired so no one enables it expecting isolation.

### L-8 (MEDIUM, next-sprint) — `FleetSLOLivenessDeploymentSilent` cannot fire for a fully-vanished deployment (NFR-5)
**Where:** `fleet-sli/slo_burnrate_rules.yml:176-178`, expr
`max by (tenant_id, deployment_id) (up{job=~"fyralis-.*"}) == 0`. When a data plane goes fully
silent (the exact NFR-5 "dead deployment within ~90s" scenario) the `up` series stops existing in
Mimir; PromQL `== 0` only matches present-and-zero series, so a vanished series yields an empty
vector and the alert never fires. `up==0` still catches scraped-but-all-workers-down, so it is not
useless — but it misses total silence, which is met today only by the console registry's read-time
`last_heartbeat` derivation, not by the fleet alert that advertises it.
**Next sprint:** add an `absent()` / last-heartbeat-staleness alert keyed per known deployment
(e.g. `time() - max by(tenant_id,deployment_id)(fyralis_agent_last_heartbeat_ts) > 90`); keep the
`up==0` rule as the partial-down signal.

### L-9 (MEDIUM, partial) — Real-Mimir end-to-end round-trip is a skipped/placeholder test (P6 exit gate)
**Where:** `tests/` — the headline guarantee that a boundary remote-write lands in a real Mimir
tenant-scoped and is query-isolated is only exercised by
`test_step_live_metric_against_real_mimir_container`, which is `--live-docker`-gated and is in
fact a pure `pytest.skip` placeholder. The default-run e2e *does* exercise the real auth-proxy +
real client cert + scope-injection + spoof-override + isolation (strong) — but against an
in-process `MockMimir`, not the `grafana/mimir:2.13.0` image. So the compose-level coherence of
the ruler-loader push, distroless-Mimir readiness, and a genuine remote-write→query round-trip are
never asserted by CI; P6's exit gate is satisfied by mocks + a skipped live step.
**Next sprint:** implement the live-docker step for real (bring the stack up in CI, remote-write
via the boundary collector, query back via the operator datasource, assert acme sees its series
and globex does not) so the real-Mimir round-trip is a gating test, not a skip.

### L-10 (MEDIUM, next-sprint) — `mimir-ruler-loader` is a non-reconciling one-shot
**Where:** the `mimir-ruler-loader` (`restart: "no"`) is the authoritative path that pushes the
golden-12 recording rules + 17 alerts + 4 SLO burn-rate rules into the `__fleet__` ruler tenant
(the on-disk mount is parity-only — Mimir's filesystem ruler does not read the multi-group YAML
dir). If the one-shot loader fails after a Mimir restart/recovery, or the ruler tenant storage is
wiped, the rules silently disappear with no always-on reconciler and no "ruler has zero groups"
alert — the fleet would look healthy while all fleet alerting is gone.
**Next sprint:** make rule-loading converge continuously (a tiny periodic re-apply, or a self-obs
check that `fyralis:`/`fleet:` rule series exist) and add a cp-self-obs alert for "fleet ruler
groups == 0".

---

## 3. LOW — minor / doc-accuracy / blast-radius

### L-11 (LOW, partial) — Break-glass approver identity not bound to the tenant / not authenticated (I5)
**Where:** `audit/breakglass.py` `approve_grant` accepts any `approved_by` string and does not
verify the approver is the customer principal owning the tenant in the grant scope. The mechanics
(customer-approved / scoped / time-boxed / audited) are correct, but the *binding of approver
identity to tenant* is left to the (unauthenticated) API layer, so a vendor operator could
self-supply `approved_by=customer`. The audit trail records who-claimed-approval, not
who-actually-approved. `BUILD_LOG.md` and `reference.md` note this; `TEST_GUIDE.md` §B.7 +
`operations.md` §8 previously presented I5 as customer-enforced — **doc overstatement, now corrected.**
**Fixed (docs):** `reference.md`, `BUILD_LOG.md`, `TEST_GUIDE.md` §B.7 step 2, and `operations.md`
§8 now state explicitly that approver identity is NOT authenticated in the MVP and that
authenticating the approver + binding identity-to-tenant is delegated to the console/auth-proxy
(this item, L-11). The runtime gap below is still open.
**Next sprint:** bind approval to an authenticated customer principal scoped to the grant's
tenant; assert `approved_by != requested_by` at the API boundary, not just in docs.

### L-12 (LOW, next-sprint) — Weak default operator-Grafana credential
**Where:** `docker-compose.control-plane.yml` sets `GF_SECURITY_ADMIN_PASSWORD` default to
`<see control-plane/.env>` (admin/admin user). Operator Grafana is the cross-fleet view (sees ALL tenants
via `__fleet__`); a deploy that forgets to override `GF_ADMIN_PASSWORD` ships a guessable admin
login to the single pane that aggregates every customer's telemetry.
**Next sprint:** remove the baked-in default and fail bring-up if `GF_ADMIN_PASSWORD` is unset (or
generate a random one in `bootstrap.sh` and print it once).

### L-13 (LOW, next-sprint) — Single signing key mounted across multiple containers (blast radius; NFR-7 / FR-G1)
**Where:** verified clean — `git ls-files` tracks no private keys / `*.pem` / `trust_root.json` /
tenant certs (`signing/keys/**`, `ca/pki/**`, `_runtime/` gitignored + bootstrap-generated).
However the CP ed25519 **private** signing key is bind-mounted read-only into BOTH the metering
and audit containers (and the active signing home into config-dist). It is the CP's own key on the
CP side (does NOT break FR-G1 — no customer secrets in the CP), but it widens the blast radius: a
compromise of the metering or audit container yields the key that signs releases/licenses/configs
that agents trust. `rotation.py` + `key_id` keyring make rotation possible, but the MVP runs one
key in several places.
**Next sprint:** consider a dedicated signing service (or per-purpose subkeys) so
release/license/config signing is not reachable from the metering/audit blast radius; at minimum
document the single-key blast radius in the security model + rotation drill.

### L-14 (LOW, next-sprint) — TEST_GUIDE doc-accuracy defects
- **B.7 break-glass walkthrough** previously hard-coded the literal grant-id `bg-1a2b3c4d5e6f`; real
  grant ids are `bg-` + random uuid4 hex (`audit/breakglass.py:212`), so copy-paste hit "no such
  grant". **FIXED:** step 1 now captures the printed id into a `GID` shell var (as `operations.md`
  §8 does) and step 2 approves `"$GID"`, so the flow is copy-paste runnable. The expected `audit
  list` output was also corrected — seqs are 0-based and the 4th event is `breakglass.check_denied`
  (from the over-broad check), not `breakglass.deny` (no `deny` command is run); the tamper demo now
  shows `CHAIN BROKEN at seq 0` (sed line 1 == seq 0).
- **Troubleshooting cert-SAN claim** says the proxy server cert SAN is "auth-proxy, not
  localhost" and therefore needs `--resolve auth-proxy:8443:127.0.0.1`. The minted cert SANs are
  `['localhost', 127.0.0.1, 'auth-proxy']` — localhost IS a SAN; the `--resolve` trick is harmless
  but unnecessary and the statement is factually wrong. **Next sprint (doc-only).**
- **Service inventory** said "all 16 services"; compose defines 17. **FIXED:** the Path-B
  intro now says "all 17 services". (The B.2 table still omits `config-dist` and
  `release-registry` rows and uses non-canonical container labels for `console`/`fyralis-agent` —
  table-row backfill remains **next sprint, doc-only**.)

---

## 4. Intentionally out of scope this sprint (`next-sprint` by design)

These were **deliberately deferred** in the BYOC MVP plan; they are not defects and not regressions.

| Item | Status | Why deferred / what it needs |
|---|---|---|
| **Live cloud / AWS deploy** | `next-sprint` | MVP is a one-command local Docker bring-up + an in-process e2e smoke. Production path is Helm/Terraform on the same bundle contract (`installer/`); no cloud account, IAM, or live VPC peering is provisioned. |
| **Formal SOC-2 (Type II) audit** | `next-sprint` | The trust model, audit hash-chain, and break-glass mechanics are *built* and tamper-evident, but no third-party audit, control-evidence collection, or continuous-compliance tooling is in place. Audit is tamper-**evident**, not tamper-**proof** (ship to a WORM/remote sink for prevention). |
| **P2: PrivateLink / air-gap / multi-cloud** | `next-sprint` | The MVP boundary egress is outbound mTLS over the public internet to the vendor CP. AWS PrivateLink / GCP PSC, fully air-gapped (offline-license) operation, and multi-cloud control planes are P2 design items, not built. |
| **RLS (row-level security)** | `next-sprint` | Only needed *if* the CP itself becomes multi-tenant in a single shared DB. Today tenant isolation is enforced cryptographically at the auth-proxy (I4) and per-tenant in Mimir/Loki via `X-Scope-OrgID`; the console registry is single-node JSON, not a multi-tenant RDBMS. Revisit when the console/registry moves to a shared DB. |
| **Operator-Grafana authn hardening / SSO** | `next-sprint` | Grafana ships with a default admin credential (L-12) and no SSO. The cross-fleet operator pane should sit behind operator SSO before exposure beyond the local host. |
| **Object storage for Mimir / Loki** | `next-sprint` | Both run on **filesystem** blocks/chunks under named Docker volumes (`mimir-data` / `loki-data`) for the testable bring-up. Production needs S3/GCS object storage — also the prerequisite for the blue-green stateful-upgrade path documented in `upgrade/UPGRADE_RUNBOOK.md` §3 (which assumes shared object storage). |
| **KMS/HSM-backed signing** | `next-sprint` | Keys-on-disk is the dev path; the `Keyring` is structured to swap in a remote KMS/HSM signer without changing the manifest or verifier. Production custody is KMS/HSM. |
| **Pull-based license revocation latency** | `n/a-by-design` | Revocation propagation = the agent's list-refresh cadence, by design — the agent is outbound-only / offline-capable (I2/I3). Cert revocation at the proxy is immediate (registry re-read per request). |
| **Console DELETE-on-offboard** | `n/a-by-design` | The P4 console contract has no DELETE verb, so against a real console an offboarded deployment ages to red/expired rather than being deleted. Proxy revocation is immediate regardless. |
| **Wall-clock break-glass expiry (no skew grace)** | `n/a-by-design` | Fails toward expiring sooner — intentional for short-lived high-stakes access. |
| **G5 coded-but-not-running workers (anomaly-processor / deadline-resolver)** | `n/a-by-design` | The boundary scrape targets these intentionally so the gap surfaces as `up==0` in the fleet view rather than being hidden. |
| **MINIMAL data-plane subset in the installer overlay** | `n/a-by-design` | `installer/deployment.compose.yml` runs a minimal pg/redis/kafka subset; the full worker fleet surfaces as `up==0`. Run the root `docker-compose.yml` on `dp-net` for the full fleet. |
| **Metering period-math under-count** | `n/a-by-design` | A data-plane gap or never-reporting deployment yields a valid all-zero bill; cross-check the C4 heartbeat to distinguish "no usage" from "no telemetry". `think_cost_usd` is self-reported; reconcile against the provider bill. |
| **shellcheck on upgrade scripts** | `next-sprint` | shellcheck absent in the build env; fell back to `bash -n`. |

---

## 5. Triage summary

| ID | Item | Severity | Status |
|---|---|---|---|
| L-1 | Compose publishes trust-bypassing ports to host (C5/I4) | high | partial — **fix now** |
| L-2 | Console registry/heartbeat API unauthenticated (FR-H/C4/I4) | high | partial — **fix now** |
| L-3 | Bootstrap idempotency ignores persistent registry → 403-all | high | **done** (reconciler + tests) |
| L-4 | Manifest not covered by signature → version downgrade (C2/I6) | medium | next-sprint |
| L-5 | T1 label redaction denylist vs documented allowlist (I1) | medium | partial |
| L-6 | `__fleet__` tenant-federation not enabled → fleet SLIs empty | medium | next-sprint |
| L-7 | No auth-proxy route to Loki (T2 log path) (C3/I4) | medium | next-sprint |
| L-8 | `FleetSLOLivenessDeploymentSilent` can't fire on total silence | medium | next-sprint |
| L-9 | Real-Mimir round-trip is a skipped/placeholder test | medium | partial |
| L-10 | `mimir-ruler-loader` is a non-reconciling one-shot | medium | next-sprint |
| L-11 | Break-glass approver not authenticated/tenant-bound (I5) | low | partial (docs fix now) |
| L-12 | Weak default operator-Grafana credential | low | next-sprint |
| L-13 | Single signing key across multiple containers (blast radius) | low | next-sprint |
| L-14 | TEST_GUIDE doc-accuracy defects | low | partial |
| §4 | Cloud deploy / SOC-2 / P2 / RLS / object-storage / KMS … | n/a | next-sprint by design |

**Handoff verdict.** No CRITICAL open at the wire level in the demo (metrics-plane isolation I4
verified clean). Two HIGH items remain as gates for a CTO handoff (L-1, L-2) — both
"do-not-publish-to-host" one-line compose changes that match the team's own caveats. The third
HIGH (L-3, the bootstrap idempotency / registry-consistency trap) is **fixed**: `bootstrap.sh`
now reconciles the registry + bundle + runtime + `.env` together (`onboarding/reconcile.py`,
`make reconcile`) and is covered by `tests/test_bootstrap_idempotency.py`. Everything else is
medium/low hardening or deliberately next-sprint.
