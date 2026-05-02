"""services/demo — VC pitch demo infrastructure.

Per DEMO-BUILD-PLAN: this module hosts everything that distinguishes a
demo tenant from a production tenant. The pieces split as:

  * `repo`         — read/write helpers for tenants, demo_configs,
                     demo_sessions, demo_session_costs.
  * `budget`       — per-session cost cap + LLM-call gating.
  * `notifications` — suppression check for outbound notifications when
                     a tenant is in demo mode.
  * `model_routing` — per-tenant override of the LLM model for Think /
                     Render / EntityResolver call sites.
  * `sessions`     — start / end / reset / inactivity sweep for the
                     demo_sessions lifecycle.
  * `simulator`    — pre-canned signal templates per company + the
                     /v1/demo/signals/inject endpoint dispatch.
  * `sse`          — Server-Sent Events stream for the action list.
  * `router`       — FastAPI APIRouter mounting every demo endpoint.

Mounted by services/gateway/main.py during `_register_routes`.
"""
