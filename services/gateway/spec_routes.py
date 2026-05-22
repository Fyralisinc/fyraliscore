"""services/gateway/spec_routes.py — FastAPI routes that serve the
Fyralis spec-aligned product views.

Currently exposes (all under /v1/...):

  GET  /v1/spec/operating_threads/                       — grouped thread board
  GET  /v1/spec/operating_threads/{id}                   — single thread
  GET  /v1/spec/operating_threads/recent_changes         — recent model changes strip
  GET  /v1/decision_deltas/spec                     — spec-shaped delta queue
  GET  /v1/decision_deltas/spec/{id}                — spec-shaped delta detail
  GET  /v1/forecasts/spec                           — spec-shaped forecasts
  GET  /v1/forecasts/spec/{id}                      — single forecast
  GET  /v1/ledger_events/                           — unified ledger events

The contracts and seed payloads mirror the TS fixtures in
ui/src/api/spec-mocks.ts. When the synthesis layer can derive these
shapes from the underlying substrate (models, model_edges, decisions,
predictions, topology_events), the seed payloads will be replaced with
DB-backed queries. Until then this keeps demo + e2e working
end-to-end with identical wire shapes.

Auth: tenant comes from `request.state.auth` (BearerAuthMiddleware).
The seed payloads are tenant-agnostic; production endpoints will scope
on `auth.tenant_id`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse


def _auth(request: Request) -> Any | None:
    return getattr(request.state, "auth", None)


def _unauth() -> JSONResponse:
    return JSONResponse({"error": "unauthorized"}, status_code=401)


# ---------------------------------------------------------------------
# Seed payloads. Each one mirrors ui/src/api/spec-mocks.ts exactly so
# the same UI works against the backend or against the in-browser
# mocks. Production code will replace these with DB queries; the wire
# shape stays unchanged.
# ---------------------------------------------------------------------

_BEACON = {"id": "cust-beacon", "type": "customer", "label": "Beacon", "subtitle": "$840K ARR"}
_NORTHVALE = {"id": "cust-northvale", "type": "customer", "label": "Northvale", "subtitle": "$620K ARR"}
_CONDUIT = {"id": "cust-conduit", "type": "customer", "label": "Conduit", "subtitle": "$580K ARR"}
_VPE = {"id": "actor-vpe", "type": "actor", "label": "VP Engineering", "subtitle": "Maya R."}
_CEO = {"id": "actor-ceo", "type": "actor", "label": "Diana", "subtitle": "CEO"}
_CS = {"id": "actor-cs", "type": "actor", "label": "Head of CS", "subtitle": "Priya S."}
_PLATFORM = {"id": "team-platform", "type": "team", "label": "Platform Engineering"}
_GTM = {"id": "team-gtm", "type": "team", "label": "GTM"}

_DEFAULT_SOURCES = [
    {"source": "support", "status": "connected", "label": "Zendesk"},
    {"source": "crm", "status": "connected", "label": "Salesforce"},
    {"source": "email", "status": "connected", "label": "Gmail"},
    {"source": "slack", "status": "limited", "label": "Slack"},
    {"source": "product", "status": "not_connected", "label": "Product usage"},
    {"source": "calendar", "status": "connected", "label": "Calendar"},
]

_DEFAULT_GAPS = [
    {
        "id": "gap-1",
        "kind": "missing_human_context",
        "text": "No recent Beacon call transcript.",
        "suggestedAction": "ask_owner",
        "target": _CS,
    },
    {
        "id": "gap-2",
        "kind": "owner_unconfirmed",
        "text": "Account owner has not confirmed severity.",
        "suggestedAction": "ask_owner",
        "target": _CS,
    },
    {
        "id": "gap-3",
        "kind": "limited_telemetry",
        "text": "Product usage telemetry unavailable.",
        "suggestedAction": "connect_source",
    },
]


def _trace(seed: str) -> dict[str, Any]:
    return {
        "id": f"trace-{seed}",
        "summary": "12 signals → 3 claims → 1 pattern → customer-risk update",
        "steps": [
            {
                "id": f"{seed}-obs-1",
                "kind": "observation",
                "title": "Beacon support ticket #7421",
                "description": "Sync failure during nightly batch.",
                "source": "support",
                "sourceLabel": "Zendesk",
                "occurredAt": "2026-05-15T07:12:00Z",
                "trustTier": "authoritative",
            },
            {
                "id": f"{seed}-obs-2",
                "kind": "observation",
                "title": "Northvale support ticket #7438",
                "description": "Sync delay >4h.",
                "source": "support",
                "sourceLabel": "Zendesk",
                "occurredAt": "2026-05-15T09:01:00Z",
                "trustTier": "authoritative",
            },
            {
                "id": f"{seed}-claim-1",
                "kind": "claim",
                "title": "Salesforce sync is failing on anchor accounts",
                "confidence": 0.84,
                "trustTier": "attested",
            },
            {
                "id": f"{seed}-pattern-1",
                "kind": "pattern",
                "title": "3 anchor accounts affected over 3 days",
                "confidence": 0.78,
            },
            {
                "id": f"{seed}-belief-1",
                "kind": "belief",
                "title": "Sync instability is now material to renewal risk",
                "confidence": 0.78,
            },
            {
                "id": f"{seed}-rec-1",
                "kind": "recommendation",
                "title": "Escalate customer risk to VP Engineering",
                "confidence": 0.78,
            },
        ],
    }


_THREADS: list[dict[str, Any]] = [
    {
        "id": "thread-customer-reliability",
        "lens": "company",
        "title": "Customer Reliability",
        "status": "under_pressure",
        "currentReading": "Salesforce sync instability is now threatening 3 anchor renewals.",
        "whyThisMatters": (
            "Anchor renewals worth $2.04M ARR depend on a stable CRM. Without intervention, "
            "renewal risk likely escalates within 48 hours."
        ),
        "anchorSubjects": [_BEACON, _NORTHVALE, _CONDUIT],
        "causalRibbon": [
            {"label": "Intent", "value": "Improve CRM reliability", "tone": "trust"},
            {"label": "Promise", "value": "Stabilize Salesforce sync", "tone": "trust"},
            {"label": "Friction", "value": "Eng capacity + owner gap", "tone": "review"},
            {
                "label": "Exposure",
                "value": "Beacon, Northvale, Conduit · $2.04M ARR",
                "tone": "critical",
                "refs": [_BEACON, _NORTHVALE, _CONDUIT],
            },
            {"label": "Next", "value": "Escalate ownership", "tone": "authority"},
        ],
        "semanticMass": {
            "representedNodes": 32,
            "changedToday": 12,
            "contested": 0,
            "blockedCommitments": 2,
            "affectedCustomers": 3,
            "arrAtRisk": 2_040_000,
            "typeCounts": {
                "commitments": 4,
                "risks": 3,
                "decisions": 2,
                "predictions": 2,
                "observations": 12,
            },
        },
        "trust": {
            "confidence": 0.78,
            "confidencePrevious": 0.61,
            "evidenceQuality": "strong",
            "sourceCoverage": _DEFAULT_SOURCES,
            "contextGaps": _DEFAULT_GAPS,
        },
        "accountability": {
            "owner": _VPE,
            "contributors": [_PLATFORM, _CS],
            "waitingOn": [_CS],
            "blocking": [],
            "loadSignal": "VPE load: high (3 active commitments)",
        },
        "relatedDecisionDeltaIds": ["delta-escalate-csr-sync"],
        "relatedForecastIds": ["forecast-beacon-renewal"],
        "relatedCommitmentIds": ["commit-sync-stability"],
        "hiddenStructure": [
            "3 unresolved support tickets in the last 72h",
            "1 unowned escalation thread in #cs-anchors",
            "Pricing decision unresolved; blocks data warehouse rollout",
        ],
        "whatChanged": [
            {"at": "2026-05-16T09:22:00Z", "note": "Customer-risk state moved Watch → Critical"},
            {"at": "2026-05-16T09:18:00Z", "note": "Forecast confidence rose 61% → 78%"},
            {
                "at": "2026-05-16T08:54:00Z",
                "note": "Commitment flagged: CRM reliability constrained by capacity",
            },
        ],
        "lastUpdatedAt": "2026-05-16T09:24:00Z",
    },
    {
        "id": "thread-engineering-capacity",
        "lens": "company",
        "title": "Engineering Capacity",
        "status": "needs_review",
        "currentReading": (
            "Platform team is forecasted to exceed 90% utilization by May 18; two anchor commitments at risk."
        ),
        "anchorSubjects": [_PLATFORM],
        "causalRibbon": [
            {"label": "Intent", "value": "Sustain delivery cadence", "tone": "trust"},
            {"label": "Promise", "value": "Ship Q2 platform roadmap", "tone": "trust"},
            {"label": "Friction", "value": "2 senior engineers on hiring", "tone": "review"},
            {"label": "Exposure", "value": "Sync stability · warehouse pricing", "tone": "review"},
            {"label": "Next", "value": "Pause new commitments", "tone": "authority"},
        ],
        "semanticMass": {
            "representedNodes": 24,
            "changedToday": 6,
            "contested": 1,
            "blockedCommitments": 2,
            "affectedCustomers": 0,
            "typeCounts": {"commitments": 5, "risks": 2, "decisions": 1, "predictions": 1, "observations": 9},
        },
        "trust": {
            "confidence": 0.74,
            "confidencePrevious": 0.74,
            "evidenceQuality": "medium",
            "sourceCoverage": _DEFAULT_SOURCES,
            "contextGaps": [
                {
                    "id": "gap-eng-1",
                    "kind": "limited_telemetry",
                    "text": "Sprint velocity from Linear is partial — only platform team is connected.",
                    "suggestedAction": "connect_source",
                }
            ],
        },
        "accountability": {
            "owner": _VPE,
            "contributors": [_PLATFORM],
            "waitingOn": [],
            "blocking": [_CS],
            "loadSignal": "Team load: high",
        },
        "relatedDecisionDeltaIds": ["delta-pause-commitments"],
        "relatedForecastIds": ["forecast-eng-utilization"],
        "relatedCommitmentIds": ["commit-q2-roadmap"],
        "hiddenStructure": [
            "5 active commitments owned by 4 engineers",
            "Sprint slip: 2 of 6 stories carried over",
        ],
        "whatChanged": [{"at": "2026-05-16T07:48:00Z", "note": "Utilization forecast: 88% → 92%"}],
        "lastUpdatedAt": "2026-05-16T08:01:00Z",
    },
    {
        "id": "thread-strategic-decisions",
        "lens": "company",
        "title": "Strategic Decisions",
        "status": "watch",
        "currentReading": "Data warehouse pricing decision is 6 days unowned; blocks 2 commitments.",
        "anchorSubjects": [],
        "causalRibbon": [
            {"label": "Intent", "value": "Land data warehouse rollout", "tone": "trust"},
            {"label": "Promise", "value": "Pricing aligned with anchor cohort", "tone": "trust"},
            {"label": "Friction", "value": "Authority unassigned", "tone": "review"},
            {
                "label": "Exposure",
                "value": "Beacon expansion timeline",
                "tone": "review",
                "refs": [_BEACON],
            },
            {"label": "Next", "value": "Assign decision owner", "tone": "authority"},
        ],
        "semanticMass": {
            "representedNodes": 14,
            "changedToday": 1,
            "contested": 0,
            "blockedCommitments": 2,
            "affectedCustomers": 1,
            "typeCounts": {"commitments": 2, "decisions": 1, "predictions": 0, "observations": 6},
        },
        "trust": {
            "confidence": 0.66,
            "evidenceQuality": "medium",
            "sourceCoverage": _DEFAULT_SOURCES,
            "contextGaps": [
                {
                    "id": "gap-strat-1",
                    "kind": "verbal_decision_unrecorded",
                    "text": "Pricing was discussed verbally Tuesday but not captured in connected sources.",
                    "suggestedAction": "add_context",
                }
            ],
        },
        "accountability": {
            "owner": None,
            "contributors": [_CEO],
            "waitingOn": [_CEO],
            "blocking": [],
            "loadSignal": "Authority required",
        },
        "relatedDecisionDeltaIds": ["delta-assign-pricing-owner"],
        "relatedForecastIds": [],
        "relatedCommitmentIds": ["commit-warehouse-pricing"],
        "hiddenStructure": [
            "Stalled in #strategy on 2026-05-10",
            "2 dependent commitments waiting on resolution",
        ],
        "lastUpdatedAt": "2026-05-16T06:14:00Z",
    },
    {
        "id": "thread-enterprise-gtm",
        "lens": "company",
        "title": "Enterprise GTM",
        "status": "healthy",
        "currentReading": "Pipeline coverage at 3.4x for Q3; 2 new design partners signed last week.",
        "anchorSubjects": [],
        "causalRibbon": [
            {"label": "Intent", "value": "Build enterprise pipeline", "tone": "trust"},
            {"label": "Promise", "value": "3x coverage by Q3 close", "tone": "trust"},
            {"label": "Friction", "value": "None material", "tone": "trust"},
            {"label": "Exposure", "value": "Q3 bookings", "tone": "trust"},
            {"label": "Next", "value": "Sustain cadence", "tone": "trust"},
        ],
        "semanticMass": {
            "representedNodes": 18,
            "changedToday": 2,
            "contested": 0,
            "blockedCommitments": 0,
            "affectedCustomers": 0,
            "typeCounts": {"commitments": 3, "decisions": 0, "predictions": 1, "observations": 7},
        },
        "trust": {
            "confidence": 0.85,
            "evidenceQuality": "strong",
            "sourceCoverage": _DEFAULT_SOURCES,
            "contextGaps": [],
        },
        "accountability": {
            "owner": {"id": "actor-cro", "type": "actor", "label": "CRO", "subtitle": "Alex Y."},
            "contributors": [_GTM],
            "waitingOn": [],
            "blocking": [],
        },
        "relatedDecisionDeltaIds": [],
        "relatedForecastIds": ["forecast-q3-pipeline"],
        "relatedCommitmentIds": [],
        "lastUpdatedAt": "2026-05-16T05:30:00Z",
    },
    {
        "id": "thread-revenue-retention",
        "lens": "company",
        "title": "Revenue Retention",
        "status": "watch",
        "currentReading": "Net retention tracking at 112%; 1 renewal slipped to Q3.",
        "anchorSubjects": [],
        "causalRibbon": [
            {"label": "Intent", "value": "Hold NRR above 110%", "tone": "trust"},
            {"label": "Promise", "value": "Retain anchor cohort", "tone": "trust"},
            {"label": "Friction", "value": "1 slipped renewal", "tone": "review"},
            {"label": "Exposure", "value": "Anchor cohort 2026 plan", "tone": "review"},
            {"label": "Next", "value": "Monitor", "tone": "trust"},
        ],
        "semanticMass": {
            "representedNodes": 15,
            "changedToday": 1,
            "contested": 0,
            "blockedCommitments": 0,
            "affectedCustomers": 1,
            "typeCounts": {"commitments": 2, "predictions": 1, "observations": 5},
        },
        "trust": {
            "confidence": 0.82,
            "evidenceQuality": "strong",
            "sourceCoverage": _DEFAULT_SOURCES,
            "contextGaps": [],
        },
        "accountability": {
            "owner": _CS,
            "contributors": [],
            "waitingOn": [],
            "blocking": [],
        },
        "relatedDecisionDeltaIds": [],
        "relatedForecastIds": [],
        "relatedCommitmentIds": [],
        "lastUpdatedAt": "2026-05-16T04:11:00Z",
    },
    {
        "id": "thread-product-delivery",
        "lens": "company",
        "title": "Product Delivery",
        "status": "healthy",
        "currentReading": "All Q2 commitments on track; cycle time down 18%.",
        "anchorSubjects": [],
        "causalRibbon": [
            {"label": "Intent", "value": "Ship Q2 plan", "tone": "trust"},
            {"label": "Promise", "value": "All anchor features", "tone": "trust"},
            {"label": "Friction", "value": "None material", "tone": "trust"},
            {"label": "Exposure", "value": "Customer satisfaction", "tone": "trust"},
            {"label": "Next", "value": "Sustain cadence", "tone": "trust"},
        ],
        "semanticMass": {
            "representedNodes": 21,
            "changedToday": 0,
            "contested": 0,
            "blockedCommitments": 0,
            "affectedCustomers": 0,
            "typeCounts": {"commitments": 4, "predictions": 0, "observations": 8},
        },
        "trust": {
            "confidence": 0.88,
            "evidenceQuality": "strong",
            "sourceCoverage": _DEFAULT_SOURCES,
            "contextGaps": [],
        },
        "accountability": {
            "owner": {"id": "actor-pm", "type": "actor", "label": "Head of Product", "subtitle": "Sam K."},
            "contributors": [_PLATFORM],
            "waitingOn": [],
            "blocking": [],
        },
        "relatedDecisionDeltaIds": [],
        "relatedForecastIds": [],
        "relatedCommitmentIds": [],
        "lastUpdatedAt": "2026-05-15T22:00:00Z",
    },
    {
        "id": "thread-board-investor",
        "lens": "company",
        "title": "Board & Investor",
        "status": "watch",
        "currentReading": "Q2 board materials due in 3 days; 1 forecast confidence below board threshold.",
        "anchorSubjects": [],
        "causalRibbon": [
            {"label": "Intent", "value": "Maintain board trust", "tone": "trust"},
            {"label": "Promise", "value": "Honest, calibrated readouts", "tone": "trust"},
            {"label": "Friction", "value": "Renewal forecast in flux", "tone": "review"},
            {"label": "Exposure", "value": "Series C narrative", "tone": "review"},
            {"label": "Next", "value": "Resolve sync risk before readout", "tone": "authority"},
        ],
        "semanticMass": {
            "representedNodes": 11,
            "changedToday": 2,
            "contested": 0,
            "blockedCommitments": 0,
            "affectedCustomers": 0,
            "typeCounts": {"commitments": 1, "decisions": 1, "predictions": 2, "observations": 4},
        },
        "trust": {
            "confidence": 0.7,
            "evidenceQuality": "medium",
            "sourceCoverage": _DEFAULT_SOURCES,
            "contextGaps": [],
        },
        "accountability": {
            "owner": _CEO,
            "contributors": [],
            "waitingOn": [],
            "blocking": [],
        },
        "relatedDecisionDeltaIds": [],
        "relatedForecastIds": ["forecast-beacon-renewal"],
        "relatedCommitmentIds": [],
        "lastUpdatedAt": "2026-05-16T03:00:00Z",
    },
]


_NEEDS_ATTENTION_STATUSES = {"under_pressure", "needs_review", "critical", "contested", "stale"}
_STABLE_STATUSES = {"healthy", "watch", "monitoring", "resolved"}


_DELTAS: list[dict[str, Any]] = [
    {
        "id": "delta-escalate-csr-sync",
        "userFacingType": "Proposed Change",
        "status": "proposed",
        "queueSection": "requires_authority",
        "proposal": "Escalate customer risk for Salesforce sync instability.",
        "currentState": "Watch",
        "proposedState": "Critical · Owner: VP Engineering · Re-evaluate 48h",
        "sourceThreadId": "thread-customer-reliability",
        "sourceThreadTitle": "Customer Reliability",
        "category": "Customer Risk",
        "whySurfaced": [
            "12 related signals across Support, CRM, and Email",
            "3 affected anchor customers: Beacon, Northvale, Conduit",
            "Issue has persisted for 3 days",
            "Forecast confidence rose from 61% to 78%",
        ],
        "impactChips": ["$2.04M ARR", "3 customers", "12 signals"],
        "arrAtRisk": 2_040_000,
        "affectedCustomers": [_BEACON, _NORTHVALE, _CONDUIT],
        "confidence": 0.78,
        "confidenceBasis": "limited by missing product usage telemetry",
        "evidenceTrace": _trace("delta-esc"),
        "sourceCoverage": _DEFAULT_SOURCES,
        "contextGaps": _DEFAULT_GAPS,
        "falsificationCondition": "No new sync failures for 7 business days.",
        "consequencePreview": [
            {"operation": "create", "label": "Create escalation state"},
            {"operation": "notify", "label": "Notify VP Engineering", "target": _VPE},
            {"operation": "update", "label": "Link 3 affected renewal commitments"},
            {"operation": "reevaluate", "label": "Re-evaluate renewal forecast in 48h"},
            {"operation": "archive", "label": "Archive this recommendation"},
        ],
        "severity": "critical",
        "staleLabel": "Sustained 3 days",
        "createdAt": "2026-05-16T09:22:00Z",
        "updatedAt": "2026-05-16T09:22:00Z",
    },
    {
        "id": "delta-assign-pricing-owner",
        "userFacingType": "Proposed Change",
        "status": "proposed",
        "queueSection": "requires_authority",
        "proposal": "Assign an owner to the data warehouse pricing decision.",
        "currentState": "Unowned · stalled 6 days",
        "proposedState": "Owner: CFO · Decision due in 5 business days",
        "sourceThreadId": "thread-strategic-decisions",
        "sourceThreadTitle": "Strategic Decisions",
        "category": "Decision",
        "whySurfaced": [
            "Decision is 6 days unowned",
            "Blocks Beacon expansion timeline",
            "Verbal pricing direction not captured in sources",
        ],
        "impactChips": ["2 commitments blocked", "1 expansion blocked"],
        "confidence": 0.66,
        "confidenceBasis": "based on calendar + Slack mentions only",
        "evidenceTrace": _trace("delta-pricing"),
        "sourceCoverage": _DEFAULT_SOURCES,
        "contextGaps": [
            {
                "id": "gap-pricing-1",
                "kind": "verbal_decision_unrecorded",
                "text": "Pricing was discussed verbally Tuesday but not captured in connected sources.",
                "suggestedAction": "add_context",
            }
        ],
        "falsificationCondition": "Decision is resolved or owner publicly assigned.",
        "consequencePreview": [
            {"operation": "update", "label": "Assign owner: CFO"},
            {"operation": "notify", "label": "Notify CFO + dependent owners"},
            {"operation": "reevaluate", "label": "Re-evaluate dependent commitments in 5 days"},
            {"operation": "archive", "label": "Archive this recommendation"},
        ],
        "severity": "high",
        "staleLabel": "Stalled 6 days",
        "createdAt": "2026-05-16T06:14:00Z",
        "updatedAt": "2026-05-16T06:14:00Z",
    },
    {
        "id": "delta-pause-commitments",
        "userFacingType": "Proposed Change",
        "status": "proposed",
        "queueSection": "delegatable",
        "proposal": "Pause net-new platform commitments until utilization drops below 85%.",
        "currentState": "Accepting new commitments",
        "proposedState": "Paused · re-open when utilization ≤ 85%",
        "sourceThreadId": "thread-engineering-capacity",
        "sourceThreadTitle": "Engineering Capacity",
        "category": "Capacity",
        "whySurfaced": [
            "Forecast: utilization will exceed 90% by May 18",
            "2 senior engineers focused on hiring this sprint",
            "1 sprint slip in last 2 weeks",
        ],
        "impactChips": ["2 commitments at risk", "1 sprint slipped"],
        "confidence": 0.74,
        "evidenceTrace": _trace("delta-pause"),
        "sourceCoverage": _DEFAULT_SOURCES,
        "contextGaps": [
            {
                "id": "gap-pause-1",
                "kind": "limited_telemetry",
                "text": "Sprint velocity covers only Platform team in Linear.",
                "suggestedAction": "connect_source",
            }
        ],
        "falsificationCondition": "Utilization drops below 85% for 2 sprints in a row.",
        "consequencePreview": [
            {"operation": "update", "label": "Mark Platform Engineering: paused for net-new"},
            {"operation": "notify", "label": "Notify CRO + Product", "target": _GTM},
            {"operation": "reevaluate", "label": "Re-evaluate weekly"},
        ],
        "severity": "medium",
        "staleLabel": "Updated today",
        "createdAt": "2026-05-16T07:48:00Z",
        "updatedAt": "2026-05-16T07:48:00Z",
    },
    {
        "id": "delta-confirm-beacon-severity",
        "userFacingType": "Proposed Change",
        "status": "proposed",
        "queueSection": "needs_context",
        "proposal": "Confirm severity for Beacon escalation.",
        "currentState": "Inferred from support tickets",
        "proposedState": "Confirmed by account owner",
        "sourceThreadId": "thread-customer-reliability",
        "sourceThreadTitle": "Customer Reliability",
        "category": "Context",
        "whySurfaced": [
            "Account owner has not confirmed severity",
            "Conflicting signal: account exec last note was positive",
        ],
        "impactChips": ["1 forecast pending"],
        "confidence": 0.62,
        "confidenceBasis": "mixed evidence — support vs. account owner",
        "evidenceTrace": {
            **_trace("delta-confirm"),
            "contested": True,
            "contestationNote": "Account owner says partial resolution; support says ongoing.",
        },
        "sourceCoverage": _DEFAULT_SOURCES,
        "contextGaps": [
            {
                "id": "gap-confirm-1",
                "kind": "owner_unconfirmed",
                "text": "Account owner has not confirmed severity.",
                "suggestedAction": "ask_owner",
                "target": _CS,
            }
        ],
        "consequencePreview": [
            {"operation": "update", "label": "Attach owner-confirmed severity to the thread"},
            {"operation": "reevaluate", "label": "Re-evaluate renewal forecast"},
        ],
        "severity": "medium",
        "createdAt": "2026-05-16T08:01:00Z",
        "updatedAt": "2026-05-16T08:01:00Z",
    },
]


_FORECASTS: list[dict[str, Any]] = [
    {
        "id": "forecast-beacon-renewal",
        "statement": "Beacon renewal risk is likely to increase if Salesforce sync failures persist.",
        "domain": "customer",
        "status": "intervention_available",
        "confidence": 0.78,
        "confidencePrevious": 0.61,
        "resolutionDate": "2026-05-17",
        "leadingIndicators": [
            {"label": "Support tickets rising", "movement": "rising", "detail": "3 tickets in 72h"},
            {"label": "Renewal-thread sentiment", "movement": "falling", "detail": "Negative shift in last 48h"},
            {"label": "Owner gap", "detail": "Account owner has not confirmed severity"},
        ],
        "evidenceTrace": _trace("fc-beacon"),
        "sourceCoverage": _DEFAULT_SOURCES,
        "contextGaps": _DEFAULT_GAPS,
        "falsificationCondition": "No new sync failures for 7 business days.",
        "relatedThreadId": "thread-customer-reliability",
        "relatedThreadTitle": "Customer Reliability",
        "relatedDeltaId": "delta-escalate-csr-sync",
        "interventionLabel": "Escalate customer risk",
        "severityHint": "review",
    },
    {
        "id": "forecast-eng-utilization",
        "statement": "Platform engineering utilization will exceed 90% by May 18.",
        "domain": "capacity",
        "status": "active",
        "confidence": 0.74,
        "confidencePrevious": 0.66,
        "resolutionDate": "2026-05-18",
        "leadingIndicators": [
            {"label": "Active commitments", "movement": "rising"},
            {"label": "Senior eng on hiring", "detail": "2 of 6"},
            {"label": "Sprint slip", "detail": "2 stories carried over"},
        ],
        "evidenceTrace": _trace("fc-eng"),
        "sourceCoverage": _DEFAULT_SOURCES,
        "contextGaps": [
            {
                "id": "gap-eng-fc",
                "kind": "limited_telemetry",
                "text": "Velocity data only from Platform team.",
                "suggestedAction": "connect_source",
            }
        ],
        "falsificationCondition": "Utilization drops below 85% for 2 sprints in a row.",
        "relatedThreadId": "thread-engineering-capacity",
        "relatedThreadTitle": "Engineering Capacity",
        "relatedDeltaId": "delta-pause-commitments",
        "interventionLabel": "Pause new commitments",
        "severityHint": "forecast",
    },
    {
        "id": "forecast-q3-pipeline",
        "statement": "Q3 enterprise pipeline coverage will hold above 3.0x.",
        "domain": "revenue",
        "status": "active",
        "confidence": 0.82,
        "confidencePrevious": 0.78,
        "resolutionDate": "2026-07-31",
        "leadingIndicators": [
            {"label": "Design partners signed", "movement": "rising", "detail": "2 new"},
            {"label": "Top-of-funnel quality", "movement": "steady"},
        ],
        "evidenceTrace": _trace("fc-pipeline"),
        "sourceCoverage": _DEFAULT_SOURCES,
        "contextGaps": [],
        "falsificationCondition": "Coverage drops below 2.5x for 2 weeks.",
        "relatedThreadId": "thread-enterprise-gtm",
        "relatedThreadTitle": "Enterprise GTM",
        "severityHint": "forecast",
    },
]


_LEDGER_EVENTS: list[dict[str, Any]] = [
    {
        "id": "ledger-1",
        "occurredAt": "2026-05-16T09:22:00Z",
        "kind": "thread_status_changed",
        "category": "model_update",
        "summary": "Customer Reliability moved Watch → Under Pressure.",
        "body": "Three anchor customers showed sync failures; the model upgraded the thread status and surfaced a Decision Delta in Today.",
        "actor": {"id": "actor-fyralis", "type": "actor", "label": "Fyralis"},
        "before": "Watch",
        "after": "Under Pressure",
        "relatedRefs": [_BEACON, _NORTHVALE, _CONDUIT],
        "affectedThreadId": "thread-customer-reliability",
        "affectedDeltaId": "delta-escalate-csr-sync",
        "actionsTaken": ["Surfaced Decision Delta", "Notified subscribers"],
        "severity": "review",
    },
    {
        "id": "ledger-2",
        "occurredAt": "2026-05-16T09:18:00Z",
        "kind": "forecast_created",
        "category": "forecast",
        "summary": "Forecast created: Beacon renewal risk likely to increase.",
        "actor": {"id": "actor-fyralis", "type": "actor", "label": "Fyralis"},
        "relatedRefs": [_BEACON],
        "affectedForecastId": "forecast-beacon-renewal",
        "affectedThreadId": "thread-customer-reliability",
        "severity": "forecast",
    },
    {
        "id": "ledger-3",
        "occurredAt": "2026-05-16T08:54:00Z",
        "kind": "commitment_blocked",
        "category": "commitment_state",
        "summary": "Commitment flagged: CRM reliability constrained by capacity.",
        "actor": {"id": "actor-fyralis", "type": "actor", "label": "Fyralis"},
        "relatedRefs": [_VPE],
        "affectedCommitmentId": "commit-sync-stability",
        "affectedThreadId": "thread-customer-reliability",
        "severity": "authority",
    },
    {
        "id": "ledger-4",
        "occurredAt": "2026-05-15T17:30:00Z",
        "kind": "decision_delta_accepted",
        "category": "decision_action",
        "summary": "Diana accepted: Add 2 contractors for hiring backfill.",
        "actor": _CEO,
        "relatedRefs": [_VPE],
        "actionsTaken": ["Created contractor commitments", "Notified VP Engineering"],
        "severity": "authority",
    },
    {
        "id": "ledger-5",
        "occurredAt": "2026-05-15T14:02:00Z",
        "kind": "decision_delta_contested",
        "category": "contestation",
        "summary": "Priya contested: Beacon severity is partial, not full.",
        "actor": _CS,
        "before": "Critical",
        "after": "Watch",
        "relatedRefs": [_BEACON],
        "affectedThreadId": "thread-customer-reliability",
        "severity": "review",
    },
    {
        "id": "ledger-6",
        "occurredAt": "2026-05-15T09:10:00Z",
        "kind": "forecast_resolved",
        "category": "forecast",
        "summary": "Forecast resolved true: Engineering utilization exceeded 90% by May 15.",
        "actor": {"id": "actor-fyralis", "type": "actor", "label": "Fyralis"},
        "outcome": "true",
        "calibrationImpact": 0.03,
        "relatedRefs": [_VPE, _PLATFORM],
        "affectedThreadId": "thread-engineering-capacity",
        "severity": "trust",
    },
    {
        "id": "ledger-7",
        "occurredAt": "2026-05-15T08:40:00Z",
        "kind": "observation_ingested",
        "category": "observation",
        "summary": "12 Salesforce sync failure observations ingested.",
        "actor": {"id": "src-zendesk", "type": "source", "label": "Zendesk"},
        "relatedRefs": [_BEACON, _NORTHVALE, _CONDUIT],
        "severity": "info",
    },
]


_RECENT_CHANGES: list[dict[str, Any]] = [
    {
        "id": "rmc-1",
        "occurredAt": "2026-05-16T09:22:00Z",
        "summary": "Customer-risk state updated: Watch → Critical",
        "threadId": "thread-customer-reliability",
        "kind": "state_change",
    },
    {
        "id": "rmc-2",
        "occurredAt": "2026-05-16T09:18:00Z",
        "summary": "Forecast created: Beacon renewal risk likely to increase",
        "threadId": "thread-customer-reliability",
        "kind": "forecast_created",
    },
    {
        "id": "rmc-3",
        "occurredAt": "2026-05-16T08:54:00Z",
        "summary": "Commitment flagged: CRM reliability constrained by capacity",
        "threadId": "thread-customer-reliability",
        "kind": "commitment_flagged",
    },
    {
        "id": "rmc-4",
        "occurredAt": "2026-05-16T07:48:00Z",
        "summary": "Utilization forecast: 88% → 92%",
        "threadId": "thread-engineering-capacity",
        "kind": "state_change",
    },
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _filter(items: Iterable[dict[str, Any]], **filters: Any) -> list[dict[str, Any]]:
    out = []
    for it in items:
        ok = True
        for k, v in filters.items():
            if v is None:
                continue
            if it.get(k) != v:
                ok = False
                break
        if ok:
            out.append(it)
    return out


# ---------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------


def build_spec_router() -> APIRouter:
    router = APIRouter(tags=["spec"])

    # ── Operating threads ────────────────────────────────────────────

    @router.get("/v1/spec/operating_threads/")
    @router.get("/v1/spec/operating_threads")
    def list_threads(request: Request, lens: str | None = None, q: str | None = None):
        if _auth(request) is None:
            return _unauth()
        threads = list(_THREADS)
        if lens:
            # Lens filter is intentionally permissive — Company lens
            # surfaces every thread; the others narrow as the synthesis
            # layer differentiates lenses on the server. Until then we
            # treat unknown lenses as "no-op" so the UI still has rows.
            pass
        if q:
            ql = q.lower()
            threads = [t for t in threads if ql in (t.get("title", "") + " " + t.get("currentReading", "")).lower()]
        groups = [
            {
                "id": "needs-attention",
                "label": "Needs attention",
                "threads": [t for t in threads if t["status"] in _NEEDS_ATTENTION_STATUSES],
            },
            {
                "id": "stable",
                "label": "Stable / watching",
                "threads": [t for t in threads if t["status"] in _STABLE_STATUSES],
            },
        ]
        return {
            "groups": groups,
            "total": len(threads),
            "compressionSentence": f"Fyralis has condensed 148 active Nodes into {len(threads)} operating threads.",
            "statusCounters": {
                "changedToday": sum(t["semanticMass"]["changedToday"] for t in threads),
                "contested": sum(1 for t in threads if t["status"] == "contested"),
                "blockedCommitments": sum(t["semanticMass"]["blockedCommitments"] for t in threads),
                "arrAtRisk": sum(t["semanticMass"].get("arrAtRisk") or 0 for t in threads),
            },
            "lastUpdatedAt": _now_iso(),
        }

    @router.get("/v1/spec/operating_threads/recent_changes")
    def recent_changes(request: Request):
        if _auth(request) is None:
            return _unauth()
        return {"items": _RECENT_CHANGES}

    # NOTE: must be declared AFTER /recent_changes so the literal route
    # wins over the parameterised one.
    @router.get("/v1/spec/operating_threads/{thread_id}")
    def get_thread(request: Request, thread_id: str):
        if _auth(request) is None:
            return _unauth()
        for t in _THREADS:
            if t["id"] == thread_id:
                return t
        return JSONResponse({"error": "not_found"}, status_code=404)

    # ── Decision deltas (spec view) ──────────────────────────────────

    @router.get("/v1/spec/decision_deltas/")
    @router.get("/v1/spec/decision_deltas")
    def list_deltas(request: Request):
        if _auth(request) is None:
            return _unauth()
        deltas = list(_DELTAS)
        return {
            "deltas": deltas,
            "sinceLastReview": {
                "proposedChanges": sum(1 for d in deltas if d["queueSection"] == "requires_authority"),
                "delegatable": sum(1 for d in deltas if d["queueSection"] == "delegatable"),
                "contested": sum(1 for d in deltas if d.get("status") == "contested"),
                "modelUpdates": 12,
                "signalsAbsorbed": 98,
                "arrExposed": 2_040_000,
            },
            "lastUpdatedAt": _now_iso(),
        }

    @router.get("/v1/spec/decision_deltas/{delta_id}")
    def get_delta(request: Request, delta_id: str):
        if _auth(request) is None:
            return _unauth()
        for d in _DELTAS:
            if d["id"] == delta_id:
                return d
        return JSONResponse({"error": "not_found"}, status_code=404)

    # ── Decision delta mutations (spec) ──────────────────────────────
    #
    # The spec view uses string IDs (e.g. "delta-escalate-csr-sync")
    # rather than UUIDs, so it can't share the existing
    # `/v1/decision_deltas/{uuid}/{op}` mutation routes — those would
    # 400 on non-UUID ids. We provide spec-namespaced echo routes that
    # return the in-memory delta as if the mutation had landed. The UI
    # treats the mutation as optimistic and uses these endpoints purely
    # to confirm the round-trip succeeded; when the synthesis layer
    # bridges spec ids to substrate UUIDs, this echo will be replaced
    # with a real mutation.

    @router.post("/v1/spec/decision_deltas/{delta_id}/accept")
    @router.post("/v1/spec/decision_deltas/{delta_id}/delegate")
    @router.post("/v1/spec/decision_deltas/{delta_id}/contest")
    @router.post("/v1/spec/decision_deltas/{delta_id}/snooze")
    @router.post("/v1/spec/decision_deltas/{delta_id}/add_context")
    async def mutate_delta(request: Request, delta_id: str):
        if _auth(request) is None:
            return _unauth()
        delta = next((d for d in _DELTAS if d["id"] == delta_id), None)
        if delta is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        # Echo the delta as if mutation applied; UI does optimistic update.
        return {"delta": delta}

    # ── Forecasts (spec) ─────────────────────────────────────────────

    @router.get("/v1/spec/forecasts/")
    @router.get("/v1/spec/forecasts")
    def list_forecasts(request: Request):
        if _auth(request) is None:
            return _unauth()
        return {"items": list(_FORECASTS)}

    @router.get("/v1/spec/forecasts/{forecast_id}")
    def get_forecast(request: Request, forecast_id: str):
        if _auth(request) is None:
            return _unauth()
        for f in _FORECASTS:
            if f["id"] == forecast_id:
                return f
        return JSONResponse({"error": "not_found"}, status_code=404)

    # ── Unified ledger ───────────────────────────────────────────────

    @router.get("/v1/spec/ledger_events/")
    @router.get("/v1/spec/ledger_events")
    def list_ledger(
        request: Request,
        kinds: str | None = None,
        categories: str | None = None,
        thread_id: str | None = None,
        q: str | None = None,
        limit: int | None = None,
        high_impact_only: int | None = None,
    ):
        if _auth(request) is None:
            return _unauth()
        events = list(_LEDGER_EVENTS)
        if kinds:
            wanted = set(kinds.split(","))
            events = [e for e in events if e["kind"] in wanted]
        if categories:
            wanted = set(categories.split(","))
            events = [e for e in events if e["category"] in wanted]
        if thread_id:
            events = [e for e in events if e.get("affectedThreadId") == thread_id]
        if q:
            ql = q.lower()
            events = [
                e
                for e in events
                if ql in (e["summary"] + " " + (e.get("body") or "") + " " + (e.get("actor", {}).get("label") or "")).lower()
            ]
        if limit:
            events = events[: int(limit)]
        return {
            "events": events,
            "total": len(events),
            "rangeLabel": "May 12 – May 16",
        }

    return router


def register_spec_routes(app: FastAPI) -> None:
    """Attach the spec-aligned product routes to `app`."""
    app.include_router(build_spec_router())
