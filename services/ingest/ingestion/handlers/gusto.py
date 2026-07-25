"""services/ingest/ingestion/handlers/gusto.py — Gusto entity handler (payroll/HR).

ONE channel `gusto:object` (mirrors jira:issue's one-channel/many-record-types
shape). The handler is a pure function (no DB / network) and branches on the
input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"employee", "payroll"} (set by the fetcher); `entity` carries the raw
    Gusto object exactly as the `/v1/companies/{company_uuid}/...` list
    endpoints return it (VERIFIED against docs.gusto.com — bare-array
    responses, snake_case fields, dollar amounts as decimal STRINGS).
  - LIVE WEBHOOK: a real Gusto thin notification (flat snake_case, no entity
    body); the handler emits a thin change observation keyed the same way so
    the poll re-fetch's full-bodied twin lands distinct only when the entity
    actually changed.

Signal mapping (the reasoning value):
  - payroll with processed == true        -> kind="state_change"
    (the cash event: a payroll run debited / pay hit employees)
  - employee with terminated == true      -> kind="state_change"
    (headcount loss — the offboarding signal)
  - everything else (active/onboarding employees, pending payrolls)
                                          -> kind="signal"

external_id — VERSIONED (the mutable-source dedup lesson; the observations repo
dedups on (source_channel, external_id) IGNORING occurred_at):
  - employee: gusto:{company}:employee:{uuid}:{version}
      (`version` is the employee object's own concurrency token — it changes
      whenever the record changes, so a re-walk only lands changed employees)
  - payroll:  gusto:{company}:payroll:{uuid}:{processed|unprocessed}
      (a payroll flipping to processed lands as a NEW state_change)

Trust posture: Gusto is the payroll system of record -> `authoritative`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion import idempotency
from services.ingest.ingestion.handlers import (
    ObservationDraft,
)


_CHANNEL = "gusto:object"
_TRUST = "authoritative"

_RECORD_TYPES = ("employee", "payroll")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
    """Parse an ISO datetime OR bare date (Gusto dates are YYYY-MM-DD)."""
    if not isinstance(value, str) or not value:
        return None
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif len(s) >= 5 and s[-5] in "+-" and s[-3] != ":" and "T" in s:
        s = s[:-2] + ":" + s[-2:]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _num(value: Any) -> float | None:
    """Gusto money is a decimal STRING in dollars (e.g. '1234.56')."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_money(amount: Any) -> str:
    val = _num(amount)
    return f"${val:,.2f}" if val is not None else str(amount)


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _clean(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


# ---------------------------------------------------------------------
# Employee records
# ---------------------------------------------------------------------

def _employee_name(e: dict[str, Any]) -> str:
    first = e.get("preferred_first_name") or e.get("first_name") or ""
    last = e.get("last_name") or ""
    name = f"{first} {last}".strip()
    return name or str(e.get("uuid") or "?")


def _primary_job(e: dict[str, Any]) -> dict[str, Any] | None:
    jobs = e.get("jobs")
    if isinstance(jobs, list):
        for job in jobs:
            if isinstance(job, dict) and job.get("primary"):
                return job
        for job in jobs:
            if isinstance(job, dict):
                return job
    return None


def _employee_status(e: dict[str, Any]) -> tuple[str, str]:
    """(kind, status_word). Termination is the state_change (headcount loss)."""
    if e.get("terminated") is True:
        return "state_change", "terminated"
    if e.get("onboarded") is False:
        return "signal", "onboarding"
    return "signal", "active"


def _employee_occurred(e: dict[str, Any]) -> datetime:
    # Employees carry no updated-at timestamp. Best event anchor: the latest
    # termination's effective_date when terminated, else the (primary) job
    # hire_date, else ingestion time.
    if e.get("terminated") is True:
        terms = e.get("terminations")
        if isinstance(terms, list) and terms:
            last = terms[-1]
            if isinstance(last, dict):
                dt = _parse_iso(last.get("effective_date"))
                if dt is not None:
                    return dt
    job = _primary_job(e)
    if job is not None:
        dt = _parse_iso(job.get("hire_date"))
        if dt is not None:
            return dt
    return _utcnow()


def _employee_draft(
    entity: dict[str, Any], company_uuid: str,
) -> ObservationDraft:
    employee_uuid = str(entity.get("uuid") or "")
    if not company_uuid or not employee_uuid:
        raise ValidationError(
            "gusto employee missing company_uuid/uuid", channel=_CHANNEL,
        )
    # `version` is Gusto's own concurrency token — the dedup discriminator.
    version = str(entity.get("version") or "0")
    external_id = idempotency.gusto_entity(
        company_uuid, "employee", employee_uuid, version,
    )

    kind, status_word = _employee_status(entity)
    name = _employee_name(entity)
    job = _primary_job(entity) or {}
    title = job.get("title")
    department = entity.get("department")

    parts = [f"Employee {name}"]
    if title:
        parts.append(f"· {title}")
    if department:
        parts.append(f"· {department}")
    parts.append(f"· {status_word}")
    content_text = " ".join(parts)

    entities_hint: list[dict[str, Any]] = [
        {"type": "gusto_object", "id": f"employee:{employee_uuid}"},
    ]
    person_id = entity.get("work_email") or entity.get("email") or name
    if person_id:
        entities_hint.append(
            {"type": "person", "role": "employee", "id": str(person_id)},
        )

    content = _clean({
        "object_type": "employee",
        "company_uuid": company_uuid,
        "entity_id": employee_uuid,
        "version": version,
        "status": status_word,
        "first_name": entity.get("first_name"),
        "last_name": entity.get("last_name"),
        "preferred_first_name": entity.get("preferred_first_name"),
        "work_email": entity.get("work_email"),
        "department": department,
        "manager_uuid": entity.get("manager_uuid"),
        "employee_code": entity.get("employee_code"),
        "current_employment_status": entity.get("current_employment_status"),
        "onboarded": entity.get("onboarded"),
        "terminated": entity.get("terminated"),
        "payment_method": entity.get("payment_method"),
        "job_title": title,
        "hire_date": job.get("hire_date"),
        # Compensation rate is a decimal string in dollars — kept verbatim.
        "rate": job.get("rate"),
        "payment_unit": job.get("payment_unit"),
    })

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=_employee_occurred(entity),
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=f"gusto:employee:{employee_uuid}",
        external_id=external_id,
        entities_hint=entities_hint,
        raw_payload=entity,
    )


# ---------------------------------------------------------------------
# Payroll records
# ---------------------------------------------------------------------

def _payroll_totals(entity: dict[str, Any]) -> dict[str, Any]:
    totals = entity.get("totals")
    return totals if isinstance(totals, dict) else {}


def _payroll_draft(
    entity: dict[str, Any], company_uuid: str,
) -> ObservationDraft:
    payroll_uuid = str(entity.get("payroll_uuid") or entity.get("uuid") or "")
    if not company_uuid or not payroll_uuid:
        raise ValidationError(
            "gusto payroll missing company_uuid/payroll_uuid", channel=_CHANNEL,
        )
    processed = entity.get("processed") is True
    # No top-level `version` on Payroll (unlike Employee) — the processed flip
    # is the meaningful mutation, so it discriminates the external_id.
    version = "processed" if processed else "unprocessed"
    external_id = idempotency.gusto_entity(
        company_uuid, "payroll", payroll_uuid, version,
    )

    kind = "state_change" if processed else "signal"
    status_word = "processed" if processed else "pending"

    check_date = entity.get("check_date")
    occurred = (
        _parse_iso(check_date)
        or _parse_iso(entity.get("processed_date"))
        or _parse_iso(entity.get("payroll_deadline"))
        or _utcnow()
    )

    pay_period = entity.get("pay_period")
    pay_period = pay_period if isinstance(pay_period, dict) else {}
    totals = _payroll_totals(entity)
    comps = entity.get("employee_compensations")
    employee_count = len(comps) if isinstance(comps, list) else None

    parts = [f"Payroll {check_date or payroll_uuid}"]
    period_start = pay_period.get("start_date")
    period_end = pay_period.get("end_date")
    if period_start and period_end:
        parts.append(f"({period_start} – {period_end})")
    gross = totals.get("gross_pay")
    if gross is not None:
        parts.append(f"· gross {_fmt_money(gross)}")
    parts.append(f"· {status_word}")
    if entity.get("off_cycle") is True:
        parts.append("· off-cycle")
    if employee_count:
        parts.append(
            f"· {employee_count} employee{'s' if employee_count != 1 else ''}"
        )
    content_text = " ".join(parts)

    # All totals are decimal strings in dollars — kept verbatim.
    content = _clean({
        "object_type": "payroll",
        "company_uuid": company_uuid,
        "entity_id": payroll_uuid,
        "status": status_word,
        "processed": entity.get("processed"),
        "processed_date": entity.get("processed_date"),
        "check_date": check_date,
        "payroll_deadline": entity.get("payroll_deadline"),
        "off_cycle": entity.get("off_cycle"),
        "external": entity.get("external"),
        "pay_period_start": period_start,
        "pay_period_end": period_end,
        "pay_schedule_uuid": pay_period.get("pay_schedule_uuid"),
        "gross_pay": gross,
        "net_pay": totals.get("net_pay"),
        "employee_taxes": totals.get("employee_taxes"),
        "employer_taxes": totals.get("employer_taxes"),
        "company_debit": totals.get("company_debit"),
        "benefits": totals.get("benefits"),
        "reimbursements": totals.get("reimbursements"),
        "employee_count": employee_count,
    })

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=[
            {"type": "gusto_object", "id": f"payroll:{payroll_uuid}"},
        ],
        raw_payload=entity,
    )


_RECORD_BUILDERS = {
    "employee": _employee_draft,
    "payroll": _payroll_draft,
}


# ---------------------------------------------------------------------
# Thin webhook change (shared shape with the poll re-fetch)
# ---------------------------------------------------------------------

def _thin_change_draft(
    entity_kind: str, entity_id: str, company_uuid: str, *,
    operation: str | None, last_updated: str | None,
    version: str | None = None,
) -> ObservationDraft:
    """A webhook notification with no full entity body (real Gusto deliveries
    are thin — id + event_type only). Emit a thin change observation; the next
    backfill/poll re-fetch fills the full body (and dedups by the entity's own
    version if unchanged).

    `version` is the delivery's own `uuid` (a stronger key than the timestamp)
    so re-deliveries dedup and distinct events stay distinct; when absent the
    change is versioned by the timestamp."""
    if not company_uuid or not entity_id:
        raise ValidationError(
            "gusto change missing company_uuid/id", channel=_CHANNEL,
        )
    ver = version or last_updated or _utcnow().isoformat()
    external_id = idempotency.gusto_change(
        company_uuid, entity_kind, entity_id, ver,
    )
    occurred = _parse_iso(last_updated) or _utcnow()
    op = operation or "update"
    content_text = (
        f"{entity_kind.replace('_', ' ').title()} #{entity_id} {op.lower()} (live)"
    )
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "object_type": entity_kind,
            "company_uuid": company_uuid,
            "entity_id": entity_id,
            "operation": op,
            "thin_change": True,
            "last_updated": last_updated,
        },
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=[{"type": "gusto_object",
                        "id": f"{entity_kind}:{entity_id}"}],
        raw_payload=None,
    )


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

async def handle_gusto_object(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("gusto payload must be a JSON object", channel=_CHANNEL)

    # --- LIVE WEBHOOK path (REAL Gusto: flat thin notification) ---
    # Real Gusto deliveries are flat snake_case:
    #   {"uuid","event_type","resource_type":"Company","resource_uuid":<company>,
    #    "entity_type","entity_uuid","timestamp"}
    # `resource_uuid` is ALWAYS the company (resource_type is always "Company");
    # `entity_type`/`entity_uuid` name the changed resource; the body carries no
    # entity body (thin notification — the poll re-fetch fills it). Versioned by
    # the delivery's own `uuid` so re-deliveries dedup and distinct events stay
    # distinct. Verified against docs.gusto.com {webhooks,company-,employee-,
    # notification-events}.
    if payload.get("resource_type") is not None and payload.get("resource_uuid"):
        company_uuid = str(payload.get("resource_uuid") or "")
        entity_type = str(
            payload.get("entity_type") or payload.get("resource_type") or "object"
        )
        entity_uuid = str(
            payload.get("entity_uuid") or payload.get("resource_uuid") or ""
        )
        event_type = str(payload.get("event_type") or "")
        action = event_type.rsplit(".", 1)[-1] if event_type else "update"
        ts = payload.get("timestamp")
        if isinstance(ts, (int, float)) and not isinstance(ts, bool):
            last_updated_iso: str | None = datetime.fromtimestamp(
                int(ts), tz=timezone.utc
            ).isoformat()
        elif isinstance(ts, str) and ts:
            last_updated_iso = ts
        else:
            last_updated_iso = None
        return _thin_change_draft(
            entity_type.strip().lower() or "object",
            entity_uuid,
            company_uuid,
            operation=action,
            last_updated=last_updated_iso,
            version=str(payload.get("uuid") or "") or None,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        builder = _RECORD_BUILDERS.get(record_type.lower())
        if builder is None:
            raise ValidationError(
                f"unsupported gusto record_type {record_type!r} "
                f"(expected one of {_RECORD_TYPES})",
                channel=_CHANNEL,
            )
        entity = payload.get("entity")
        entity = entity if isinstance(entity, dict) else {}
        company_uuid = str(payload.get("_fyralis_company_uuid") or "")
        return builder(entity, company_uuid)

    raise ValidationError(
        "gusto payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )




__all__ = ["handle_gusto_object"]
