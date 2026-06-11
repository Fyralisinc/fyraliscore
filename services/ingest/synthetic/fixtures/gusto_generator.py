"""Gusto entity fixture generator (payroll/HR, IN-FIN).

`make_gusto(company_uuid=..., entities=[...], rows_per_entity=N)` produces a
deterministic per-entity-kind fixture shaped to feed `MockGustoClient`. The
mock paginates each entity list by the REAL Gusto offset cursor (`page`/`per`
query params, `X-Total-Count`-style totals) and the fetcher drives one
`gusto_entity` shard per entity kind.

Rows mirror the REAL wire shapes (VERIFIED against docs.gusto.com — bare-array
list endpoints, snake_case fields, dollar amounts as decimal STRINGS):

  - `employee`: `uuid`, `version` (the concurrency token that discriminates
    the external_id), `first_name`/`last_name`, `work_email`, `department`,
    `terminated`/`onboarded`, `jobs[]` with `hire_date`/`title`/`rate`(string)/
    `payment_unit`.
  - `payroll`: `payroll_uuid` (+ `uuid` twin), `check_date`, `processed` (+
    `processed_date`), `off_cycle`, `pay_period{start_date,end_date,
    pay_schedule_uuid}`, `totals{gross_pay,net_pay,...}` as decimal strings,
    `employee_compensations` (left empty — list endpoints elide them unless
    included).

Even payroll rows are processed (-> state_change "processed"); odd rows stay
unprocessed. Employees are active (state "signal"); bump `version` (and
`terminated`) on a row to exercise external_id versioning.

Determinism: payroll check_dates are spaced one day apart, oldest first,
anchored at `base_iso`; ids/amounts are derived from a stable SHA-256 digest of
(company_uuid, entity_type, idx). Re-running with the same args yields
byte-identical output.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The two entity kinds the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = ("employee", "payroll")

_FIRST_NAMES = ("Ava", "Noah", "Mia", "Liam", "Zoe", "Eli", "Ivy", "Max")
_LAST_NAMES = ("Reyes", "Chen", "Okafor", "Silva", "Novak", "Hart", "Kim",
               "Patel")
_DEPARTMENTS = ("Engineering", "Sales", "Operations", "Finance")
_TITLES = ("Software Engineer", "Account Executive", "Ops Manager",
           "Financial Analyst")


def make_gusto(
    *,
    company_uuid: str = "8b342a55-907e-4ba8-a95d-d29fbf95d6e1",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a Gusto company fixture.

    Args:
      company_uuid: Gusto company UUID (stamped into rows + returned at top
        level; every API call is scoped to it).
      entities: Entity kinds to generate; defaults to ("employee", "payroll").
      rows_per_entity: Number of rows generated for EACH entity kind.
      base_iso: Anchor for the (deterministic, 1-day-spaced) payroll
        check_dates / employee hire_dates. Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-request `per` cap (so callers can drive
        multi-page pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockGustoClient(fixture=...)`:
        {
          "company_uuid": "...",
          "page_size": 100,
          "entities": {
            "employee": [ {<real-shaped employee>}, ... ],  # ordered, stable
            "payroll":  [ {<real-shaped payroll>}, ... ],   # oldest-first
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        if entity_type == "payroll":
            rows = [_payroll(company_uuid, idx, base)
                    for idx in range(rows_per_entity)]
        else:  # employee (and any future people-shaped kind)
            rows = [_employee(company_uuid, idx, base)
                    for idx in range(rows_per_entity)]
        entities_out[entity_type] = rows

    return {
        "company_uuid": company_uuid,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _employee(company_uuid: str, idx: int, base: datetime) -> dict[str, Any]:
    seed = _digest(company_uuid, "employee", idx)
    uuid = _uuid_from(seed)
    first = _FIRST_NAMES[idx % len(_FIRST_NAMES)]
    last = _LAST_NAMES[(idx + int(seed[:2], 16)) % len(_LAST_NAMES)]
    # Annual salary as a decimal STRING in dollars (real wire shape).
    rate = f"{60000 + (int(seed[:6], 16) % 90000)}.00"
    hire_date = (base - timedelta(days=200 + idx * 30)).date().isoformat()

    return {
        "uuid": uuid,
        # The concurrency token; bump it to exercise external_id versioning.
        "version": seed[8:16],
        "first_name": first,
        "last_name": last,
        "middle_initial": None,
        "email": f"{first.lower()}.{last.lower()}@personal.example",
        "work_email": f"{first.lower()}.{last.lower()}@acme.example",
        "company_uuid": company_uuid,
        "manager_uuid": None,
        "department": _DEPARTMENTS[idx % len(_DEPARTMENTS)],
        "terminated": False,
        "onboarded": True,
        "current_employment_status": "full_time",
        "employee_code": f"E{1000 + idx}",
        "payment_method": "Direct Deposit",
        "has_ssn": True,
        "phone": None,
        "preferred_first_name": None,
        "jobs": [{
            "uuid": _uuid_from(_digest(uuid, "job", 0)),
            "primary": True,
            "title": _TITLES[idx % len(_TITLES)],
            "hire_date": hire_date,
            "rate": rate,
            "payment_unit": "Year",
        }],
        "eligible_paid_time_off": [],
        "terminations": [],
        "garnishments": [],
    }


def _payroll(company_uuid: str, idx: int, base: datetime) -> dict[str, Any]:
    seed = _digest(company_uuid, "payroll", idx)
    payroll_uuid = _uuid_from(seed)
    # check_dates spaced 1 day apart, oldest first.
    check = (base + timedelta(days=idx)).date()
    period_start = (base + timedelta(days=idx) - timedelta(days=14)).date()
    period_end = (base + timedelta(days=idx) - timedelta(days=1)).date()
    # Even rows are processed (state_change "processed"); odd rows pending.
    processed = idx % 2 == 0
    gross = 10000.0 + (int(seed[:6], 16) % 900_000) / 100.0
    employee_taxes = round(gross * 0.18, 2)
    employer_taxes = round(gross * 0.08, 2)
    net = round(gross - employee_taxes, 2)

    return {
        "payroll_uuid": payroll_uuid,
        "uuid": payroll_uuid,
        "company_uuid": company_uuid,
        "check_date": check.isoformat(),
        "processed": processed,
        "processed_date": check.isoformat() if processed else None,
        "calculated_at": f"{period_end.isoformat()}T12:00:00Z",
        "payroll_deadline": f"{period_end.isoformat()}T17:00:00Z",
        "off_cycle": False,
        "external": False,
        "pay_period": {
            "start_date": period_start.isoformat(),
            "end_date": period_end.isoformat(),
            "pay_schedule_uuid": _uuid_from(_digest(company_uuid, "sched", 0)),
        },
        # All dollar amounts are decimal STRINGS, to the cent.
        "totals": {
            "company_debit": f"{round(gross + employer_taxes, 2):.2f}",
            "net_pay_debit": f"{net:.2f}",
            "tax_debit": f"{round(employee_taxes + employer_taxes, 2):.2f}",
            "reimbursement_debit": "0.00",
            "child_support_debit": "0.00",
            "reimbursements": "0.00",
            "net_pay": f"{net:.2f}",
            "gross_pay": f"{gross:.2f}",
            "employee_taxes": f"{employee_taxes:.2f}",
            "employer_taxes": f"{employer_taxes:.2f}",
            "benefits": "0.00",
        },
        # List endpoints elide per-employee compensations unless included.
        "employee_compensations": [],
    }


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_iso(value: str) -> datetime:
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


def _uuid_from(seed: str) -> str:
    """A stable UUID-shaped string from a hex digest."""
    return f"{seed[:8]}-{seed[8:12]}-{seed[12:16]}-{seed[16:20]}-{seed[20:32]}"


__all__ = ["make_gusto", "DEFAULT_ENTITIES"]
