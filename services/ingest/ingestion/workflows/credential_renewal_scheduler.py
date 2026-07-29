"""Contract-derived scheduler for bounded credential renewal.

The scheduler deliberately owns no provider-specific branching, credentials,
or retry policy.  It derives all credential sources, cadence, and invokers
from :class:`SourceDefinition`; each source-owned invoker acquires the exact
durable renewal-job lease and performs the ProviderTransport-governed request.

Discovery is intentionally priority-ordered and split into safe steps:

* a bounded global scan of already-due durable renewal jobs runs first; it is
  independent of the sampled onboarding scan, so a retry that falls outside
  that sample can never be hidden by it;
* only when those due jobs do not fill the batch, a bounded, secret-free scan
  of completed ``source_onboarding_runs`` obtains additional exact
  ``(source, tenant, installation)`` identities;
* every candidate is rechecked against its source-owned installation table in
  a transaction with that tenant bound before it can reach an invoker.

The scheduler never reads or logs credential references.  A source invoker
rechecks the same exact active installation after claiming its durable lease,
which closes the disable/reinstall race between discovery and provider I/O.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import logging
import socket
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from lib.shared.tenant_context import bind_tenant
from services.ingest.integrations.bounded_renewal import (
    RenewalInvocation,
    RenewalOutcome,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.source_contract.models import SourceDefinition
from services.ingest.source_contract.runtime import resolve_renewal_invoker
from services.ingest.ingestion.workflows.runtime import LongRunningService


log = logging.getLogger(__name__)


WORKER_COMPONENT_ID = "credential_renewal"
WORKFLOW_KIND = "credential_renewal_scheduler"
DEFAULT_BATCH_SIZE = 64
DEFAULT_CANDIDATE_SCAN_LIMIT = 512
DEFAULT_MAX_CONCURRENCY = 8
DEFAULT_LEASE_TIMEOUT_SECONDS = 60.0


class CredentialRenewalSchedulerError(RuntimeError):
    """The contract-derived scheduler cannot safely continue one tick."""


class CredentialRenewalContractError(CredentialRenewalSchedulerError):
    """A credential-renewal source is missing its required runtime contract."""


@dataclass(frozen=True, slots=True)
class CredentialRenewalCandidate:
    """A non-secret exact installation target selected for one tick."""

    source_id: str
    tenant_id: UUID
    installation_id: UUID
    last_claimed_at: dt.datetime | None = None
    durable_due: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ValueError("source_id must be non-empty")
        for field_name in ("tenant_id", "installation_id"):
            value = getattr(self, field_name)
            if not isinstance(value, UUID):
                try:
                    value = UUID(str(value))
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{field_name} must be an exact UUID") from exc
                object.__setattr__(self, field_name, value)
        if self.last_claimed_at is not None:
            if (
                self.last_claimed_at.tzinfo is None
                or self.last_claimed_at.utcoffset() is None
            ):
                raise ValueError("last_claimed_at must be timezone-aware")
            object.__setattr__(
                self,
                "last_claimed_at",
                self.last_claimed_at.astimezone(dt.timezone.utc),
            )
        if not isinstance(self.durable_due, bool):
            raise TypeError("durable_due must be a boolean")


@dataclass(frozen=True, slots=True)
class CredentialRenewalSchedulerConfig:
    """Bounded work controls for the shared credential-renewal process."""

    batch_size: int = DEFAULT_BATCH_SIZE
    candidate_scan_limit: int = DEFAULT_CANDIDATE_SCAN_LIMIT
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    lease_timeout_seconds: float = DEFAULT_LEASE_TIMEOUT_SECONDS
    instance_name: str = WORKFLOW_KIND

    def __post_init__(self) -> None:
        for field_name in (
            "batch_size",
            "candidate_scan_limit",
            "max_concurrency",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")
        if self.candidate_scan_limit < self.batch_size:
            raise ValueError("candidate_scan_limit must be at least batch_size")
        if (
            isinstance(self.lease_timeout_seconds, bool)
            or not isinstance(self.lease_timeout_seconds, (int, float))
            or self.lease_timeout_seconds <= 0
        ):
            raise ValueError("lease_timeout_seconds must be positive")
        if not isinstance(self.instance_name, str) or not self.instance_name.strip():
            raise ValueError("instance_name must be non-empty")


@dataclass(frozen=True, slots=True)
class CredentialRenewalTickResult:
    """Secret-free operational result for one bounded scheduler tick."""

    discovered_count: int
    selected_count: int
    outcomes: tuple[RenewalOutcome, ...]
    invocation_error_count: int


CredentialRenewalInvoker = Callable[[RenewalInvocation], Awaitable[RenewalOutcome]]
CredentialRenewalCandidateLoader = Callable[
    [Any, tuple[SourceDefinition, ...], int, int, dt.datetime],
    Awaitable[Sequence[CredentialRenewalCandidate]],
]
Clock = Callable[[], dt.datetime]


# ``source_renewal_jobs`` has strict RLS.  Tenant enumeration uses only the
# pre-existing non-secret onboarding metadata; each durable-job read below is
# then made with that exact tenant bound.  This avoids requiring a worker role
# that can bypass RLS while still finding due work outside an onboarding sample.
_DISCOVER_DUE_RENEWAL_TENANTS_SQL = """
SELECT DISTINCT tenant_id
  FROM source_onboarding_runs
 WHERE source = ANY($1::text[])
   AND status = 'completed'
   AND installation_row_id IS NOT NULL
 ORDER BY tenant_id
"""

_LOAD_DUE_CREDENTIAL_RENEWAL_JOBS_FOR_TENANT_SQL = """
WITH eligible AS MATERIALIZED (
    SELECT source_id,
           tenant_id,
           installation_id,
           last_claimed_at
      FROM source_renewal_jobs
     WHERE tenant_id = $1
       AND source_id = ANY($2::text[])
       AND target_key = 'installation'
       AND (
            (
                state IN ('pending', 'retry_scheduled')
                AND next_attempt_at <= $3
            )
            OR (
                state = 'leased'
                AND lease_expires_at <= $3
            )
       )
)
SELECT source_id,
       tenant_id,
       installation_id,
       last_claimed_at
  FROM eligible
 ORDER BY last_claimed_at NULLS FIRST,
          source_id,
          installation_id
 LIMIT $4
"""


_DISCOVER_ONBOARDED_INSTALLATIONS_SQL = """
WITH installed AS MATERIALIZED (
    SELECT source, tenant_id, installation_row_id
      FROM source_onboarding_runs
     WHERE source = ANY($1::text[])
       AND status = 'completed'
       AND installation_row_id IS NOT NULL
     GROUP BY source, tenant_id, installation_row_id
)
SELECT source, tenant_id, installation_row_id
  FROM installed
 ORDER BY md5(
              source || ':' || tenant_id::text || ':'
              || installation_row_id::text || ':' || $2::text
          ),
          source,
          tenant_id,
          installation_row_id
 LIMIT $3
"""

_LOAD_ACTIVE_INSTALLATIONS_SQL = """
SELECT id
  FROM {installation_table}
 WHERE tenant_id = $1
   AND disabled_at IS NULL
   AND id = ANY($2::uuid[])
"""

_LOAD_CLAIM_TIMES_SQL = """
SELECT source_id, installation_id, last_claimed_at
  FROM source_renewal_jobs
 WHERE tenant_id = $1
   AND target_key = 'installation'
   AND source_id = ANY($2::text[])
"""


def credential_renewal_sources(
    definitions: Iterable[SourceDefinition] | None = None,
) -> tuple[SourceDefinition, ...]:
    """Return the credential-renewal subset of the canonical source catalog.

    This is the only source selection in this module.  It intentionally does
    not carry a mutable registry or a literal source list: adding a future
    credential source changes the scheduler only through its contract entry.
    """

    selected = tuple(
        sorted(
            (
                source
                for source in (
                    SOURCE_DEFINITIONS if definitions is None else definitions
                )
                if source.renewal is not None
                and source.renewal.kind == "credential"
            ),
            key=lambda source: source.source_id,
        )
    )
    if not selected:
        raise CredentialRenewalContractError(
            "source catalog declares no credential renewal sources",
        )
    for source in selected:
        _validate_credential_renewal_source(source)
    return selected


def credential_renewal_tick_interval_seconds(
    definitions: Iterable[SourceDefinition] | None = None,
) -> float:
    """Derive the shared process wake cadence from the source contracts."""

    return min(
        float(source.renewal.cadence_seconds)
        for source in credential_renewal_sources(definitions)
        if source.renewal is not None
    )


def _validate_credential_renewal_source(source: SourceDefinition) -> None:
    renewal = source.renewal
    if renewal is None or renewal.kind != "credential":
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} is not a credential-renewal source",
        )
    if source.credential_refresh is None:
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} lacks credential_refresh",
        )
    if renewal.lease_scope != "installation":
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} must use an installation lease",
        )
    try:
        worker = source.live_runtime.worker(WORKER_COMPONENT_ID)
    except KeyError as exc:
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} lacks {WORKER_COMPONENT_ID!r} worker",
        ) from exc
    if worker.role != "credential_renewal":
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} has invalid credential worker role",
        )
    if worker.transport is not None or worker.lease_scope != "installation":
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} credential worker must be "
            "supplemental and installation-scoped",
        )
    if worker.cadence_seconds != renewal.cadence_seconds:
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} credential worker cadence diverges "
            "from its renewal contract",
        )


def fair_credential_renewal_candidates(
    candidates: Iterable[CredentialRenewalCandidate],
    *,
    limit: int,
) -> tuple[CredentialRenewalCandidate, ...]:
    """Select a bounded deterministic, priority/fair renewal batch.

    Already-due durable work always gets the first portion of a batch.  Within
    each priority portion, ``last_claimed_at`` is the ordering signal:
    installations that have never won a lease are served first; otherwise the
    least recently claimed installation leads.  One candidate per tenant is
    emitted per round, so a busy tenant cannot monopolize the batch.
    """

    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")

    identities: set[tuple[str, UUID, UUID]] = set()
    due_candidates: list[CredentialRenewalCandidate] = []
    sampled_candidates: list[CredentialRenewalCandidate] = []
    for candidate in candidates:
        if not isinstance(candidate, CredentialRenewalCandidate):
            raise TypeError("candidates must be CredentialRenewalCandidate values")
        identity = (
            candidate.source_id,
            candidate.tenant_id,
            candidate.installation_id,
        )
        if identity in identities:
            raise CredentialRenewalSchedulerError(
                "credential renewal candidate list contains an exact duplicate",
            )
        identities.add(identity)
        if candidate.durable_due:
            due_candidates.append(candidate)
        else:
            sampled_candidates.append(candidate)

    selected = list(_fair_candidates_within_priority(due_candidates, limit=limit))
    remaining = limit - len(selected)
    if remaining:
        selected.extend(
            _fair_candidates_within_priority(
                sampled_candidates,
                limit=remaining,
            )
        )
    return tuple(selected)


def _fair_candidates_within_priority(
    candidates: Iterable[CredentialRenewalCandidate],
    *,
    limit: int,
) -> tuple[CredentialRenewalCandidate, ...]:
    """Round-robin tenants after the durable-priority split."""

    grouped: dict[UUID, list[CredentialRenewalCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate.tenant_id].append(candidate)

    queues: dict[UUID, deque[CredentialRenewalCandidate]] = {
        tenant_id: deque(sorted(items, key=_candidate_priority))
        for tenant_id, items in grouped.items()
    }
    tenant_order = tuple(
        sorted(
            queues,
            key=lambda tenant_id: (
                _candidate_priority(queues[tenant_id][0]),
                str(tenant_id),
            ),
        )
    )

    selected: list[CredentialRenewalCandidate] = []
    while len(selected) < limit and any(queues.values()):
        for tenant_id in tenant_order:
            queue = queues[tenant_id]
            if not queue:
                continue
            selected.append(queue.popleft())
            if len(selected) == limit:
                break
    return tuple(selected)


def _candidate_priority(
    candidate: CredentialRenewalCandidate,
) -> tuple[bool, dt.datetime, str, str]:
    # ``datetime.min`` only supplies a stable placeholder; the leading boolean
    # ensures it is never compared as a real timestamp against a claim time.
    last_claimed = candidate.last_claimed_at
    return (
        last_claimed is not None,
        last_claimed or dt.datetime.min.replace(tzinfo=dt.timezone.utc),
        candidate.source_id,
        str(candidate.installation_id),
    )


async def load_active_credential_renewal_candidates(
    pool: asyncpg.Pool,
    sources: tuple[SourceDefinition, ...],
    onboarding_scan_limit: int,
    due_limit: int,
    now: dt.datetime,
) -> tuple[CredentialRenewalCandidate, ...]:
    """Discover an active, exact-installation renewal candidate set.

    Due durable jobs use their own globally complete, tenant-bound scan and
    batch bound.  They are therefore never hidden by the onboarding sample.
    The onboarding scan runs only to fill a batch left short by that due work.
    Every candidate is rechecked through its source-owned installation table
    with its exact tenant bound before an invoker can receive it.
    """

    for field_name, value in (
        ("onboarding_scan_limit", onboarding_scan_limit),
        ("due_limit", due_limit),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{field_name} must be a positive integer")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(dt.timezone.utc)

    source_by_id = {source.source_id: source for source in sources}
    if len(source_by_id) != len(sources):
        raise CredentialRenewalContractError(
            "credential renewal catalog contains duplicate source IDs",
        )
    for source in sources:
        _validate_credential_renewal_source(source)

    due_candidates = await _load_due_credential_renewal_candidates(
        pool,
        source_by_id=source_by_id,
        limit=due_limit,
        now=now,
    )
    active_due_candidates = await _filter_to_active_installations(
        pool,
        source_by_id=source_by_id,
        candidates=due_candidates,
    )
    if len(active_due_candidates) >= due_limit:
        return fair_credential_renewal_candidates(
            active_due_candidates,
            limit=due_limit,
        )

    scan_cadence = credential_renewal_tick_interval_seconds(sources)
    scan_epoch = int(now.timestamp() // scan_cadence)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            _DISCOVER_ONBOARDED_INSTALLATIONS_SQL,
            list(source_by_id),
            scan_epoch,
            onboarding_scan_limit,
        )

    discovered: dict[tuple[str, UUID], set[UUID]] = defaultdict(set)
    for row in rows:
        source_id = str(row["source"])
        if source_id not in source_by_id:
            raise CredentialRenewalSchedulerError(
                "onboarding discovery returned a non-contract credential source",
            )
        tenant_id = _row_uuid(row["tenant_id"], field_name="tenant_id")
        installation_id = _row_uuid(
            row["installation_row_id"],
            field_name="installation_row_id",
        )
        discovered[(source_id, tenant_id)].add(installation_id)

    claim_times: dict[tuple[str, UUID, UUID], dt.datetime | None] = {}
    for tenant_id in sorted({tenant for _, tenant in discovered}, key=str):
        tenant_source_ids = sorted(
            source_id
            for source_id, candidate_tenant_id in discovered
            if candidate_tenant_id == tenant_id
        )
        claim_times.update(
            await _load_claim_times_for_tenant(
                pool,
                tenant_id=tenant_id,
                source_ids=tuple(tenant_source_ids),
            )
        )

    sampled_candidates = tuple(
        CredentialRenewalCandidate(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=installation_id,
            last_claimed_at=claim_times.get(
                (source_id, tenant_id, installation_id),
            ),
        )
        for (source_id, tenant_id), installation_ids in discovered.items()
        for installation_id in sorted(installation_ids, key=str)
    )
    active_sampled_candidates = await _filter_to_active_installations(
        pool,
        source_by_id=source_by_id,
        candidates=sampled_candidates,
    )
    return _merge_due_and_sampled_candidates(
        active_due_candidates,
        active_sampled_candidates,
    )


async def _load_due_credential_renewal_candidates(
    pool: asyncpg.Pool,
    *,
    source_by_id: dict[str, SourceDefinition],
    limit: int,
    now: dt.datetime,
) -> tuple[CredentialRenewalCandidate, ...]:
    """Load only claimable durable metadata, before onboarding sampling.

    Tenant IDs are discovered from non-secret onboarding rows, then each
    durable-job read is RLS-bound to that exact tenant.  Those reads exclude
    future cooldowns and both terminal states.  Expired leases are included
    because the fenced renewal substrate explicitly allows another worker to
    recover them.
    """

    async with pool.acquire() as conn:
        tenant_rows = await conn.fetch(
            _DISCOVER_DUE_RENEWAL_TENANTS_SQL,
            sorted(source_by_id),
        )
    tenant_ids = tuple(
        sorted(
            {
                _row_uuid(row["tenant_id"], field_name="tenant_id")
                for row in tenant_rows
            },
            key=str,
        )
    )

    candidates: list[CredentialRenewalCandidate] = []
    identities: set[tuple[str, UUID, UUID]] = set()
    for tenant_id in tenant_ids:
        async with pool.acquire() as conn:
            async with conn.transaction():
                async with bind_tenant(conn, tenant_id) as tctx:
                    rows = await tctx.fetch(
                        _LOAD_DUE_CREDENTIAL_RENEWAL_JOBS_FOR_TENANT_SQL,
                        tenant_id,
                        sorted(source_by_id),
                        now,
                        limit,
                    )
        for row in rows:
            source_id = str(row["source_id"])
            if source_id not in source_by_id:
                raise CredentialRenewalSchedulerError(
                    "durable renewal discovery returned a non-contract source",
                )
            row_tenant_id = _row_uuid(row["tenant_id"], field_name="tenant_id")
            if row_tenant_id != tenant_id:
                raise CredentialRenewalSchedulerError(
                    "tenant-bound durable renewal query returned another tenant",
                )
            installation_id = _row_uuid(
                row["installation_id"],
                field_name="installation_id",
            )
            identity = (source_id, tenant_id, installation_id)
            if identity in identities:
                raise CredentialRenewalSchedulerError(
                    "durable renewal discovery returned an exact duplicate",
                )
            identities.add(identity)
            candidates.append(
                CredentialRenewalCandidate(
                    source_id=source_id,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    last_claimed_at=_row_optional_timestamp(
                        row["last_claimed_at"],
                        field_name="last_claimed_at",
                    ),
                    durable_due=True,
                )
            )
    return tuple(candidates)


async def _filter_to_active_installations(
    pool: asyncpg.Pool,
    *,
    source_by_id: dict[str, SourceDefinition],
    candidates: Iterable[CredentialRenewalCandidate],
) -> tuple[CredentialRenewalCandidate, ...]:
    """Keep only source-owned installations active for their exact tenant."""

    candidate_list = tuple(candidates)
    grouped: dict[tuple[str, UUID], set[UUID]] = defaultdict(set)
    for candidate in candidate_list:
        grouped[(candidate.source_id, candidate.tenant_id)].add(
            candidate.installation_id,
        )

    active_identities: set[tuple[str, UUID, UUID]] = set()
    for (source_id, tenant_id), installation_ids in sorted(
        grouped.items(),
        key=lambda item: (item[0][0], str(item[0][1])),
    ):
        source = source_by_id[source_id]
        active_ids = await _load_exact_active_installations(
            pool,
            source=source,
            tenant_id=tenant_id,
            installation_ids=tuple(sorted(installation_ids, key=str)),
        )
        active_identities.update(
            (source_id, tenant_id, installation_id)
            for installation_id in active_ids
        )
    return tuple(
        candidate
        for candidate in candidate_list
        if (candidate.source_id, candidate.tenant_id, candidate.installation_id)
        in active_identities
    )


def _merge_due_and_sampled_candidates(
    due_candidates: Iterable[CredentialRenewalCandidate],
    sampled_candidates: Iterable[CredentialRenewalCandidate],
) -> tuple[CredentialRenewalCandidate, ...]:
    """Deduplicate exact identities, retaining durable state as authority."""

    merged: dict[tuple[str, UUID, UUID], CredentialRenewalCandidate] = {}
    for candidate in due_candidates:
        identity = (candidate.source_id, candidate.tenant_id, candidate.installation_id)
        if identity in merged:
            raise CredentialRenewalSchedulerError(
                "durable renewal discovery returned an exact duplicate",
            )
        merged[identity] = candidate
    for candidate in sampled_candidates:
        identity = (candidate.source_id, candidate.tenant_id, candidate.installation_id)
        merged.setdefault(identity, candidate)
    return tuple(
        sorted(
            merged.values(),
            key=lambda candidate: (
                not candidate.durable_due,
                candidate.source_id,
                str(candidate.tenant_id),
                str(candidate.installation_id),
            ),
        )
    )


async def _load_exact_active_installations(
    pool: asyncpg.Pool,
    *,
    source: SourceDefinition,
    tenant_id: UUID,
    installation_ids: tuple[UUID, ...],
) -> set[UUID]:
    """Read only active IDs under the exact tenant's RLS context."""

    if not installation_ids:
        return set()
    refresh = source.credential_refresh
    if refresh is None:  # guaranteed by ``_validate_credential_renewal_source``
        raise CredentialRenewalContractError(
            f"source {source.source_id!r} lacks credential_refresh",
        )
    query = _LOAD_ACTIVE_INSTALLATIONS_SQL.format(
        installation_table=refresh.install_table,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                rows = await tctx.fetch(
                    query,
                    tenant_id,
                    list(installation_ids),
                )
    return {_row_uuid(row["id"], field_name="id") for row in rows}


async def _load_claim_times_for_tenant(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_ids: tuple[str, ...],
) -> dict[tuple[str, UUID, UUID], dt.datetime | None]:
    """Load durable fairness metadata without reading a credential reference."""

    if not source_ids:
        return {}
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                rows = await tctx.fetch(
                    _LOAD_CLAIM_TIMES_SQL,
                    tenant_id,
                    list(source_ids),
                )
    result: dict[tuple[str, UUID, UUID], dt.datetime | None] = {}
    for row in rows:
        source_id = str(row["source_id"])
        installation_id = _row_uuid(row["installation_id"], field_name="installation_id")
        claimed_at = _row_optional_timestamp(
            row["last_claimed_at"],
            field_name="last_claimed_at",
        )
        result[(source_id, tenant_id, installation_id)] = claimed_at
    return result


def _row_uuid(value: object, *, field_name: str) -> UUID:
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise CredentialRenewalSchedulerError(
            f"discovered renewal candidate has invalid {field_name}",
        ) from exc


def _row_optional_timestamp(
    value: object,
    *,
    field_name: str,
) -> dt.datetime | None:
    if value is None:
        return None
    if not isinstance(value, dt.datetime):
        raise CredentialRenewalSchedulerError(
            f"renewal job has an invalid {field_name} value",
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise CredentialRenewalSchedulerError(
            f"renewal job has a naive {field_name} value",
        )
    return value.astimezone(dt.timezone.utc)


class CredentialRenewalScheduler(LongRunningService):
    """Run source-owned credential renewals in bounded fair batches."""

    def __init__(
        self,
        pool: Any,
        *,
        secret_store: Any,
        http: httpx.AsyncClient,
        config: CredentialRenewalSchedulerConfig | None = None,
        source_definitions: Iterable[SourceDefinition] | None = None,
        candidate_loader: CredentialRenewalCandidateLoader = (
            load_active_credential_renewal_candidates
        ),
        invoker_resolver: Callable[[str], CredentialRenewalInvoker] = (
            resolve_renewal_invoker
        ),
        clock: Clock | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._http = http
        self._config = config or CredentialRenewalSchedulerConfig()
        self._sources = credential_renewal_sources(source_definitions)
        self._source_ids = frozenset(source.source_id for source in self._sources)
        self._candidate_loader = candidate_loader
        self._invoker_resolver = invoker_resolver
        self._clock = clock or _utcnow
        self._worker_id = f"{self._config.instance_name}@{socket.gethostname()}"
        self.last_tick_result: CredentialRenewalTickResult | None = None

    @property
    def tick_interval_seconds(self) -> float:
        return credential_renewal_tick_interval_seconds(self._sources)

    async def tick(self) -> None:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise CredentialRenewalSchedulerError("scheduler clock returned naive time")
        now = now.astimezone(dt.timezone.utc)
        discovered = tuple(
            await self._candidate_loader(
                self._pool,
                self._sources,
                self._config.candidate_scan_limit,
                self._config.batch_size,
                now,
            )
        )
        for candidate in discovered:
            if candidate.source_id not in self._source_ids:
                raise CredentialRenewalSchedulerError(
                    "candidate loader returned a source outside the credential "
                    "renewal contract",
                )
        selected = fair_credential_renewal_candidates(
            discovered,
            limit=self._config.batch_size,
        )
        results = await self._invoke_selected(selected, now=now)
        outcomes = tuple(
            result for result in results if isinstance(result, RenewalOutcome)
        )
        invocation_errors = sum(
            1 for result in results if not isinstance(result, RenewalOutcome)
        )
        self.last_tick_result = CredentialRenewalTickResult(
            discovered_count=len(discovered),
            selected_count=len(selected),
            outcomes=outcomes,
            invocation_error_count=invocation_errors,
        )
        log.info(
            "credential_renewal_scheduler.tick_completed",
            extra={
                "discovered_count": len(discovered),
                "selected_count": len(selected),
                "outcome_count": len(outcomes),
                "invocation_error_count": invocation_errors,
            },
        )

    async def _invoke_selected(
        self,
        selected: tuple[CredentialRenewalCandidate, ...],
        *,
        now: dt.datetime,
    ) -> tuple[RenewalOutcome | BaseException, ...]:
        semaphore = asyncio.Semaphore(self._config.max_concurrency)

        async def invoke_one(
            candidate: CredentialRenewalCandidate,
        ) -> RenewalOutcome:
            async with semaphore:
                invoker = self._invoker_resolver(candidate.source_id)
                invocation = RenewalInvocation(
                    pool=self._pool,
                    tenant_id=candidate.tenant_id,
                    installation_id=candidate.installation_id,
                    target_key="installation",
                    secret_store=self._secret_store,
                    http=self._http,
                    worker_id=self._worker_id,
                    now=now,
                    lease_timeout_seconds=self._config.lease_timeout_seconds,
                )
                result = invoker(invocation)
                if not inspect.isawaitable(result):
                    raise CredentialRenewalContractError(
                        "credential renewal invoker must return an awaitable",
                    )
                outcome = await result
                if not isinstance(outcome, RenewalOutcome):
                    raise CredentialRenewalContractError(
                        "credential renewal invoker returned an invalid outcome",
                    )
                return outcome

        raw_results = await asyncio.gather(
            *(invoke_one(candidate) for candidate in selected),
            return_exceptions=True,
        )
        for result in raw_results:
            if isinstance(result, BaseException):
                # Only the controlled exception type is recorded.  Provider
                # response text and secret-store failures must never reach
                # this scheduler's process logs.
                log.error(
                    "credential_renewal_scheduler.invocation_failed",
                    extra={"error_type": type(result).__name__},
                )
        return tuple(raw_results)


async def run_forever(
    pool: Any,
    *,
    secret_store: Any,
    http: httpx.AsyncClient,
    config: CredentialRenewalSchedulerConfig | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Executable contract launcher shared by all credential sources."""

    scheduler = CredentialRenewalScheduler(
        pool,
        secret_store=secret_store,
        http=http,
        config=config,
    )
    await scheduler.run(stop_event=stop_event)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


__all__ = [
    "CredentialRenewalCandidate",
    "CredentialRenewalContractError",
    "CredentialRenewalScheduler",
    "CredentialRenewalSchedulerConfig",
    "CredentialRenewalSchedulerError",
    "CredentialRenewalTickResult",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_CANDIDATE_SCAN_LIMIT",
    "DEFAULT_LEASE_TIMEOUT_SECONDS",
    "DEFAULT_MAX_CONCURRENCY",
    "WORKER_COMPONENT_ID",
    "WORKFLOW_KIND",
    "credential_renewal_sources",
    "credential_renewal_tick_interval_seconds",
    "fair_credential_renewal_candidates",
    "load_active_credential_renewal_candidates",
    "run_forever",
]
