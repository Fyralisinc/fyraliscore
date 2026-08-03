"""Old-shape worker adapters backed by registry-resolved capabilities."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar
from uuid import UUID, uuid4

from services.ingest.connector_platform.legacy_context import (
    LegacyBindingPayload,
    legacy_binding_scope,
)
from services.ingest.connector_runtime.composition import ConnectorRuntimeComposition
from services.ingest.connector_runtime.execution import (
    CapabilityExecutionRequest,
    ConnectorCapabilityExecutor,
)
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.lifecycle import (
    DesiredInstallationState,
    InstallationLifecycle,
    InstallationPhase,
)
from services.ingest.connector_runtime.shadow import (
    ShadowDimension,
    ShadowReportSink,
)
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.ingestion.handlers import ObservationDraft as LegacyDraft
from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision as LegacyReconciliationDecision,
    ResharedShard,
)
from services.ingest.source_contract.capabilities import (
    HISTORICAL_PULL_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    CursorState,
    FetchRequest,
    IdentityInput,
    InstallationRef,
    NormalizationInput,
    PlanRequest,
    ReconciliationRequest,
    ShardPlan,
    VerifiedWebhookResult,
)


T = TypeVar("T")
LegacyWebhookCall = Callable[[], Awaitable[VerifiedWebhookResult]]


def _value(record: Any, key: str, default: Any = None) -> Any:
    try:
        return record[key]
    except (KeyError, TypeError):
        getter = getattr(record, "get", None)
        return getter(key, default) if getter is not None else default


def _legacy_shard(shard: ShardPlan) -> Shard:
    return Shard(
        shard_kind=shard.kind,
        shard_identifier=dict(shard.identifier),
        recency_score=shard.priority,
        window_start=shard.window_start,
        window_end=shard.window_end,
    )


def _legacy_draft(draft: Any) -> LegacyDraft:
    return LegacyDraft(
        source_channel=draft.source_channel,
        content_text=draft.content_text,
        content=dict(draft.content),
        occurred_at=draft.occurred_at,
        trust_tier=draft.trust_tier,
        kind=draft.kind,
        source_actor_ref=draft.source_actor_ref,
        external_id=draft.external_id,
        entities_hint=list(draft.entities_hint),
        raw_payload=draft.raw_payload,
    )


class LegacyExecutionRouter:
    """Compatibility facade retaining every pre-Phase-2 return shape."""

    def __init__(
        self,
        composition: ConnectorRuntimeComposition,
        host_services: HostServicesFactory,
        *,
        shadow_sink: ShadowReportSink | None = None,
        deadline_seconds: float = 60.0,
    ) -> None:
        self._composition = composition
        self._host_services = host_services
        self._executor = ConnectorCapabilityExecutor(
            composition.registry,
            composition.routing,
            shadow_sink=shadow_sink,
        )
        self._deadline_seconds = deadline_seconds

    def _installation(self, source: str, install: Any) -> InstallationRef:
        description = self._composition.registry.for_source(source).describe()
        return InstallationRef(
            id=UUID(str(_value(install, "id"))),
            tenant_id=UUID(str(_value(install, "tenant_id"))),
            connector_id=description.connector_id,
            generation=1,
        )

    def _authority(self, source: str) -> GrantedAuthority:
        manifest = self._composition.registry.for_source(source).manifest
        permissions = manifest.spec.permissions
        return GrantedAuthority(
            secret_slots=frozenset(permissions.secret_slots),
            outbound_hosts=frozenset(permissions.outbound_hosts),
            scopes=frozenset(permissions.requested_scopes),
            maximum_trust_tier=manifest.spec.trust.maximum_tier,
        )

    def _lifecycle(
        self, installation: InstallationRef, install: Any
    ) -> InstallationLifecycle:
        enabled = bool(_value(install, "enabled", True))
        return InstallationLifecycle(
            installation_id=installation.id,
            tenant_id=installation.tenant_id,
            connector_id=installation.connector_id,
            desired=(
                DesiredInstallationState.READY
                if enabled
                else DesiredInstallationState.PAUSED
            ),
            observed=(InstallationPhase.READY if enabled else InstallationPhase.PAUSED),
            generation=installation.generation,
            observed_generation=installation.generation,
        )

    def _base(
        self,
        source: str,
        install: Any,
    ) -> tuple[InstallationRef, GrantedAuthority, Any, InstallationLifecycle]:
        installation = self._installation(source, install)
        authority = self._authority(source)
        services = self._host_services.build(
            installation.id,
            authority,
            connector_id=installation.connector_id,
        )
        return installation, authority, services, self._lifecycle(installation, install)

    def _deadline(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(seconds=self._deadline_seconds)

    async def plan(
        self,
        source: str,
        planner_context: Any,
        *,
        shadow_safe: bool = False,
    ) -> list[Shard]:
        install = planner_context.install
        installation, authority, services, lifecycle = self._base(source, install)
        payload = LegacyBindingPayload(
            install=install,
            planner_context=planner_context,
            external_installation_id=str(_value(install, "installation_id", "")),
        )

        async def legacy_call() -> list[Shard]:
            return await PLANNER_DISPATCH[source](planner_context)

        async def connector_call(capability: Any, operation: Any) -> list[Shard]:
            result = await capability.plan(PlanRequest(), operation)
            return [_legacy_shard(shard) for shard in result.shards]

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=HISTORICAL_PULL_V1,
            connector_call=connector_call,
            legacy_call=legacy_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
            shadow_safe=shadow_safe,
            shadow_projection=lambda value: {ShadowDimension.STATE: value},
        )
        with legacy_binding_scope(payload):
            return await self._executor.execute(request)

    async def fetch(
        self,
        source: str,
        install: Any,
        shard_identifier: dict[str, Any],
        cursor: dict[str, Any] | None,
        *,
        shard_kind: str | None = None,
        shadow_safe: bool = False,
    ) -> FetchResult:
        installation, authority, services, lifecycle = self._base(source, install)
        payload = LegacyBindingPayload(
            install=install,
            external_installation_id=str(_value(install, "installation_id", "")),
        )

        async def legacy_call() -> FetchResult:
            return await FETCHER_DISPATCH[source](install, shard_identifier, cursor)

        async def connector_call(capability: Any, operation: Any) -> FetchResult:
            page = await capability.fetch(
                FetchRequest(
                    shard=ShardPlan(
                        kind=shard_kind
                        or str(shard_identifier.get("shard_kind") or "legacy_shard"),
                        identifier=shard_identifier,
                    ),
                    cursor=CursorState(schema_version=1, payload=cursor)
                    if cursor is not None
                    else None,
                ),
                operation,
            )
            return FetchResult(
                records=[
                    dict(record.payload)
                    for record in page.records
                    if isinstance(record.payload, dict)
                ],
                next_cursor=(
                    page.next_cursor.payload if page.next_cursor is not None else None
                ),
                end_of_data=page.end_of_data,
            )

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=HISTORICAL_PULL_V1,
            connector_call=connector_call,
            legacy_call=legacy_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
            shadow_safe=shadow_safe,
            shadow_projection=lambda value: {
                ShadowDimension.PUBLICATION: value.records,
                ShadowDimension.CURSOR: value.next_cursor,
                ShadowDimension.STATE: value.end_of_data,
            },
        )
        with legacy_binding_scope(payload):
            return await self._executor.execute(request)

    async def reconcile(
        self,
        source: str,
        install: Any,
        shards: list[Any],
        run: Any,
        *,
        shadow_safe: bool = False,
    ) -> LegacyReconciliationDecision:
        installation, authority, services, lifecycle = self._base(source, install)
        payload = LegacyBindingPayload(
            install=install,
            external_installation_id=str(_value(install, "installation_id", "")),
            reconciliation_shards=shards,
            reconciliation_run=run,
        )

        async def legacy_call() -> LegacyReconciliationDecision:
            return await RECONCILER_DISPATCH[source](shards, run)

        async def connector_call(
            capability: Any, operation: Any
        ) -> LegacyReconciliationDecision:
            decision = await capability.reconcile(
                ReconciliationRequest(
                    run_id=UUID(str(_value(run, "onboarding_run_id", uuid4()))),
                    shards=(),
                    pass_number=max(
                        1, int(_value(run, "reconciliation_pass_count", 0)) + 1
                    ),
                ),
                operation,
            )
            return LegacyReconciliationDecision(
                has_gaps=decision.has_gaps,
                message=decision.message,
                new_shards=[
                    ResharedShard(
                        shard=_legacy_shard(item.shard),
                        parent_shard_id=item.parent_shard_id,
                    )
                    for item in decision.new_shards
                ],
            )

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=RECONCILIATION_V1,
            connector_call=connector_call,
            legacy_call=legacy_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
            shadow_safe=shadow_safe,
            shadow_projection=lambda value: {ShadowDimension.STATE: value},
        )
        with legacy_binding_scope(payload):
            return await self._executor.execute(request)

    async def normalize(
        self,
        source: str,
        install: Any,
        input: NormalizationInput,
        legacy_call: Callable[[], Awaitable[LegacyDraft]],
    ) -> LegacyDraft:
        installation, authority, services, lifecycle = self._base(source, install)
        payload = LegacyBindingPayload(
            install=install,
            external_installation_id=str(_value(install, "installation_id", "")),
        )

        async def connector_call(capability: Any, operation: Any) -> LegacyDraft:
            drafts = await capability.normalize(input, operation)
            if len(drafts) != 1:
                raise ValueError("legacy normalizer bridge requires one draft")
            return _legacy_draft(drafts[0])

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=NORMALIZATION_V1,
            connector_call=connector_call,
            legacy_call=legacy_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
            shadow_projection=lambda value: {
                ShadowDimension.IDENTITY: value.external_id,
                ShadowDimension.NORMALIZATION: value,
            },
        )
        with legacy_binding_scope(payload):
            return await self._executor.execute(request)

    async def webhook(
        self,
        source: str,
        install: Any,
        request_value: BoundedWebhookRequest,
        legacy_call: LegacyWebhookCall,
    ) -> VerifiedWebhookResult:
        installation, authority, services, lifecycle = self._base(source, install)
        payload = LegacyBindingPayload(
            install=install,
            external_installation_id=str(_value(install, "installation_id", "")),
        )

        async def connector_call(capability: Any, operation: Any) -> VerifiedWebhookResult:
            return await capability.verify_and_decode(request_value, operation)

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=WEBHOOK_V1,
            connector_call=connector_call,
            legacy_call=legacy_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
            shadow_projection=lambda value: {
                ShadowDimension.IDENTITY: tuple(
                    event.external_installation_id for event in value.events
                ),
                ShadowDimension.PUBLICATION: tuple(
                    event.record for event in value.events
                ),
            },
        )
        with legacy_binding_scope(payload):
            return await self._executor.execute(request)


__all__ = ["LegacyExecutionRouter", "LegacyWebhookCall"]
