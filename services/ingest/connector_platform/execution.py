"""Workflow adapters backed exclusively by registry-resolved capabilities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any
from uuid import UUID, uuid4

from services.ingest.connector_runtime.authority import (
    AuthorityRepository,
    scope_authority,
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
from services.ingest.connector_platform.workflow_models import (
    FetchResult,
    ObservationDraft,
    ReconciliationDecision,
    ResharedShard,
    Shard,
)
from services.ingest.source_contract.capabilities import (
    GATEWAY_STREAM_V1,
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    PUSH_SUBSCRIPTION_V1,
    RECONCILIATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.capabilities.ingestion import (
    GatewayOpenRequest,
    GatewayReceiveRequest,
    SubscriptionRequest,
    SubscriptionState,
)
from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.errors import BindingError
from services.ingest.source_contract.host_services import InstallationDataPatch
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    CursorState,
    FetchRequest,
    IdentityInput,
    InstallationRef,
    NormalizationInput,
    PlanRequest,
    PollRequest,
    ReconciliationRequest,
    ShardSummary,
    ShardPlan,
    VerifiedWebhookResult,
)

def _value(record: Any, key: str, default: Any = None) -> Any:
    try:
        return record[key]
    except (KeyError, TypeError):
        getter = getattr(record, "get", None)
        return getter(key, default) if getter is not None else default


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (str, bytes)):
        parsed = json.loads(value)
        return dict(parsed) if isinstance(parsed, dict) else {}
    try:
        return dict(value)
    except (TypeError, ValueError):
        return {}


def _workflow_shard(shard: ShardPlan) -> Shard:
    return Shard(
        shard_kind=shard.kind,
        shard_identifier=dict(shard.identifier),
        recency_score=shard.priority,
        window_start=shard.window_start,
        window_end=shard.window_end,
    )


def _observation_draft(draft: Any) -> ObservationDraft:
    return ObservationDraft(
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
        source_object=draft.source_object,
    )


class ConnectorExecutionRouter:
    """Resolve and execute contract capabilities for existing workflow DTOs."""

    def __init__(
        self,
        composition: ConnectorRuntimeComposition,
        host_services: HostServicesFactory,
        *,
        deadline_seconds: float = 60.0,
        authority_repository: AuthorityRepository | None = None,
        require_durable_authority: bool = False,
    ) -> None:
        self._composition = composition
        self._host_services = host_services
        self._executor = ConnectorCapabilityExecutor(
            composition.registry,
            composition.routing,
        )
        self._deadline_seconds = deadline_seconds
        self._authority_repository = authority_repository
        self._require_durable_authority = require_durable_authority

    def supports(self, source: str) -> bool:
        try:
            self._composition.registry.for_source(source)
        except Exception:
            return False
        return True

    def is_native(self, source: str) -> bool:
        try:
            return self._composition.registry.for_source(source).origin.startswith(
                "first-party-native:"
            )
        except Exception:
            return False

    def _installation(self, source: str, install: Any) -> InstallationRef:
        description = self._composition.registry.for_source(source).describe()
        return InstallationRef(
            id=UUID(str(_value(install, "id"))),
            tenant_id=UUID(str(_value(install, "tenant_id"))),
            connector_id=description.connector_id,
            generation=max(1, int(_value(install, "generation", 1))),
        )

    def _authority(self, source: str, install: Any) -> GrantedAuthority:
        manifest = self._composition.registry.for_source(source).manifest
        # Processes without a durable authority repository are restricted to
        # secretless, networkless capabilities such as pure normalization.
        # Requested manifest permissions are never treated as grants.
        return GrantedAuthority(
            maximum_trust_tier=manifest.spec.trust.maximum_tier,
        )

    def _lifecycle(
        self, installation: InstallationRef, install: Any
    ) -> InstallationLifecycle:
        enabled = bool(_value(install, "enabled", True))
        desired_value = str(
            _value(
                install,
                "desired_state",
                "Ready" if enabled else "Paused",
            )
        )
        observed_value = str(
            _value(
                install,
                "observed_phase",
                "Ready" if enabled else "Paused",
            )
        )
        return InstallationLifecycle(
            installation_id=installation.id,
            tenant_id=installation.tenant_id,
            connector_id=installation.connector_id,
            desired=DesiredInstallationState(desired_value),
            observed=InstallationPhase(observed_value),
            generation=installation.generation,
            observed_generation=max(
                0,
                int(
                    _value(
                        install,
                        "observed_generation",
                        installation.generation,
                    )
                ),
            ),
        )

    async def _base(
        self,
        source: str,
        install: Any,
    ) -> tuple[InstallationRef, GrantedAuthority, Any, InstallationLifecycle]:
        installation = self._installation(source, install)
        authority = self._authority(source, install)
        if self._authority_repository is not None:
            durable = await self._authority_repository.load(installation.id)
            if durable is not None:
                authority = durable.validate_for(installation)
            elif self._require_durable_authority:
                raise BindingError(
                    "installation has no durable authority grant",
                    details={"installation_id": str(installation.id)},
                )
        elif self._require_durable_authority:
            raise BindingError("durable authority repository is unavailable")
        authority = scope_authority(
            self._composition.registry.for_source(source).manifest,
            authority,
        )
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
    ) -> list[Shard]:
        install = planner_context.install
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, operation: Any) -> list[Shard]:
            selected = tuple(
                str(item)
                for item in (_value(install, "selected_resources", ()) or ())
            )
            result = await capability.plan(
                PlanRequest(selected_resources=selected), operation
            )
            return [_workflow_shard(shard) for shard in result.shards]

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=HISTORICAL_PULL_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def fetch(
        self,
        source: str,
        install: Any,
        shard_identifier: dict[str, Any],
        cursor: dict[str, Any] | None,
        *,
        shard_kind: str | None = None,
    ) -> FetchResult:
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, operation: Any) -> FetchResult:
            page = await capability.fetch(
                FetchRequest(
                    shard=ShardPlan(
                        kind=shard_kind
                        or str(shard_identifier.get("shard_kind") or "source_shard"),
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
                    (page.checkpoint or page.next_cursor).payload
                    if (page.checkpoint or page.next_cursor) is not None
                    else None
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
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def poll(
        self,
        source: str,
        install: Any,
        shard_identifier: dict[str, Any],
        cursor: dict[str, Any] | None,
    ) -> FetchResult:
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, operation: Any) -> FetchResult:
            page = await capability.poll(
                PollRequest(
                    cursor=CursorState(schema_version=1, payload=cursor)
                    if cursor is not None
                    else None,
                    selected_resources=(
                        str(shard_identifier.get("resource_id")),
                    )
                    if shard_identifier.get("resource_id") is not None
                    else (),
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
                    (page.checkpoint or page.next_cursor).payload
                    if (page.checkpoint or page.next_cursor) is not None
                    else None
                ),
                end_of_data=page.end_of_data,
            )

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=INCREMENTAL_POLL_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def identity(
        self,
        source: str,
        install: Any,
        input: IdentityInput,
    ) -> str:
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, _operation: Any) -> str:
            return capability.external_id(input)

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=IDENTITY_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def reconcile(
        self,
        source: str,
        install: Any,
        shards: list[Any],
        run: Any,
    ) -> ReconciliationDecision:
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(
            capability: Any, operation: Any
        ) -> ReconciliationDecision:
            decision = await capability.reconcile(
                ReconciliationRequest(
                    run_id=UUID(str(_value(run, "onboarding_run_id", uuid4()))),
                    shards=tuple(
                        ShardSummary(
                            shard_id=UUID(str(_value(shard, "id"))),
                            shard=ShardPlan(
                                kind=str(_value(shard, "shard_kind", "source_shard")),
                                identifier=_object(
                                    _value(shard, "shard_identifier", {})
                                ),
                            ),
                            state=str(_value(shard, "state", "unknown")),
                            cursor=(
                                CursorState(
                                    schema_version=1,
                                    payload=_object(_value(shard, "cursor")),
                                )
                                if _value(shard, "cursor") is not None
                                else None
                            ),
                            record_count=max(
                                0, int(_value(shard, "observations_seen", 0))
                            ),
                        )
                        for shard in shards
                    ),
                    pass_number=max(
                        1, int(_value(run, "reconciliation_pass_count", 0)) + 1
                    ),
                ),
                operation,
            )
            return ReconciliationDecision(
                has_gaps=decision.has_gaps,
                message=decision.message,
                new_shards=[
                    ResharedShard(
                        shard=_workflow_shard(item.shard),
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
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def normalize(
        self,
        source: str,
        install: Any,
        input: NormalizationInput,
    ) -> ObservationDraft:
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, operation: Any) -> ObservationDraft:
            drafts = await capability.normalize(input, operation)
            if len(drafts) != 1:
                raise ValueError("normalizer workflow requires exactly one draft")
            return _observation_draft(drafts[0])

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=NORMALIZATION_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def webhook(
        self,
        source: str,
        install: Any,
        request_value: BoundedWebhookRequest,
    ) -> VerifiedWebhookResult:
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(
            capability: Any, operation: Any
        ) -> VerifiedWebhookResult:
            return await capability.verify_and_decode(request_value, operation)

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=WEBHOOK_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def webhook_and_emit(
        self,
        source: str,
        install: Any,
        request_value: BoundedWebhookRequest,
    ) -> VerifiedWebhookResult:
        """Verify/decode then durably publish every contract event."""

        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(
            capability: Any, operation: Any
        ) -> VerifiedWebhookResult:
            result = await capability.verify_and_decode(request_value, operation)
            for event in result.events:
                await operation.services.raw_emission.emit(
                    event.record, ingress_kind="webhook"
                )
            return result

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=WEBHOOK_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def run_gateway(
        self,
        source: str,
        install: Any,
        stop_event: Any,
        *,
        batch_size: int = 100,
    ) -> None:
        """Own one installation's resumable gateway session until stopped.

        Raw publication is acknowledged before the resume state advances. This
        preserves the same durable publication/checkpoint ordering as backfill.
        """

        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, operation: Any) -> None:
            stored = await operation.services.installation_store.read("gateway.resume")
            generation = stored.generation if stored is not None else 0
            resume = None
            if stored is not None and stored.values.get("payload") is not None:
                resume = CursorState(
                    schema_version=int(stored.values.get("schema_version", 1)),
                    payload=_object(stored.values.get("payload")),
                )
            session = await capability.open(
                GatewayOpenRequest(resume_state=resume), operation
            )
            try:
                while not stop_event.is_set():
                    batch = await capability.receive(
                        GatewayReceiveRequest(
                            session=session,
                            max_records=batch_size,
                        ),
                        operation,
                    )
                    for record in batch.records:
                        await operation.services.raw_emission.emit(
                            record, ingress_kind="gateway"
                        )
                    if batch.resume_state is not None:
                        generation = await operation.services.installation_store.compare_and_set(
                            InstallationDataPatch(
                                namespace="gateway.resume",
                                expected_generation=generation,
                                values={
                                    "schema_version": batch.resume_state.schema_version,
                                    "payload": dict(batch.resume_state.payload),
                                },
                            )
                        )
                        session = session.model_copy(
                            update={"resume_state": batch.resume_state}
                        )
                    await operation.services.lease.heartbeat(
                        {
                            "source": source,
                            "records": len(batch.records),
                            "resume_generation": generation,
                        }
                    )
                    if batch.session_closed:
                        await capability.close(session, operation)
                        session = await capability.open(
                            GatewayOpenRequest(resume_state=session.resume_state),
                            operation,
                        )
            finally:
                await capability.close(session, operation)

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=GATEWAY_STREAM_V1,
            connector_call=connector_call,
            deadline=datetime.now(timezone.utc) + timedelta(days=365),
            lifecycle=lifecycle,
        )
        await self._executor.execute(request)

    async def ensure_subscription(
        self,
        source: str,
        install: Any,
        *,
        event_types: tuple[str, ...] = (),
    ) -> SubscriptionState:
        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, operation: Any) -> SubscriptionState:
            callback_url = "https://localhost.invalid/unused"
            endpoint_id = f"{source}:{installation.id}"
            verification_token = None
            if source != "gmail":
                allocation = await operation.services.subscription_callbacks.allocate(
                    f"{source}.watch"
                )
                callback_url = allocation.callback_url
                endpoint_id = allocation.endpoint_id
                verification_token = allocation.verification_nonce.reveal_text()
            subscription = await capability.ensure(
                SubscriptionRequest(
                    callback_url=callback_url,
                    endpoint_id=endpoint_id,
                    event_types=event_types,
                    verification_token=verification_token,
                ),
                operation,
            )
            stored = await operation.services.installation_store.read(
                "subscription.state"
            )
            await operation.services.installation_store.compare_and_set(
                InstallationDataPatch(
                    namespace="subscription.state",
                    expected_generation=stored.generation if stored else 0,
                    values=subscription.model_dump(mode="json"),
                )
            )
            return subscription

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=PUSH_SUBSCRIPTION_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)

    async def poll_and_emit(
        self,
        source: str,
        install: Any,
        *,
        page_size: int = 100,
    ) -> tuple[int, bool]:
        """Poll one page, durably emit it, then atomically advance state."""

        installation, authority, services, lifecycle = await self._base(source, install)

        async def connector_call(capability: Any, operation: Any) -> tuple[int, bool]:
            stored = await operation.services.installation_store.read("poll.cursor")
            generation = stored.generation if stored is not None else 0
            cursor = None
            if stored is not None:
                cursor = CursorState(
                    schema_version=int(stored.values.get("schema_version", 1)),
                    payload=_object(stored.values.get("payload")),
                )
            elif source == "gmail":
                subscription = await operation.services.installation_store.read(
                    "subscription.state"
                )
                state = _object(subscription.values.get("state")) if subscription else {}
                payload = _object(state.get("payload"))
                if payload:
                    cursor = CursorState(
                        schema_version=int(state.get("schema_version", 1)),
                        payload=payload,
                    )
            page = await capability.poll(
                PollRequest(cursor=cursor, page_size_hint=page_size), operation
            )
            for record in page.records:
                await operation.services.raw_emission.emit(
                    record, ingress_kind="poll"
                )
            proposed = page.next_cursor or page.checkpoint
            if proposed is not None:
                await operation.services.installation_store.compare_and_set(
                    InstallationDataPatch(
                        namespace="poll.cursor",
                        expected_generation=generation,
                        values={
                            "schema_version": proposed.schema_version,
                            "payload": dict(proposed.payload),
                        },
                    )
                )
            return len(page.records), not page.end_of_data

        request = CapabilityExecutionRequest(
            installation=installation,
            source=source,
            authority=authority,
            services=services,
            capability=INCREMENTAL_POLL_V1,
            connector_call=connector_call,
            deadline=self._deadline(),
            lifecycle=lifecycle,
        )
        return await self._executor.execute(request)


__all__ = ["ConnectorExecutionRouter"]
