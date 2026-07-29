from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from services.ingest.source_contract import runtime as source_runtime
from services.ingest.source_contract.catalog import (
    NORMALIZER_BINDING_CATALOG,
    SOURCE_DEFINITIONS,
)
from services.ingest.source_contract.runtime import (
    BindingResolutionError,
    build_normalizer_ingress_headers,
    HistoryCallable,
    HistoryNotSupportedError,
    launch_live_worker,
    LiveWorkerNotFoundError,
    ManagedLiveRuntimeError,
    NormalizationChannelNotFoundError,
    NormalizerIngressMetadataError,
    RenewalNotSupportedError,
    override_history_bindings,
    resolve_callable_reference,
    resolve_fetcher,
    resolve_history_binding,
    resolve_handler,
    resolve_idempotency_builders,
    resolve_live_worker_dispatch,
    resolve_live_worker_launcher,
    resolve_onboarding_access_status,
    resolve_planner,
    resolve_provider_handoff,
    resolve_provider_setup_builder,
    resolve_provider_setup_notes,
    resolve_reconciler,
    resolve_renewal_invoker,
    resolve_rehearsal_artifact_builder,
    split_callable_reference,
    validate_runtime_bindings,
)


_RENEWAL_SOURCE_IDS = (
    "gmail",
    "google_calendar",
    "google_drive",
    "quickbooks",
    "ramp",
    "gusto",
    "carta",
    "linkedin",
)


def test_every_historical_binding_resolves_to_a_callable() -> None:
    historical_sources = [
        source for source in SOURCE_DEFINITIONS if source.history is not None
    ]

    assert len(historical_sources) == 26
    for source in historical_sources:
        assert callable(resolve_planner(source.source_id))
        assert callable(resolve_fetcher(source.source_id))
        assert callable(resolve_reconciler(source.source_id))


def test_every_declared_normalizer_binding_resolves_to_a_callable() -> None:
    assert len(NORMALIZER_BINDING_CATALOG) == 37
    for channel in NORMALIZER_BINDING_CATALOG:
        assert callable(resolve_handler(channel))


def test_every_source_idempotency_builder_resolves_to_a_callable() -> None:
    assert (
        sum(len(source.idempotency_builder_bindings) for source in SOURCE_DEFINITIONS)
        == 43
    )
    for source in SOURCE_DEFINITIONS:
        builders = resolve_idempotency_builders(source.source_id)
        assert len(builders) == len(source.idempotency_builder_bindings)
        assert all(callable(builder) for builder in builders)


def test_every_source_provider_handoff_resolves_to_a_callable() -> None:
    for source in SOURCE_DEFINITIONS:
        assert callable(resolve_provider_handoff(source.source_id))


def test_provider_setup_and_rehearsal_bindings_are_source_owned() -> None:
    for source in SOURCE_DEFINITIONS:
        notes = resolve_provider_setup_notes(source.source_id)
        assert notes(source.source_id)

    assert callable(resolve_provider_setup_builder("slack"))
    assert callable(resolve_provider_setup_builder("notion"))
    assert resolve_provider_setup_builder("github") is None
    for source_id in ("slack", "jira", "telegram"):
        assert callable(resolve_rehearsal_artifact_builder(source_id))
    assert resolve_rehearsal_artifact_builder("github") is None


def test_access_status_binding_is_source_owned_and_optional() -> None:
    assert callable(resolve_onboarding_access_status("discord"))
    assert resolve_onboarding_access_status("slack") is None


def test_startup_guard_resolves_every_contract_callable() -> None:
    validate_runtime_bindings()


def test_every_declared_renewal_invoker_resolves_to_a_callable() -> None:
    renewal_sources = tuple(
        source.source_id for source in SOURCE_DEFINITIONS if source.renewal is not None
    )

    assert renewal_sources == _RENEWAL_SOURCE_IDS
    for source_id in renewal_sources:
        assert callable(resolve_renewal_invoker(source_id))


def test_credential_renewal_workers_resolve_from_each_source_contract() -> None:
    credential_sources = tuple(
        source
        for source in SOURCE_DEFINITIONS
        if source.renewal is not None and source.renewal.kind == "credential"
    )

    assert len(credential_sources) == 5
    for source in credential_sources:
        assert callable(
            resolve_live_worker_launcher(
                source.source_id,
                "credential_renewal",
            )
        )


def test_source_without_renewal_fails_explicitly() -> None:
    with pytest.raises(
        RenewalNotSupportedError,
        match="no bounded renewal contract",
    ):
        resolve_renewal_invoker("slack")


def test_every_live_runtime_callable_resolves_or_declares_managed_boundary() -> None:
    for source in SOURCE_DEFINITIONS:
        for worker in source.live_runtime.workers:
            if worker.deployment_owner == "customer_deployment":
                with pytest.raises(ManagedLiveRuntimeError):
                    resolve_live_worker_launcher(
                        source.source_id,
                        worker.component_id,
                    )
                assert callable(
                    resolve_live_worker_dispatch(
                        source.source_id,
                        worker.component_id,
                    )
                )
                continue
            assert callable(
                resolve_live_worker_launcher(
                    source.source_id,
                    worker.component_id,
                )
            )
            assert (
                resolve_live_worker_dispatch(
                    source.source_id,
                    worker.component_id,
                )
                is None
            )


async def test_launch_live_worker_executes_resolved_async_launcher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def fake_launcher(*args: object, **kwargs: object) -> str:
        calls.append((args, kwargs))
        return "stopped"

    monkeypatch.setattr(
        source_runtime,
        "resolve_live_worker_launcher",
        lambda source_name, component_id: fake_launcher,
    )

    result = await launch_live_worker(
        "gmail",
        "gmail_history_poller",
        object(),
        stop_event=object(),
    )

    assert result == "stopped"
    assert len(calls) == 1
    assert len(calls[0][0]) == 1
    assert set(calls[0][1]) == {"stop_event"}


def test_unknown_live_worker_fails_explicitly() -> None:
    with pytest.raises(LiveWorkerNotFoundError, match="no live worker"):
        resolve_live_worker_launcher("slack", "imaginary_gateway")


def test_startup_guard_fails_closed_on_broken_live_launcher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = next(
        candidate for candidate in SOURCE_DEFINITIONS if candidate.source_id == "gmail"
    )
    worker = source.live_runtime.worker("gmail_history_poller")
    broken_worker = replace(
        worker,
        launcher_binding=(
            "services.ingest.source_contract.catalog:" "not_a_real_live_launcher"
        ),
    )
    replacement = replace(
        source,
        live_runtime=replace(
            source.live_runtime,
            workers=tuple(
                broken_worker if candidate is worker else candidate
                for candidate in source.live_runtime.workers
            ),
        ),
    )
    monkeypatch.setattr(
        source_runtime,
        "SOURCE_DEFINITIONS",
        tuple(
            replacement if candidate is source else candidate
            for candidate in SOURCE_DEFINITIONS
        ),
    )
    source_runtime.validate_runtime_bindings.cache_clear()
    try:
        with pytest.raises(BindingResolutionError, match="has no attribute"):
            source_runtime.validate_runtime_bindings()
    finally:
        source_runtime.validate_runtime_bindings.cache_clear()


def test_startup_guard_fails_closed_on_broken_renewal_invoker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = next(
        candidate for candidate in SOURCE_DEFINITIONS if candidate.source_id == "gmail"
    )
    assert source.renewal is not None
    replacement = replace(
        source,
        renewal=replace(
            source.renewal,
            invoker_binding=(
                "services.ingest.source_contract.catalog:" "not_a_real_renewal_invoker"
            ),
        ),
    )
    monkeypatch.setattr(
        source_runtime,
        "SOURCE_DEFINITIONS",
        tuple(
            replacement if candidate is source else candidate
            for candidate in SOURCE_DEFINITIONS
        ),
    )
    source_runtime.validate_runtime_bindings.cache_clear()
    try:
        with pytest.raises(BindingResolutionError, match="has no attribute"):
            source_runtime.validate_runtime_bindings()
    finally:
        source_runtime.validate_runtime_bindings.cache_clear()


@pytest.mark.parametrize(
    ("source_id", "field_name"),
    (
        ("slack", "metrics_export_bindings"),
        ("mercury", "finance_testing_binding"),
    ),
)
def test_startup_guard_fails_closed_on_broken_auxiliary_binding(
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
    field_name: str,
) -> None:
    source = next(
        candidate
        for candidate in SOURCE_DEFINITIONS
        if candidate.source_id == source_id
    )
    broken_reference = (
        "services.ingest.source_contract.catalog:not_a_real_auxiliary_callable"
    )
    if field_name == "metrics_export_bindings":
        replacement = replace(
            source,
            metrics_export_bindings=(broken_reference,),
        )
    else:
        replacement = replace(
            source,
            finance_testing_binding=broken_reference,
        )
    monkeypatch.setattr(
        source_runtime,
        "SOURCE_DEFINITIONS",
        tuple(
            replacement if candidate is source else candidate
            for candidate in SOURCE_DEFINITIONS
        ),
    )
    source_runtime.validate_runtime_bindings.cache_clear()
    try:
        with pytest.raises(BindingResolutionError, match="has no attribute"):
            source_runtime.validate_runtime_bindings()
    finally:
        source_runtime.validate_runtime_bindings.cache_clear()


def test_unknown_normalizer_channel_fails_explicitly() -> None:
    with pytest.raises(
        NormalizationChannelNotFoundError,
        match="no declared normalizer",
    ):
        resolve_handler("mars:webhook")


def test_webhook_handler_headers_are_projected_from_contract_metadata() -> None:
    assert build_normalizer_ingress_headers(
        source_name="github",
        ingress_kind="webhook",
        channel="github:webhook",
        ingress_metadata={
            "event_type": "pull_request",
            "delivery_id": "delivery-1",
        },
    ) == {"X-GitHub-Event": "pull_request"}
    assert (
        build_normalizer_ingress_headers(
            source_name="slack",
            ingress_kind="webhook",
            channel="slack:message",
            ingress_metadata={"event_type": "message"},
        )
        == {}
    )
    assert (
        build_normalizer_ingress_headers(
            source_name="github",
            ingress_kind="backfill",
            channel="github:webhook",
            ingress_metadata={},
        )
        == {}
    )


@pytest.mark.parametrize(
    "metadata",
    (
        {},
        {"event_type": None},
        {"event_type": ""},
        {"event_type": "   "},
        {"event_type": 7},
    ),
)
def test_webhook_handler_header_projection_fails_closed(
    metadata: dict[str, object],
) -> None:
    with pytest.raises(
        NormalizerIngressMetadataError,
        match="event_type",
    ):
        build_normalizer_ingress_headers(
            source_name="github",
            ingress_kind="webhook",
            channel="github:webhook",
            ingress_metadata=metadata,
        )


def test_webhook_handler_header_projection_requires_exact_contract() -> None:
    with pytest.raises(
        NormalizerIngressMetadataError,
        match="exactly one",
    ):
        build_normalizer_ingress_headers(
            source_name="github",
            ingress_kind="webhook",
            channel="github:not-the-contract-channel",
            ingress_metadata={"event_type": "issues"},
        )


@pytest.mark.parametrize(
    ("source_name", "channel"),
    (
        ("whatsapp", "whatsapp:message"),
        ("facebook_pages", "facebook_pages:message"),
        ("google_calendar", "google_calendar:event"),
        ("google_drive", "google_drive:file"),
    ),
)
def test_dedicated_webhook_ingress_without_projection_builds_empty_headers(
    source_name: str,
    channel: str,
) -> None:
    assert (
        build_normalizer_ingress_headers(
            source_name=source_name,
            ingress_kind="webhook",
            channel=channel,
            ingress_metadata={"delivery_id": "dedicated-delivery"},
        )
        == {}
    )


@pytest.mark.parametrize("role", ["planner", "fetcher", "reconciler"])
def test_whatsapp_fails_explicitly_as_having_no_history(role: str) -> None:
    with pytest.raises(HistoryNotSupportedError, match="no historical"):
        resolve_history_binding("whatsapp", role)  # type: ignore[arg-type]


def test_callable_reference_validation_and_resolution_fail_loudly() -> None:
    with pytest.raises(BindingResolutionError, match="expected"):
        split_callable_reference("services.ingest.planners.slack")
    with pytest.raises(BindingResolutionError, match="could not import"):
        resolve_callable_reference("not_a_real_package.module:callable")
    with pytest.raises(BindingResolutionError, match="has no attribute"):
        resolve_callable_reference(
            "services.ingest.source_contract.catalog:not_a_real_attribute"
        )
    with pytest.raises(BindingResolutionError, match="non-callable"):
        resolve_callable_reference(
            "services.ingest.source_contract.catalog:CANONICAL_SOURCE_IDS"
        )


def test_scoped_overrides_nest_restore_and_do_not_mutate_catalog() -> None:
    original = resolve_planner("slack")

    def outer(*args: object, **kwargs: object) -> object:
        return None

    def inner(*args: object, **kwargs: object) -> object:
        return None

    with override_history_bindings(planners={"slack": outer}):
        assert resolve_planner("slack") is outer
        with override_history_bindings(planners={"slack": inner}):
            assert resolve_planner("slack") is inner
        assert resolve_planner("slack") is outer

    assert resolve_planner("slack") is original


def test_override_can_replace_each_history_role_in_one_scope() -> None:
    def planner(*args: object, **kwargs: object) -> object:
        return None

    def fetcher(*args: object, **kwargs: object) -> object:
        return None

    def reconciler(*args: object, **kwargs: object) -> object:
        return None

    with override_history_bindings(
        planners={"github": planner},
        fetchers={"github": fetcher},
        reconcilers={"github": reconciler},
    ):
        assert resolve_planner("github") is planner
        assert resolve_fetcher("github") is fetcher
        assert resolve_reconciler("github") is reconciler


@pytest.mark.asyncio
async def test_overrides_are_isolated_between_concurrent_tasks() -> None:
    original = resolve_fetcher("slack")

    async def first(*args: object, **kwargs: object) -> object:
        return None

    async def second(*args: object, **kwargs: object) -> object:
        return None

    async def resolve_inside(
        replacement: HistoryCallable,
    ) -> HistoryCallable:
        with override_history_bindings(
            fetchers={"slack": replacement},
        ):
            await asyncio.sleep(0)
            return resolve_fetcher("slack")

    resolved = await asyncio.gather(
        resolve_inside(first),
        resolve_inside(second),
    )

    assert resolved == [first, second]
    assert resolve_fetcher("slack") is original


def test_invalid_overrides_fail_before_entering_scope() -> None:
    with pytest.raises(TypeError, match="must be callable"):
        with override_history_bindings(
            planners={"slack": object()},  # type: ignore[dict-item]
        ):
            raise AssertionError("unreachable")

    with pytest.raises(HistoryNotSupportedError, match="no historical"):
        with override_history_bindings(
            fetchers={"whatsapp": lambda: None},
        ):
            raise AssertionError("unreachable")

    with pytest.raises(ValueError, match="at least one"):
        with override_history_bindings():
            raise AssertionError("unreachable")


def test_legacy_mutable_dispatch_registries_are_not_exposed() -> None:
    from services.ingest.ingestion import fetchers, handlers, planners, reconcilers

    absent: tuple[tuple[object, str], ...] = (
        (planners, "PLANNER_DISPATCH"),
        (fetchers, "FETCHER_DISPATCH"),
        (reconcilers, "RECONCILER_DISPATCH"),
        (handlers, "_HANDLERS"),
        (handlers, "register"),
    )
    assert all(not hasattr(module, name) for module, name in absent)
