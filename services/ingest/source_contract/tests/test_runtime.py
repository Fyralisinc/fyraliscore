from __future__ import annotations

import asyncio

import pytest

from services.ingest.source_contract.catalog import (
    NORMALIZER_BINDING_CATALOG,
    SOURCE_DEFINITIONS,
)
from services.ingest.source_contract.runtime import (
    BindingResolutionError,
    HistoryCallable,
    HistoryNotSupportedError,
    NormalizationChannelNotFoundError,
    override_history_bindings,
    resolve_callable_reference,
    resolve_fetcher,
    resolve_history_binding,
    resolve_handler,
    resolve_idempotency_builders,
    resolve_planner,
    resolve_reconciler,
    split_callable_reference,
    validate_runtime_bindings,
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
    assert sum(
        len(source.idempotency_builder_bindings)
        for source in SOURCE_DEFINITIONS
    ) == 43
    for source in SOURCE_DEFINITIONS:
        builders = resolve_idempotency_builders(source.source_id)
        assert len(builders) == len(source.idempotency_builder_bindings)
        assert all(callable(builder) for builder in builders)


def test_startup_guard_resolves_every_contract_callable() -> None:
    validate_runtime_bindings()


def test_unknown_normalizer_channel_fails_explicitly() -> None:
    with pytest.raises(
        NormalizationChannelNotFoundError,
        match="no declared normalizer",
    ):
        resolve_handler("mars:webhook")


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
