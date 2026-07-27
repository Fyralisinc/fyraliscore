from __future__ import annotations

import datetime as dt
from uuid import UUID

import pytest
from fastapi import FastAPI

from services.ingest.synthetic.validation_runs.composition import (
    HMAC_SOURCES,
    _LiveCutoverDeps,
    _attach_cutover_state,
    _partition_probe_contract,
    _partition_recovery_candidates,
    _select_missing_recovery_month,
    live_target_for,
    seed_contract_live_only_targets,
)


_TENANT = UUID("aaaaaaaa-1111-7777-8888-bbbbbbbbbbbb")


def test_attach_cutover_state_threads_shared_dependencies() -> None:
    app = FastAPI()
    cutover = _LiveCutoverDeps(
        kafka_producer=object(),
        s3_raw_client=object(),
        tenant_flags=object(),
    )

    _attach_cutover_state(app, cutover)

    assert app.state.kafka_producer is cutover.kafka_producer
    assert app.state.s3_raw_client is cutover.s3_raw_client
    assert app.state.tenant_flags is cutover.tenant_flags


def test_signature_probe_membership_is_contract_derived() -> None:
    from services.ingest.source_contract.catalog import source_definition

    for source in HMAC_SOURCES:
        runtime = source_definition(source).certification.validation_runtime
        assert runtime is not None
        assert runtime.signature_probe_binding is not None


def test_meta_live_targets_use_exact_provider_scope() -> None:
    whatsapp = live_target_for(
        _TENANT,
        "whatsapp",
        "wa-acme",
        {"phone_number_id": "15551234567"},
    )
    facebook = live_target_for(
        _TENANT,
        "facebook_pages",
        "fb-acme",
        {},
    )

    assert whatsapp.whatsapp_phone_number_id == "15551234567"
    assert facebook.facebook_page_id == "x3-fb-acme-facebook_pages"


def test_whatsapp_live_target_requires_explicit_phone_scope() -> None:
    with pytest.raises(ValueError, match="explicit phone_number_id"):
        live_target_for(_TENANT, "whatsapp", "wa-acme", {})


class _RecordingPool:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> None:
        self.executions.append((query, args))


@pytest.mark.asyncio
async def test_live_only_target_bootstrap_is_contract_derived_and_secretless() -> None:
    pool = _RecordingPool()

    targets = await seed_contract_live_only_targets(  # type: ignore[arg-type]
        pool,
        tenants_per_source=2,
    )

    assert [target.source for target in targets] == ["whatsapp", "whatsapp"]
    assert len({target.tenant_id for target in targets}) == 2
    assert all(target.whatsapp_phone_number_id for target in targets)
    install_queries = [
        (query, args)
        for query, args in pool.executions
        if "INSERT INTO whatsapp_installations" in query
    ]
    assert len(install_queries) == 2
    for query, _args in install_queries:
        assert "app_secret_ref" in query
        assert "NULL, NULL, NULL, NULL, NULL, NULL, TRUE" in query
        assert "ON CONFLICT" not in query


def test_partition_probe_metadata_comes_from_every_source_contract() -> None:
    from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS

    resolved = {
        definition.source_id: _partition_probe_contract(
            definition.source_id,
        )
        for definition in SOURCE_DEFINITIONS
    }

    assert set(resolved) == {
        definition.source_id for definition in SOURCE_DEFINITIONS
    }
    for definition in SOURCE_DEFINITIONS:
        ingress, channel, trust, kind = resolved[definition.source_id]
        assert channel in definition.normalization_inputs
        assert ingress in {
            route.ingress_kind for route in definition.ingress_routes
        }
        assert trust == definition.default_trust_tier
        assert kind in definition.allowed_observation_kinds


def test_partition_recovery_candidates_are_distinct_and_in_guardrail() -> None:
    as_of = dt.datetime(2026, 7, 27, 8, 30, tzinfo=dt.timezone.utc)

    candidates = _partition_recovery_candidates(as_of=as_of)

    assert len(candidates) == 77
    assert len({(candidate.year, candidate.month) for candidate in candidates}) == 77
    assert candidates[0] == dt.datetime(
        2018,
        7,
        1,
        tzinfo=dt.timezone.utc,
    )
    assert candidates[-1] == dt.datetime(
        2024,
        11,
        1,
        tzinfo=dt.timezone.utc,
    )
    assert all(
        dt.timedelta(days=365)
        < as_of - candidate
        < dt.timedelta(days=3660)
        for candidate in candidates
    )


@pytest.mark.asyncio
async def test_partition_recovery_month_selection_never_drops_populated_data() -> None:
    candidates = (
        dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc),
        dt.datetime(2020, 3, 1, tzinfo=dt.timezone.utc),
    )

    class _PartitionPool:
        def __init__(self) -> None:
            self.partitions: dict[str, int] = {
                "observations_2020_01": 3,
                "observations_2020_02": 0,
            }
            self.dropped: list[str] = []

        async def fetchval(self, query: str, *args: object) -> object:
            if "to_regclass" in query:
                name = str(args[0])
                return name if name in self.partitions else None
            name = query.split('"')[1]
            return self.partitions[name]

        async def execute(self, query: str) -> None:
            name = query.split('"')[1]
            self.dropped.append(name)
            del self.partitions[name]

    pool = _PartitionPool()
    reserved: set[str] = set()

    occurred_at, partition = await _select_missing_recovery_month(
        pool,  # type: ignore[arg-type]
        candidates=candidates,
        reserved=reserved,
    )

    assert partition == "observations_2020_02"
    assert occurred_at == dt.datetime(2020, 2, 15, tzinfo=dt.timezone.utc)
    assert pool.partitions["observations_2020_01"] == 3
    assert pool.dropped == ["observations_2020_02"]
    assert reserved == {"observations_2020_02"}

    _, second_partition = await _select_missing_recovery_month(
        pool,  # type: ignore[arg-type]
        candidates=candidates,
        reserved=reserved,
    )
    assert second_partition == "observations_2020_03"
    assert pool.partitions["observations_2020_01"] == 3
