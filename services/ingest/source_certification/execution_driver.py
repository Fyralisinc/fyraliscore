"""Executable, fail-closed local stages for source certification.

This driver deliberately separates *measured local diagnostics* from release
certification.  It exercises the real Provider Lab ASGI boundary, its exact
source-owned routes, the shared ``ProviderTransport`` retry policy, quota
controls, and fault controls.  It does not promote those diagnostics into:

* a complete raw-evidence -> normalization -> Observation -> T1 proof;
* a 15-minute/60-minute throughput envelope;
* documented provider quota evidence; or
* a real-provider canary.

Those claims remain blocked until a source-specific executable supplies the
missing proof.  In particular, the canary stage never sends a request merely
because credentials happen to be present.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import math
import os
import re
import resource
import statistics
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

import httpx

from lib.shared.provider_transport import (
    ProviderTransientError,
    ProviderTransport,
    RequestContext,
)
from lib.shared.provider_transport.retry_after import rate_limited_from_headers
from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.distributed_transport_diagnostic import (
    run_distributed_transport_diagnostic_from_env,
)
from services.ingest.source_certification.io import write_certification_input
from services.ingest.source_certification.load_search import (
    LoadArtifact,
    LoadMeasurement,
    LoadMode,
    LoadSearchConfig,
    LoadTopology,
    Phase,
    VerifiedQuotaEvidence,
    compare_load_envelopes,
    run_artifact_load_search,
)
from services.ingest.source_certification.models import (
    CanaryOperationResult,
    CanaryResult,
    CertificationInput,
    CertificationState,
    ExecutableLoadOperation,
    LoadSuite,
    ScenarioResult,
    SourceCertificationSpec,
    SuiteKind,
    SuiteResult,
)
from services.ingest.source_certification.pipeline_load_runner import (
    PipelineAdapterFactory,
    declared_pipeline_workload_from_suite,
    diagnostic_pipeline_load_config_from_suite,
    run_pipeline_load,
    validate_pipeline_load_artifact,
    write_pipeline_load_artifact,
)
from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_TOPOLOGY_SCENARIO_IDS,
    pipeline_scenario_ids_for_source,
    run_pipeline_probe,
)
from services.ingest.source_certification.stage_artifacts import (
    CANARY_EXECUTION_SCHEMA_VERSION,
    STAGE_ARTIFACT_SCHEMA_VERSION,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    effective_request_policy,
    source_definition,
)
from services.ingest.source_certification.runtime import (
    resolve_fixture_count_oracle,
    resolve_fixture_factory,
    resolve_installation_seeder,
    resolve_live_fixture_factory,
)
from services.ingest.synthetic.provider_lab import (
    build_lab_adapter_registry,
    build_provider_lab_app,
)
from services.ingest.synthetic.provider_lab.calibration import (
    LabCalibrationConfig,
    calibrate_provider_lab,
)
from services.ingest.synthetic.provider_lab.protocol import (
    ProviderOperationBinding,
    ProviderRoute,
)
from services.ingest.synthetic.provider_lab.runtime import (
    QuotaConfiguration,
    QuotaRequirement,
)
from services.ingest.source_contract.runtime import resolve_callable_reference

Stage = Literal["local_correctness", "load", "fault_recovery", "canary"]
_STAGES: tuple[Stage, ...] = (
    "local_correctness",
    "load",
    "fault_recovery",
    "canary",
)
_EVIDENCE_FILE = "evidence-file:stage.json"
_PATH_PARAMETER_RE = re.compile(r"\{([^{}]+)\}")
_QUOTA_ENV = "FYRALIS_PROVIDER_QUOTAS_JSON"
_DEFAULT_LOAD_REQUESTS = 64
_DEFAULT_DIAGNOSTIC_OFFER_LIMIT = 4.0
_EXECUTION_PLAN_SCHEMA_VERSION = (
    "fyralis.source-certification-local-execution-plan.v1"
)

_SCENARIO_LOCAL_PROBES: Mapping[str, tuple[str, ...]] = {
    "auth_success_and_expiry": (
        "provider_lab_route_dispatch",
        "credential_free_callable_resolution",
    ),
    "exact_tenant_and_installation_resolution": (
        "exact_installation_identity",
        "per_tenant_observation_count",
        "cross_tenant_zero_leaks",
    ),
    "pagination_or_stream_resume": (
        "all_declared_routes_dispatched",
        "history_callable_resolution",
    ),
    "create_update_delete_or_declared_absence": (
        "provider_lab_state_machine_present",
        "source_history_declaration",
    ),
    "duplicate_delivery_and_idempotency": (
        "idempotency_builder_resolution",
        "observation_idempotency_replay",
    ),
    "out_of_order_delivery": (
        "deterministic_fixture",
    ),
    "provider_429_shared_cooldown": (
        "all_retry_safe_operations_429_retry",
    ),
    "provider_5xx_timeout_and_recovery": (
        "all_retry_safe_operations_503_retry",
    ),
    "no_cursor_advance_past_required_hydration_failure": (
        "history_callable_resolution",
    ),
    "raw_evidence_and_normalized_topic": (
        "normalizer_binding_resolution",
        "s3_raw_evidence_inspection",
        "raw_kafka_inspection",
        "normalized_kafka_inspection",
        "raw_replay_normalized_reprocessing",
    ),
    "observation_persistence_and_t1_trigger": (
        "normalizer_binding_resolution",
        "postgres_observation_exact_count",
        "observation_idempotency_replay",
        "same_tenant_exactly_one_t1",
    ),
    "two_replica_cross_tenant_isolation": (
        "durable_two_replica_participation",
        "cross_tenant_zero_leaks",
    ),
}

_SCENARIO_UNPROVEN_REQUIREMENTS: Mapping[str, tuple[str, ...]] = {
    "auth_success_and_expiry": (
        "provider-issued credential expiry and refresh were not executed",
    ),
    "exact_tenant_and_installation_resolution": (
        "the 2-tenant × 2-installation database-backed topology was not "
        "executed",
    ),
    "pagination_or_stream_resume": (
        "every provider-specific cursor/delta resume path was not executed",
    ),
    "create_update_delete_or_declared_absence": (
        "the source-specific lifecycle oracle was not executed",
    ),
    "duplicate_delivery_and_idempotency": (
        "duplicate records were not persisted through Observation uniqueness",
    ),
    "out_of_order_delivery": (
        "out-of-order events were not driven through the ingestion pipeline",
    ),
    "provider_429_shared_cooldown": (
        "a distributed Redis cooldown across two worker processes was not executed",
    ),
    "provider_5xx_timeout_and_recovery": (
        "timeout/disconnect recovery and backlog drain were not executed",
    ),
    "no_cursor_advance_past_required_hydration_failure": (
        "durable cursor state was not observed around a hydration failure",
    ),
    "raw_evidence_and_normalized_topic": (
        "S3 raw evidence and Kafka raw/normalized topics were not inspected",
    ),
    "observation_persistence_and_t1_trigger": (
        "Postgres Observation and exactly-one T1 trigger were not inspected",
    ),
    "two_replica_cross_tenant_isolation": (
        "two durable worker claims and cross-tenant pipeline isolation were "
        "not executed",
    ),
}


class ExecutionDriverError(RuntimeError):
    """The local stage cannot produce trustworthy evidence."""


@dataclasses.dataclass(frozen=True, slots=True)
class LoadStageOptions:
    """Execution controls for the artifact-producing load stage.

    The default is a fast virtual-clock diagnostic. Promotion callers must
    explicitly request wall-clock execution, the weekly soak, and the declared
    source timings. The Provider Lab-only implementation always records
    ``pipeline_e2e_proven=False`` unless an injected end-to-end runner proves
    otherwise.
    """

    initial_rate: float = 1.0
    offer_limit_rate: float = _DEFAULT_DIAGNOSTIC_OFFER_LIMIT
    clock_mode: Literal["wall", "virtual"] = "virtual"
    include_soak: bool = False
    promotion: bool = False
    diagnostic_duration_seconds: int = 1
    calibration_probe_seconds: float = 0.1
    calibration_concurrency: int = 4
    calibration_minimum_samples: int = 1
    client_timeout_seconds: float = 30.0
    maximum_requests_per_trial: int = 256

    def __post_init__(self) -> None:
        for name in (
            "initial_rate",
            "offer_limit_rate",
            "calibration_probe_seconds",
            "client_timeout_seconds",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                raise ValueError(f"{name} must be a finite positive number")
        if self.offer_limit_rate <= self.initial_rate:
            raise ValueError("offer_limit_rate must be greater than initial_rate")
        for name in (
            "diagnostic_duration_seconds",
            "calibration_concurrency",
            "calibration_minimum_samples",
            "maximum_requests_per_trial",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.clock_mode not in {"wall", "virtual"}:
            raise ValueError("clock_mode must be 'wall' or 'virtual'")
        if self.promotion and self.clock_mode != "wall":
            raise ValueError("promotion load must use wall-clock timing")
        if self.promotion and not self.include_soak:
            raise ValueError("promotion load must include the weekly soak")
        if self.promotion and self.calibration_probe_seconds < 30:
            raise ValueError(
                "promotion load requires at least 30 seconds of Provider Lab "
                "calibration",
            )

    def search_config(self, suite: LoadSuite) -> LoadSearchConfig:
        if self.promotion:
            return LoadSearchConfig.from_suite(
                suite,
                initial_rate=self.initial_rate,
            )
        seconds = self.diagnostic_duration_seconds
        return LoadSearchConfig(
            initial_rate=self.initial_rate,
            step_fraction=suite.step_percent / 100,
            tolerance_fraction=suite.search_tolerance_percent / 100,
            warmup_seconds=seconds,
            step_seconds=seconds,
            validation_seconds=seconds,
            soak_seconds=seconds,
            maximum_steps=20,
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _source_callable_bindings(source_id: str) -> tuple[dict[str, str], ...]:
    """Return every ingestion/certification callable owned by the source.

    This deliberately records contract references rather than maintaining a
    second source switch.  Resolution is executed separately so an import
    failure becomes source-confined diagnostic evidence.
    """

    source = source_definition(source_id)
    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    values: list[tuple[str, str | None]] = [
        *(
            (f"normalizer:{channel}", reference)
            for channel, reference in source.normalization_contracts()
        ),
        *(
            (f"idempotency:{index}", reference)
            for index, reference in enumerate(
                source.idempotency_builder_bindings,
                start=1,
            )
        ),
        ("history:planner", source.planner_binding),
        ("history:fetcher", source.fetcher_binding),
        ("history:reconciler", source.reconciler_binding),
        (
            "certification:fixture_factory",
            (
                spec.fixture_factory_binding.reference
                if spec.fixture_factory_binding is not None
                else None
            ),
        ),
        (
            "certification:live_fixture_factory",
            (
                spec.live_fixture_factory_binding.reference
                if spec.live_fixture_factory_binding is not None
                else None
            ),
        ),
        (
            "certification:fixture_count_oracle",
            (
                spec.fixture_count_oracle_binding.reference
                if spec.fixture_count_oracle_binding is not None
                else None
            ),
        ),
        (
            "certification:installation_seeder",
            (
                spec.installation_seeder_binding.reference
                if spec.installation_seeder_binding is not None
                else None
            ),
        ),
    ]
    if source.installation_adapter is not None:
        adapter = source.installation_adapter
        values.extend(
            (
                ("installation:loader", adapter.loader_binding),
                ("installation:status_loader", adapter.status_loader_binding),
                (
                    "installation:planner_client_builder",
                    adapter.planner_client_builder_binding,
                ),
                (
                    "installation:onboarding_failure",
                    adapter.onboarding_failure_binding,
                ),
            )
        )
    validation = source.certification.validation_runtime
    if validation is not None:
        for field in dataclasses.fields(validation):
            if field.name.endswith("_binding"):
                values.append(
                    (
                        f"validation:{field.name}",
                        getattr(validation, field.name),
                    )
                )
    for worker in source.live_runtime.workers:
        values.extend(
            (
                (
                    f"live:{worker.component_id}:launcher",
                    worker.launcher_binding,
                ),
                (
                    f"live:{worker.component_id}:dispatch",
                    worker.dispatch_binding,
                ),
            )
        )
    return tuple(
        {"role": role, "reference": reference}
        for role, reference in values
        if reference is not None
    )


def build_declared_execution_plan(source_id: str) -> dict[str, object]:
    """Build the exact source-specific local plan sealed into its binding."""

    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    source = source_definition(source_id)
    adapter = build_lab_adapter_registry().require(source_id)
    return {
        "schema_version": _EXECUTION_PLAN_SCHEMA_VERSION,
        "source_id": source_id,
        "spec_hash": spec.declaration_hash(),
        "history": source.history,
        "required_scenarios": [
            {
                "scenario_id": scenario_id,
                "local_probe_ids": list(
                    _SCENARIO_LOCAL_PROBES.get(
                        scenario_id,
                        ("source_specific_executor_absent",),
                    )
                ),
            }
            for scenario_id in spec.required_scenarios
        ],
        "load_suites": [
            {
                "kind": suite.kind,
                "workload": suite.execution_workload_dict(),
                "compatibility_operation_mix": list(suite.operation_mix),
                "tenants": suite.tenants,
                "installations_per_tenant": suite.installations_per_tenant,
                "replicas": suite.replicas,
                "warmup_seconds": suite.warmup_seconds,
                "stable_seconds": suite.stable_seconds,
                "weekly_soak_seconds": suite.weekly_soak_seconds,
                "step_percent": suite.step_percent,
                "search_tolerance_percent": suite.search_tolerance_percent,
            }
            for suite in spec.load_suites
        ],
        "callable_bindings": list(_source_callable_bindings(source_id)),
        "provider_lab_routes": [
            {
                "route_id": route.route_id,
                "path_template": route.path_template,
                "methods": list(route.methods),
                "operation_ids": list(route.operation_ids),
                "quota_bucket": route.quota_bucket,
                "transport": route.transport,
            }
            for route in adapter.routes
        ],
        "provider_lab_protocol_surfaces": [
            {
                "surface_id": surface.surface_id,
                "transport": surface.transport,
                "operation_ids": list(surface.operation_ids),
            }
            for surface in adapter.protocol_surfaces
        ],
        "live_transports": list(source.live_transports),
        "normalization_inputs": list(source.normalization_inputs),
        "idempotency_builder_bindings": list(
            source.idempotency_builder_bindings
        ),
    }


def declared_execution_plan_sha256(source_id: str) -> str:
    return _sha256(
        _canonical_json(
            build_declared_execution_plan(source_id),
        ).encode("utf-8")
    )


def _golden_fixture(source_id: str) -> Mapping[str, Any]:
    source = source_definition(source_id)
    factory = (
        resolve_fixture_factory(source_id)
        if source.history is not None
        else resolve_live_fixture_factory(source_id)
    )
    value = factory(
        fixture_params={},
        installation_id=f"certification-execution-{source_id}",
    )
    if not isinstance(value, Mapping):
        raise ExecutionDriverError(
            f"{source_id} certification fixture must be a mapping",
        )
    return value


def _fixture_and_binding_probe(source_id: str) -> dict[str, object]:
    """Execute deterministic source-owned fixture and callable prerequisites."""

    source = source_definition(source_id)
    installation_id = f"certification-execution-{source_id}"
    sibling_installation_id = f"{installation_id}-sibling"
    factory = (
        resolve_fixture_factory(source_id)
        if source.history is not None
        else resolve_live_fixture_factory(source_id)
    )
    fixture = factory(
        fixture_params={},
        installation_id=installation_id,
    )
    repeated = factory(
        fixture_params={},
        installation_id=installation_id,
    )
    sibling = factory(
        fixture_params={},
        installation_id=sibling_installation_id,
    )
    if not all(isinstance(value, Mapping) for value in (fixture, repeated, sibling)):
        raise ExecutionDriverError(
            f"{source_id} fixture factory returned a non-mapping value",
        )
    fixture_bytes = _canonical_json(fixture).encode("utf-8")
    repeated_bytes = _canonical_json(repeated).encode("utf-8")
    sibling_bytes = _canonical_json(sibling).encode("utf-8")

    observation_count: int | None = None
    count_oracle_deterministic: bool | None = None
    installation_seeder_resolved = False
    if source.history is not None:
        oracle = resolve_fixture_count_oracle(source_id)
        observation_count = oracle(fixture)
        repeated_count = oracle(repeated)
        count_oracle_deterministic = (
            isinstance(observation_count, int)
            and not isinstance(observation_count, bool)
            and observation_count > 0
            and observation_count == repeated_count
        )
        resolve_installation_seeder(source_id)
        installation_seeder_resolved = True

    binding_results: list[dict[str, object]] = []
    for declared in _source_callable_bindings(source_id):
        reference = declared["reference"]
        try:
            resolved = resolve_callable_reference(reference)
        except Exception as exc:  # noqa: BLE001 - preserve exact failed binding
            binding_results.append(
                {
                    **declared,
                    "state": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
        else:
            resolved_name = getattr(
                resolved,
                "__qualname__",
                getattr(resolved, "__name__", type(resolved).__qualname__),
            )
            binding_results.append(
                {
                    **declared,
                    "state": "resolved",
                    "resolved_name": (
                        f"{getattr(resolved, '__module__', type(resolved).__module__)}:"
                        f"{resolved_name}"
                    ),
                }
            )

    target_result: dict[str, object]
    validation = source.certification.validation_runtime
    if validation is None:
        target_result = {
            "state": "failed",
            "error": "source has no validation runtime declaration",
        }
    else:
        try:
            target_builder = resolve_callable_reference(
                validation.live_target_binding,
            )
            target = target_builder(
                uuid5(NAMESPACE_URL, f"fyralis-certification:{source_id}"),
                source_id,
                f"certification-{source_id}",
                dict(fixture),
            )
            target_values = dataclasses.asdict(target)
            target_result = {
                "state": "constructed",
                "non_null_fields": {
                    key: (
                        str(value)
                        if not isinstance(value, (str, int, float, bool))
                        else value
                    )
                    for key, value in target_values.items()
                    if value is not None
                },
            }
        except Exception as exc:  # noqa: BLE001 - diagnostic stays fail closed
            target_result = {
                "state": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }

    return {
        "fixture_factory_kind": (
            "historical" if source.history is not None else "live_only"
        ),
        "fixture_sha256": _sha256(fixture_bytes),
        "fixture_bytes": len(fixture_bytes),
        "deterministic_repeat": fixture_bytes == repeated_bytes,
        "sibling_fixture_sha256": _sha256(sibling_bytes),
        "sibling_fixture_is_distinct": fixture_bytes != sibling_bytes,
        "exact_observation_count": observation_count,
        "count_oracle_deterministic": count_oracle_deterministic,
        "installation_seeder_resolved": installation_seeder_resolved,
        "callable_bindings": binding_results,
        "all_callable_bindings_resolved": all(
            item["state"] == "resolved" for item in binding_results
        ),
        "live_target": target_result,
    }


def _route_path(
    route: ProviderRoute,
    binding: ProviderOperationBinding | None = None,
) -> str:
    """Build a deterministic route-matching path, not provider fixture data."""

    path_values = dict(binding.path_values) if binding is not None else {}

    def _replace(match: re.Match[str]) -> str:
        parameter = match.group(1).split(":", 1)[0]
        return path_values.get(parameter, "1")

    return _PATH_PARAMETER_RE.sub(_replace, route.path_template)


def _request_headers(
    source_id: str,
    *,
    scope: str | None = None,
) -> dict[str, str]:
    authorization = "Bearer provider-lab-certification"
    if source_id == "telegram":
        authorization = "Session provider-lab-certification"
    return {
        "Authorization": authorization,
        "X-Provider-Lab-Scope": (
            scope or f"certification-{source_id}"
        ),
    }


async def _provider_request(
    client: httpx.AsyncClient,
    *,
    source_id: str,
    route: ProviderRoute,
    method: str,
    scope: str | None = None,
    binding: ProviderOperationBinding | None = None,
    quota_requirements: Sequence[QuotaRequirement] = (),
) -> httpx.Response:
    if binding is not None:
        if method != binding.method:
            raise ExecutionDriverError(
                f"{binding.operation_id} requires {binding.method}, got {method}"
            )
        if route.binding_for(binding.operation_id) != binding:
            raise ExecutionDriverError(
                f"{binding.operation_id} is not bound to {route.route_id}"
            )
    kwargs: dict[str, Any] = {}
    if binding is not None and binding.body is not None:
        kwargs["content"] = binding.body
    elif method in {"POST", "PUT", "PATCH"}:
        kwargs["json"] = {}
    headers = _request_headers(source_id, scope=scope)
    if binding is not None:
        headers.update(dict(binding.headers))
    if quota_requirements:
        headers["X-Provider-Lab-Quota-Requirements"] = json.dumps(
            [
                {
                    "scope": requirement.scope,
                    "bucket": requirement.bucket,
                    "limit_id": requirement.limit_id,
                    "cost": float(requirement.cost),
                }
                for requirement in quota_requirements
            ],
            separators=(",", ":"),
            sort_keys=True,
        )
    params: list[tuple[str, str]] = [
        ("hub.challenge", "provider-lab-certification"),
        ("hub.mode", "subscribe"),
        ("hub.verify_token", "provider-lab-certification"),
    ]
    if binding is not None:
        params.extend(binding.query_items)
    return await client.request(
        method,
        f"/{source_id}{_route_path(route, binding)}",
        headers=headers,
        params=params,
        **kwargs,
    )


def _response_record(
    *,
    route: ProviderRoute,
    method: str,
    response: httpx.Response,
    ledger_count: int,
) -> dict[str, object]:
    return {
        "route_id": route.route_id,
        "method": method,
        "operation_ids": list(route.operation_ids),
        "status_code": response.status_code,
        "response_bytes": len(response.content),
        "response_sha256": _sha256(response.content),
        "ledger_count": ledger_count,
    }


async def _probe_used_surface(
    source_id: str,
) -> tuple[dict[str, object], Any]:
    """Exercise every HTTP route owned by one source through the ASGI app."""

    registry = build_lab_adapter_registry()
    adapter = registry.require(source_id)
    app = build_provider_lab_app(
        registry=registry,
        fixtures={source_id: [dict(_golden_fixture(source_id))]},
    )
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
        raise_app_exceptions=False,
    )
    route_results: list[dict[str, object]] = []
    scope_probe_results: list[dict[str, object]] = []
    scope_probe_values = tuple(
        f"tenant-{tenant}:installation-{installation}"
        for tenant in (1, 2)
        for installation in (1, 2)
    )
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
        timeout=30.0,
    ) as client:
        inventory_response = await client.get("/_lab/adapters")
        inventory_response.raise_for_status()
        inventory = inventory_response.json()
        source_inventory = [
            item
            for item in inventory["adapters"]
            if item["source"] == source_id
        ]
        if len(source_inventory) != 1:
            raise ExecutionDriverError(
                f"Provider Lab inventory has {len(source_inventory)} "
                f"entries for {source_id}",
            )

        for route in adapter.routes:
            for method in route.methods:
                response = await _provider_request(
                    client,
                    source_id=source_id,
                    route=route,
                    method=method,
                )
                ledger_count = len(
                    app.state.provider_lab.ledger.list(
                        source=source_id,
                        route_id=route.route_id,
                        limit=1_000,
                    )
                )
                if ledger_count < 1:
                    raise ExecutionDriverError(
                        f"{source_id}.{route.route_id}.{method} did not reach "
                        "the Provider Lab request ledger",
                    )
                route_results.append(
                    _response_record(
                        route=route,
                        method=method,
                        response=response,
                        ledger_count=ledger_count,
                    )
                )
                if response.status_code >= 500:
                    raise ExecutionDriverError(
                        f"{source_id}.{route.route_id}.{method} returned "
                        f"unexpected server error {response.status_code}",
                    )

        scope_route = adapter.routes[0]
        for scope in scope_probe_values:
            response = await _provider_request(
                client,
                source_id=source_id,
                route=scope_route,
                method=scope_route.methods[0],
                scope=scope,
            )
            if response.status_code >= 500:
                raise ExecutionDriverError(
                    f"{source_id} four-scope probe returned "
                    f"{response.status_code}",
                )
            scope_probe_results.append(
                {
                    "scope": scope,
                    "route_id": scope_route.route_id,
                    "status_code": response.status_code,
                }
            )

        unknown = await client.get(
            f"/{source_id}/__fyralis_certification_unknown_route__",
        )
        if unknown.status_code != 501:
            raise ExecutionDriverError(
                f"{source_id} unknown route returned {unknown.status_code}, "
                "expected strict 501",
            )

    ledger = app.state.provider_lab.ledger.list(
        source=source_id,
        limit=1_000,
    )
    foreign_sources = sorted(
        {
            str(item.get("source"))
            for item in ledger
            if item.get("source") != source_id
        }
    )
    if foreign_sources:
        raise ExecutionDriverError(
            f"{source_id} probe touched foreign sources: {foreign_sources}",
        )
    observed_scopes = {
        str(item["scope"])
        for item in ledger
        if item.get("scope") in scope_probe_values
    }
    if observed_scopes != set(scope_probe_values):
        raise ExecutionDriverError(
            f"{source_id} four-scope ledger mismatch: "
            f"{sorted(observed_scopes)}",
        )

    owned_operations = {
        operation_id
        for route in adapter.routes
        for operation_id in route.operation_ids
    } | {
        operation_id
        for surface in adapter.protocol_surfaces
        for operation_id in surface.operation_ids
    }
    expected_operations = set(
        source_definition(source_id).operation_policy_ids,
    )
    if owned_operations != expected_operations:
        raise ExecutionDriverError(
            f"{source_id} operation ownership mismatch: "
            f"missing={sorted(expected_operations - owned_operations)}, "
            f"unexpected={sorted(owned_operations - expected_operations)}",
        )

    return (
        {
            "inventory": source_inventory[0],
            "route_results": route_results,
            "protocol_surfaces": [
                {
                    "surface_id": surface.surface_id,
                    "transport": surface.transport,
                    "operation_ids": list(surface.operation_ids),
                    "execution_state": "owned_not_exercised_by_http_probe",
                }
                for surface in adapter.protocol_surfaces
            ],
            "unknown_route_status": unknown.status_code,
            "ledger_entries": len(ledger),
            "source_isolation": not foreign_sources,
            "four_scope_request_ledger": {
                "expected_scopes": list(scope_probe_values),
                "observed_scopes": sorted(observed_scopes),
                "results": scope_probe_results,
                "passed": observed_scopes == set(scope_probe_values),
            },
        },
        app,
    )


def _local_scenario_diagnostics(
    spec: SourceCertificationSpec,
    *,
    fixture_probe: Mapping[str, object],
    used_surface: Mapping[str, object],
    pipeline_probe: Mapping[str, object],
) -> tuple[
    list[dict[str, object]],
    dict[str, str],
    dict[str, CertificationState],
]:
    """Record prerequisites and only promote fully executed scenarios."""

    binding_rows = fixture_probe.get("callable_bindings")
    resolved_roles = (
        {
            str(item["role"])
            for item in binding_rows
            if isinstance(item, Mapping)
            and item.get("state") == "resolved"
        }
        if isinstance(binding_rows, list)
        else set()
    )
    measured = {
        "provider_lab_route_dispatch",
        "all_declared_routes_dispatched",
        "provider_lab_state_machine_present",
        "source_history_declaration",
    }
    if fixture_probe.get("deterministic_repeat") is True:
        measured.add("deterministic_fixture")
    if fixture_probe.get("sibling_fixture_is_distinct") is True:
        measured.add("sibling_fixture_identity")
    if fixture_probe.get("installation_seeder_resolved") is True:
        measured.add("installation_seeder_binding_resolution")
    if fixture_probe.get("all_callable_bindings_resolved") is True:
        measured.add("credential_free_callable_resolution")
    if any(role.startswith("normalizer:") for role in resolved_roles):
        measured.add("normalizer_binding_resolution")
    if any(role.startswith("idempotency:") for role in resolved_roles):
        measured.add("idempotency_builder_resolution")
    if all(
        role in resolved_roles
        for role in (
            "history:planner",
            "history:fetcher",
            "history:reconciler",
        )
    ):
        measured.add("history_callable_resolution")
    four_scope = used_surface.get("four_scope_request_ledger")
    if isinstance(four_scope, Mapping) and four_scope.get("passed") is True:
        measured.add("four_scope_request_ledger")

    pipeline_state = pipeline_probe.get("state")
    certified_raw = pipeline_probe.get("certified_scenarios")
    certified_pipeline_scenarios = (
        {
            str(value)
            for value in certified_raw
            if isinstance(value, str)
        }
        if isinstance(certified_raw, (list, tuple, set, frozenset))
        else set()
    )
    expected_pipeline_scenarios = pipeline_scenario_ids_for_source(
        spec.source_id,
    )
    if (
        pipeline_state == "passed"
        and certified_pipeline_scenarios == expected_pipeline_scenarios
    ):
        measured.update(
            {
                "s3_raw_evidence_inspection",
                "raw_kafka_inspection",
                "normalized_kafka_inspection",
                "raw_replay_normalized_reprocessing",
                "postgres_observation_exact_count",
                "observation_idempotency_replay",
                "same_tenant_exactly_one_t1",
            }
        )
        if PIPELINE_TOPOLOGY_SCENARIO_IDS.issubset(
            certified_pipeline_scenarios,
        ):
            measured.update(
                {
                    "exact_installation_identity",
                    "per_tenant_observation_count",
                    "cross_tenant_zero_leaks",
                    "durable_two_replica_participation",
                },
            )

    rows: list[dict[str, object]] = []
    failures: dict[str, str] = {}
    states: dict[str, CertificationState] = {}
    for scenario_id in spec.required_scenarios:
        declared = _SCENARIO_LOCAL_PROBES.get(
            scenario_id,
            ("source_specific_executor_absent",),
        )
        measured_for_scenario = tuple(
            probe for probe in declared if probe in measured
        )
        unmeasured = tuple(
            probe for probe in declared if probe not in measured
        )
        unproven = _SCENARIO_UNPROVEN_REQUIREMENTS.get(
            scenario_id,
            (
                "no committed source-specific executable exists for this "
                "special scenario",
            ),
        )
        if (
            spec.source_id == "whatsapp"
            and scenario_id in PIPELINE_TOPOLOGY_SCENARIO_IDS
        ):
            unproven = (
                "live-only topology blocker: WhatsApp has no historical "
                "onboarding work, and the current Kafka normalizer/writer "
                "path exposes no durable per-replica work attribution",
            )
        if (
            scenario_id in expected_pipeline_scenarios
            and pipeline_state == "passed"
            and scenario_id in certified_pipeline_scenarios
            and not unmeasured
        ):
            states[scenario_id] = "passed"
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "certification_state": "passed",
                    "declared_probe_ids": list(declared),
                    "measured_probe_ids": list(measured_for_scenario),
                    "unmeasured_probe_ids": [],
                    "unproven_requirements": [],
                }
            )
            continue
        if (
            scenario_id in expected_pipeline_scenarios
            and pipeline_state == "failed"
        ):
            reason = (
                f"{scenario_id} failed: the isolated data-plane probe "
                f"reported {pipeline_probe.get('error', 'execution failed')}"
            )
            states[scenario_id] = "failed"
            failures[scenario_id] = reason
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "certification_state": "failed",
                    "declared_probe_ids": list(declared),
                    "measured_probe_ids": list(measured_for_scenario),
                    "unmeasured_probe_ids": list(unmeasured),
                    "unproven_requirements": [reason],
                }
            )
            continue
        reason = (
            f"{scenario_id} remains blocked: "
            + "; ".join(unproven)
        )
        states[scenario_id] = "blocked"
        failures[scenario_id] = reason
        rows.append(
            {
                "scenario_id": scenario_id,
                "certification_state": "blocked",
                "declared_probe_ids": list(declared),
                "measured_probe_ids": list(measured_for_scenario),
                "unmeasured_probe_ids": list(unmeasured),
                "unproven_requirements": list(unproven),
            }
        )
    return rows, failures, states


def _blocked_suites(
    *,
    reason: str,
    metrics: Mapping[str, float] | None = None,
) -> tuple[SuiteResult, ...]:
    metric_values = tuple(sorted((metrics or {}).items()))
    kinds: tuple[SuiteKind, ...] = (
        "historical",
        "live",
        "combined",
    )
    return tuple(
        SuiteResult(
            kind=kind,
            state="blocked",
            artifact_uri=_EVIDENCE_FILE,
            metrics=metric_values,
            failures=(reason,),
        )
        for kind in kinds
    )


def _base_input(
    spec: SourceCertificationSpec,
    *,
    reason: str,
    scenario_failures: Mapping[str, str] | None = None,
    scenario_states: Mapping[str, CertificationState] | None = None,
    local_correctness: Literal["blocked", "failed"] = "blocked",
) -> CertificationInput:
    suites = _blocked_suites(reason=reason)
    return CertificationInput(
        spec_hash=spec.declaration_hash(),
        local_correctness=local_correctness,
        local_correctness_artifact=_EVIDENCE_FILE,
        scenario_results=tuple(
            ScenarioResult(
                scenario_id=scenario_id,
                state=(scenario_states or {}).get(
                    scenario_id,
                    "blocked",
                ),
                artifact_uri=_EVIDENCE_FILE,
                failures=(
                    ()
                    if (scenario_states or {}).get(scenario_id) == "passed"
                    else (
                        (scenario_failures or {}).get(
                            scenario_id,
                            reason,
                        ),
                    )
                ),
            )
            for scenario_id in spec.required_scenarios
        ),
        provider_safe_suites=suites,
        fyralis_ceiling_suites=suites,
        fault_recovery_suites=suites,
        canary=CanaryResult(
            state="blocked",
            operation_results=tuple(
                CanaryOperationResult(
                    operation_id=operation_id,
                    state="blocked",
                    artifact_uri=_EVIDENCE_FILE,
                    failures=(reason,),
                )
                for operation_id in spec.canary.required_operations
            ),
            artifact_uri=_EVIDENCE_FILE,
            failures=(reason,),
        ),
        legacy_reference_count=0,
    )


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(
        0,
        min(
            len(ordered) - 1,
            math.ceil(percentile * len(ordered)) - 1,
        ),
    )
    return ordered[index]


def _representative_route(
    source_id: str,
) -> tuple[ProviderRoute, str] | None:
    adapter = build_lab_adapter_registry().require(source_id)
    candidates: list[tuple[ProviderRoute, str]] = []
    for route in adapter.routes:
        for operation_id in route.operation_ids:
            policy = effective_request_policy(source_id, operation_id)
            if policy.max_attempts > 1 and route.quota_bucket is not None:
                candidates.append((route, operation_id))
    return candidates[0] if candidates else None


def _retryable_route_operations(
    source_id: str,
) -> tuple[tuple[ProviderRoute, str], ...]:
    adapter = build_lab_adapter_registry().require(source_id)
    return tuple(
        (route, operation_id)
        for route in adapter.routes
        if route.quota_bucket is not None
        for operation_id in route.operation_ids
        if effective_request_policy(
            source_id,
            operation_id,
        ).max_attempts > 1
    )


async def _transport_request(
    *,
    client: httpx.AsyncClient,
    source_id: str,
    route: ProviderRoute,
    operation_id: str,
) -> int:
    policy = effective_request_policy(source_id, operation_id)
    binding = route.binding_for(operation_id)
    response = await _provider_request(
        client,
        source_id=source_id,
        route=route,
        method=binding.method,
        binding=binding,
    )
    if response.status_code == 429:
        raise rate_limited_from_headers(
            response.headers,
            message="Provider Lab injected a rate limit",
            status_code=response.status_code,
            header_parser_id=(
                policy.rate_limit_header_parser_id or "http.retry_after"
            ),
        )
    if response.status_code in {408, 425, 500, 502, 503, 504}:
        raise ProviderTransientError(
            "Provider Lab injected a retryable response",
            status_code=response.status_code,
            source=source_id,
            operation=operation_id,
        )
    return response.status_code


async def _run_transport_call(
    *,
    client: httpx.AsyncClient,
    source_id: str,
    route: ProviderRoute,
    operation_id: str,
    transport: ProviderTransport | None = None,
) -> int:
    async def _no_sleep(_delay: float) -> None:
        return None

    executor = transport or ProviderTransport(
        sleep=_no_sleep,
        random_fn=lambda: 0.0,
    )
    return await executor.execute(
        RequestContext(
            source=source_id,
            operation=operation_id,
            tenant_id=f"certification-{source_id}-tenant",
            installation_id=f"certification-{source_id}-installation",
        ),
        effective_request_policy(source_id, operation_id),
        lambda: _transport_request(
            client=client,
            source_id=source_id,
            route=route,
            operation_id=operation_id,
        ),
    )


def _load_quota_config(
    source_id: str,
    route: ProviderRoute,
    *,
    ambient_env: Mapping[str, str],
) -> tuple[QuotaConfiguration | None, float | None, str]:
    routes = build_lab_adapter_registry().require(source_id).routes
    evidence, note = _load_quota_evidence(
        source_id,
        routes,
        ambient_env=ambient_env,
    )
    matching = tuple(
        item for item in evidence if item.bucket == route.quota_bucket
    )
    if len(matching) != 1:
        return None, None, (
            note
            if not matching
            else (
                "the finite provider-safe diagnostic supports one constraint; "
                "the offered-load runner owns simultaneous quota evidence"
            )
        )
    item = matching[0]
    return (
        QuotaConfiguration(
            source=source_id,
            scope=item.scope,
            bucket=item.bucket,
            limit_id=item.limit_id,
            mode="enforce",
            capacity=item.capacity,
            refill_per_second=item.refill_per_second,
        ),
        item.cost,
        (
            "operator supplied an evidence-labelled budget; the diagnostic "
            "does not independently verify that provider evidence"
        ),
    )


def _load_quota_evidence(
    source_id: str,
    routes: Sequence[ProviderRoute],
    *,
    ambient_env: Mapping[str, str],
) -> tuple[tuple[VerifiedQuotaEvidence, ...], str]:
    """Load exact, evidence-labelled budgets for every exercised quota bucket.

    A single object remains accepted for one-constraint providers. Providers
    subject to simultaneous app/workspace/route limits or several time windows
    supply one item per independent constraint. Missing buckets, extra buckets,
    duplicate constraint identities, or partially labelled budgets block the
    provider-safe envelope.
    """

    raw = ambient_env.get(_QUOTA_ENV)
    if not raw:
        return (), (
            f"{_QUOTA_ENV} has no evidence-backed budget for {source_id}; "
            "provider-safe offered-load search was not attempted"
        )
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        return (), f"{_QUOTA_ENV} is not valid JSON: {exc}"
    if not isinstance(decoded, Mapping):
        return (), f"{_QUOTA_ENV} must be an object keyed by source"
    source_value = decoded.get(source_id)
    if isinstance(source_value, Mapping):
        entries: Sequence[object] = (source_value,)
    elif (
        isinstance(source_value, Sequence)
        and not isinstance(source_value, (str, bytes, bytearray))
    ):
        entries = source_value
    else:
        return (), (
            f"{_QUOTA_ENV}.{source_id} must be an exact quota object or "
            "a list of exact quota objects"
        )

    expected_fields = {
        "bucket",
        "capacity",
        "cost",
        "refill_per_second",
        "scope",
        "limit_id",
        "evidence_uri",
        "verified_at",
    }
    evidence: list[VerifiedQuotaEvidence] = []
    errors: list[str] = []
    for index, raw_entry in enumerate(entries):
        prefix = f"{_QUOTA_ENV}.{source_id}[{index}]"
        if not isinstance(raw_entry, Mapping):
            errors.append(f"{prefix} must be an object")
            continue
        if set(raw_entry) != expected_fields:
            errors.append(
                f"{prefix} fields must equal {sorted(expected_fields)}",
            )
            continue
        try:
            verified_at = datetime.fromisoformat(
                str(raw_entry["verified_at"]),
            )
            item = VerifiedQuotaEvidence(
                bucket=str(raw_entry["bucket"]),
                scope=str(raw_entry["scope"]),
                limit_id=str(raw_entry["limit_id"]),
                cost=raw_entry["cost"],  # type: ignore[arg-type]
                capacity=raw_entry["capacity"],  # type: ignore[arg-type]
                refill_per_second=raw_entry[
                    "refill_per_second"
                ],  # type: ignore[arg-type]
                evidence_uri=str(raw_entry["evidence_uri"]),
                verified_at=verified_at,
            )
        except (TypeError, ValueError) as exc:
            errors.append(f"{prefix} is invalid: {exc}")
            continue
        evidence.append(item)

    expected_buckets = {
        route.quota_bucket
        for route in routes
        if route.quota_bucket is not None
    }
    actual_buckets = {item.bucket for item in evidence}
    identities = [
        (item.bucket, item.scope.casefold(), item.limit_id)
        for item in evidence
    ]
    duplicate_constraints = sorted(
        {
            identity
            for identity in identities
            if identities.count(identity) > 1
        }
    )
    if duplicate_constraints:
        errors.append(
            "quota evidence contains duplicate constraints: "
            + ", ".join(
                f"{bucket}/{scope}/{limit_id}"
                for bucket, scope, limit_id in duplicate_constraints
            ),
        )
    missing = sorted(expected_buckets - actual_buckets)
    extra = sorted(actual_buckets - expected_buckets)
    if missing:
        errors.append(
            "quota evidence is missing exercised buckets: "
            + ", ".join(missing),
        )
    if extra:
        errors.append(
            "quota evidence contains unexercised buckets: "
            + ", ".join(extra),
        )
    if errors:
        return (), "; ".join(errors)
    if not evidence:
        return (), (
            f"{source_id} has no quota-bearing HTTP route to model for "
            "provider-safe offered-load search"
        )
    return (
        tuple(
            sorted(
                evidence,
                key=lambda item: (
                    item.bucket,
                    item.scope.casefold(),
                    item.limit_id,
                ),
            )
        ),
        (
            "all exercised Provider Lab quota buckets have exact, independent "
            "evidence-labelled constraints"
        ),
    )


@dataclasses.dataclass(frozen=True, slots=True)
class _ProviderLabHttpOperation:
    """One strict Provider Lab request template.

    This is deliberately separate from an executable load operation.  The Lab
    diagnostic can exercise a provider route, but only the typed pipeline
    runner can invoke the declared Fyralis binding and produce a receipt.
    """

    route: ProviderRoute
    method: str
    operation_id: str | None
    binding: ProviderOperationBinding | None = None

    @property
    def label(self) -> str:
        return (
            self.operation_id
            or f"{self.route.route_id}:{self.method.casefold()}"
        )


@dataclasses.dataclass(frozen=True, slots=True)
class _OfferedOperation:
    """Typed data work paired with one Provider Lab HTTP template.

    The pairing is diagnostic route coverage only.  It must never be confused
    with execution of ``load_operation.executable_binding``.
    """

    load_operation: ExecutableLoadOperation
    provider_operation: _ProviderLabHttpOperation

    @property
    def route(self) -> ProviderRoute:
        return self.provider_operation.route

    @property
    def method(self) -> str:
        return self.provider_operation.method

    @property
    def operation_id(self) -> str | None:
        return self.provider_operation.operation_id

    @property
    def binding(self) -> ProviderOperationBinding | None:
        return self.provider_operation.binding

    @property
    def label(self) -> str:
        return self.provider_operation.label


@dataclasses.dataclass(frozen=True, slots=True)
class _LoadLane:
    tenant: int
    installation: int
    replica: int

    @property
    def installation_scope(self) -> str:
        return (
            f"tenant-{self.tenant}:installation-{self.installation}"
        )

    @property
    def replica_label(self) -> str:
        return f"replica-{self.replica}"


def _provider_lab_http_operation_plan(
    source_id: str,
) -> tuple[_ProviderLabHttpOperation, ...]:
    adapter = build_lab_adapter_registry().require(source_id)
    plan: list[_ProviderLabHttpOperation] = []
    for route in adapter.routes:
        if route.operation_ids:
            plan.extend(
                _ProviderLabHttpOperation(
                    route=route,
                    method=binding.method,
                    operation_id=operation_id,
                    binding=binding,
                )
                for operation_id in route.operation_ids
                for binding in (route.binding_for(operation_id),)
            )
            continue
        for method in route.methods:
            plan.extend(
                (
                    _ProviderLabHttpOperation(
                        route=route,
                        method=method,
                        operation_id=None,
                    ),
                )
            )
    if not plan:
        raise ExecutionDriverError(
            f"{source_id} has no HTTP operation for Provider Lab diagnostics",
        )
    return tuple(plan)


def _offered_operation_plan(
    source_id: str,
    suite: LoadSuite,
) -> tuple[_OfferedOperation, ...]:
    """Pair every declared data operation with strict HTTP diagnostics.

    The route pairing preserves full Provider Lab used-surface coverage but
    does not claim a typed callable ran.  ``run_pipeline_load`` remains the
    only execution path allowed to emit durable typed operation receipts.
    """

    if suite.non_applicability is not None:
        raise ExecutionDriverError(
            f"{source_id}.{suite.kind} is explicitly not applicable",
        )
    http_operations = _provider_lab_http_operation_plan(source_id)
    plan = tuple(
        _OfferedOperation(
            load_operation=load_operation,
            provider_operation=provider_operation,
        )
        for load_operation in suite.data_operations
        for provider_operation in http_operations
    )
    if not plan:
        raise ExecutionDriverError(
            f"{source_id}.{suite.kind} has no typed data operation for "
            "Provider Lab diagnostics",
        )
    return plan


def _load_lanes(
    topology: LoadTopology,
) -> tuple[_LoadLane, ...]:
    """Return installation quota scopes paired with rotating replica labels."""

    return tuple(
        _LoadLane(
            tenant=tenant,
            installation=installation,
            replica=replica,
        )
        for replica in range(1, topology.replicas + 1)
        for tenant in range(1, topology.tenants + 1)
        for installation in range(1, topology.installations_per_tenant + 1)
    )


def _quota_scope_key(
    *,
    source_id: str,
    evidence: VerifiedQuotaEvidence,
    lane: _LoadLane,
    operation: _OfferedOperation,
) -> str:
    """Materialize the documented quota identity for one exact request."""

    parts = [f"source={source_id}"]
    for component in evidence.scope.casefold().split("/"):
        if component in {"app", "application", "global"}:
            parts.append(f"{component}={source_id}")
        elif component == "region":
            parts.append("region=provider-lab-default")
        elif component == "tenant":
            parts.append(f"tenant={lane.tenant}")
        elif component in {
            "installation",
            "realm",
            "user",
            "workspace",
        }:
            parts.append(
                f"{component}={lane.tenant}.{lane.installation}",
            )
        elif component == "route":
            parts.append(f"route={operation.route.route_id}")
        elif component == "method":
            parts.append(f"method={operation.method}")
        else:  # VerifiedQuotaEvidence rejects this before execution.
            raise ExecutionDriverError(
                f"unsupported quota scope component {component!r}",
            )
    scope = ";".join(parts)
    if len(scope) > 256:
        raise ExecutionDriverError(
            f"modeled quota scope exceeds Provider Lab limit: {scope!r}",
        )
    return scope


class _ProviderLabOfferedLoadRunner:
    """Drive the complete HTTP surface for one declared workload envelope."""

    def __init__(
        self,
        *,
        source_id: str,
        suite: LoadSuite,
        topology: LoadTopology,
        options: LoadStageOptions,
        app: Any,
        quota_evidence: tuple[VerifiedQuotaEvidence, ...],
    ) -> None:
        self.source_id = source_id
        self.suite = suite
        self.topology = topology
        self.options = options
        self.app = app
        self.quota_evidence = quota_evidence
        self.operation_plan = _offered_operation_plan(source_id, suite)
        self.lanes = _load_lanes(topology)

    def _prepare_trial(self, mode: LoadMode) -> None:
        runtime = self.app.state.provider_lab
        runtime.reset_source_state(self.source_id)
        runtime.ledger.clear()
        runtime.quotas.clear()
        if mode != "provider_safe":
            return
        for evidence in self.quota_evidence:
            operations = tuple(
                operation
                for operation in self.operation_plan
                if operation.route.quota_bucket == evidence.bucket
            )
            scopes = {
                _quota_scope_key(
                    source_id=self.source_id,
                    evidence=evidence,
                    lane=lane,
                    operation=operation,
                )
                for lane in self.lanes
                for operation in operations
            }
            for scope in sorted(scopes):
                runtime.quotas.configure(
                    QuotaConfiguration(
                        source=self.source_id,
                        scope=scope,
                        bucket=evidence.bucket,
                        limit_id=evidence.limit_id,
                        mode="enforce",
                        capacity=evidence.capacity,
                        refill_per_second=evidence.refill_per_second,
                    )
                )

    def _quota_requirements(
        self,
        *,
        operation: _OfferedOperation,
        lane: _LoadLane,
        mode: LoadMode,
    ) -> tuple[QuotaRequirement, ...]:
        if mode != "provider_safe":
            return ()
        return tuple(
            QuotaRequirement(
                scope=_quota_scope_key(
                    source_id=self.source_id,
                    evidence=evidence,
                    lane=lane,
                    operation=operation,
                ),
                bucket=evidence.bucket,
                limit_id=evidence.limit_id,
                cost=evidence.cost,
            )
            for evidence in self.quota_evidence
            if evidence.bucket == operation.route.quota_bucket
        )

    async def __call__(
        self,
        offered_rate: float,
        duration_seconds: int,
        _phase: Phase,
        mode: LoadMode,
    ) -> LoadMeasurement:
        if offered_rate > self.options.offer_limit_rate:
            raise ExecutionDriverError(
                "offered-load search reached its safety cap before finding a "
                "measured unstable point; no maximum throughput may be claimed "
                f"above {self.options.offer_limit_rate:g} requests/second"
            )
        self._prepare_trial(mode)
        requested_count = max(1, math.ceil(offered_rate * duration_seconds))
        request_count = (
            requested_count
            if self.options.clock_mode == "wall"
            else min(
                requested_count,
                self.options.maximum_requests_per_trial,
            )
        )
        status_counts: dict[str, int] = {}
        operation_counts: dict[str, int] = {}
        for operation in self.suite.control_operations:
            scheduled_count = (
                1
                if operation.execution_frequency == "once_per_trial"
                else math.ceil(
                    duration_seconds / (operation.cadence_seconds or 1.0),
                )
            )
            operation_counts[
                f"scheduled_control_operation:{operation.operation_id}"
            ] = scheduled_count
        latencies_ms: list[float] = []
        response_bytes = 0
        quota_units = 0.0
        cpu_started = time.process_time()
        wall_started = time.perf_counter()
        transport = httpx.ASGITransport(
            app=self.app,
            client=("127.0.0.1", 43123),
            raise_app_exceptions=False,
        )
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://provider-lab",
            timeout=self.options.client_timeout_seconds,
        ) as client:
            for index in range(request_count):
                if self.options.clock_mode == "wall":
                    target = wall_started + index / offered_rate
                    delay = target - time.perf_counter()
                    if delay > 0:
                        await asyncio.sleep(delay)
                else:
                    self.app.state.provider_lab.clock.advance(
                        seconds=duration_seconds / request_count,
                    )

                operation = self.operation_plan[
                    index % len(self.operation_plan)
                ]
                lane = self.lanes[index % len(self.lanes)]
                requirements = self._quota_requirements(
                    operation=operation,
                    lane=lane,
                    mode=mode,
                )
                scope = lane.installation_scope
                request_started = time.perf_counter()
                response = await _provider_request(
                    client,
                    source_id=self.source_id,
                    route=operation.route,
                    method=operation.method,
                    scope=scope,
                    binding=operation.binding,
                    quota_requirements=requirements,
                )
                latency_ms = (
                    time.perf_counter() - request_started
                ) * 1_000
                latencies_ms.append(latency_ms)
                response_bytes += len(response.content)
                status_label = str(response.status_code)
                status_counts[status_label] = (
                    status_counts.get(status_label, 0) + 1
                )
                labels = [
                    # This Provider Lab boundary runner schedules a typed
                    # data operation but never invokes its declared callable.
                    # Only the pipeline runner may emit ``executed_*``
                    # evidence consumed by promotion.
                    "scheduled_data_operation:"
                    f"{operation.load_operation.operation_id}",
                    lane.replica_label,
                    f"route:{operation.route.route_id}",
                ]
                if response.status_code < 400:
                    labels.append(operation.label)
                for label in labels:
                    operation_counts[label] = (
                        operation_counts.get(label, 0) + 1
                    )
                if (
                    response.status_code < 400
                    and operation.route.quota_bucket is not None
                ):
                    quota_units += (
                        sum(requirement.cost for requirement in requirements)
                        if requirements
                        else operation.route.quota_cost
                    )

        if self.options.clock_mode == "wall":
            remaining = duration_seconds - (
                time.perf_counter() - wall_started
            )
            if remaining > 0:
                await asyncio.sleep(remaining)
        wall_elapsed = time.perf_counter() - wall_started
        cpu_elapsed = max(0.0, time.process_time() - cpu_started)
        measurement_window = (
            wall_elapsed
            if self.options.clock_mode == "wall"
            else float(duration_seconds)
        )
        achieved_rate = request_count / max(measurement_window, 1e-9)
        backlog_growth = max(0.0, offered_rate - achieved_rate)
        rate_limited = status_counts.get("429", 0)
        server_errors = sum(
            count
            for status, count in status_counts.items()
            if int(status) >= 500
        )
        client_errors = sum(
            count
            for status, count in status_counts.items()
            if 400 <= int(status) < 500 and int(status) != 429
        )
        stable = (
            rate_limited == 0
            and client_errors == 0
            and server_errors == 0
            and achieved_rate >= offered_rate * 0.99
        )
        limiting_component = (
            "provider_quota"
            if rate_limited
            else "provider_lab_request_error"
            if client_errors
            else "provider_lab_error"
            if server_errors
            else "provider_lab_capacity"
            if backlog_growth > 0
            else "provider_lab_boundary"
        )
        memory_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return LoadMeasurement(
            offered_rate=offered_rate,
            duration_seconds=duration_seconds,
            stable=stable,
            requests_per_second=achieved_rate,
            quota_units_per_second=quota_units / measurement_window,
            records_per_second=0.0,
            bytes_per_second=response_bytes / measurement_window,
            p50_latency_ms=statistics.median(latencies_ms),
            p95_latency_ms=_percentile(latencies_ms, 0.95),
            p99_latency_ms=_percentile(latencies_ms, 0.99),
            kafka_lag=0.0,
            observation_p99_latency_ms=0.0,
            backlog_growth_per_second=backlog_growth,
            missing_records=0,
            unexpected_duplicates=0,
            cross_tenant_leaks=0,
            cooldown_violations=0,
            cursor_consistency_errors=0,
            dlq_entries=0,
            cpu_percent=(
                cpu_elapsed / max(wall_elapsed, 1e-9) * 100
            ),
            memory_bytes=int(memory_kib * 1024),
            limiting_component=limiting_component,
            retries=0,
            rate_limited_responses=rate_limited,
            hot_loops=0,
            wall_elapsed_seconds=wall_elapsed,
            request_count=request_count,
            response_bytes=response_bytes,
            operation_counts=tuple(sorted(operation_counts.items())),
            status_counts=tuple(sorted(status_counts.items())),
        )


async def _calibrate_offered_load_runner(
    runner: _ProviderLabOfferedLoadRunner,
) -> Any:
    """Calibrate every typed-data × Provider Lab HTTP diagnostic case."""

    runtime = runner.app.state.provider_lab
    runtime.reset_source_state(runner.source_id)
    runtime.quotas.clear()
    runtime.ledger.clear()
    cases = runner.operation_plan
    if not cases:
        raise ExecutionDriverError(
            f"{runner.source_id} has no typed Provider Lab calibration cases"
        )
    expected_cases = {
        (operation.load_operation.operation_id, operation.label)
        for operation in cases
    }
    successful_cases: set[tuple[str, str]] = set()
    next_case = 0
    transport = httpx.ASGITransport(
        app=runner.app,
        client=("127.0.0.1", 43123),
        raise_app_exceptions=False,
    )
    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://provider-lab",
            timeout=runner.options.client_timeout_seconds,
        ) as client:
            # First prove each request template is provider-valid. This also
            # establishes deterministic state required by a later operation
            # (for example Signal subscribe -> SSE replay).
            for index, operation in enumerate(runner.operation_plan):
                lane = runner.lanes[index % len(runner.lanes)]
                response = await _provider_request(
                    client,
                    source_id=runner.source_id,
                    route=operation.route,
                    method=operation.method,
                    scope=lane.installation_scope,
                    binding=operation.binding,
                )
                if response.status_code >= 400:
                    raise ExecutionDriverError(
                        f"{runner.source_id} operation {operation.label!r} "
                        "has no successful exact Provider Lab request "
                        f"semantics (HTTP {response.status_code})"
                    )

            async def _probe() -> bool:
                nonlocal next_case
                index = next_case
                next_case += 1
                operation = cases[index % len(cases)]
                lane = runner.lanes[index % len(runner.lanes)]
                response = await _provider_request(
                    client,
                    source_id=runner.source_id,
                    route=operation.route,
                    method=operation.method,
                    scope=lane.installation_scope,
                    binding=operation.binding,
                )
                accepted = response.status_code < 400
                if accepted:
                    successful_cases.add(
                        (
                            operation.load_operation.operation_id,
                            operation.label,
                        ),
                    )
                return accepted

            calibration = await calibrate_provider_lab(
                _probe,
                config=LabCalibrationConfig(
                    target_fyralis_rps=runner.options.offer_limit_rate,
                    client_timeout_seconds=(
                        runner.options.client_timeout_seconds
                    ),
                    probe_seconds=runner.options.calibration_probe_seconds,
                    concurrency=runner.options.calibration_concurrency,
                    minimum_samples=max(
                        runner.options.calibration_minimum_samples,
                        len(cases),
                    ),
                ),
            )
        missing_cases = sorted(expected_cases - successful_cases)
        if missing_cases:
            preview = ", ".join(
                f"{load_operation}/{provider_operation}"
                for load_operation, provider_operation in missing_cases[:8]
            )
            suffix = (
                f" (+{len(missing_cases) - 8} more)"
                if len(missing_cases) > 8
                else ""
            )
            raise ExecutionDriverError(
                "Provider Lab calibration did not successfully exercise every "
                f"typed-data/provider-operation case: {preview}{suffix}"
            )
        return calibration
    finally:
        runtime.reset_source_state(runner.source_id)
        runtime.quotas.clear()
        runtime.ledger.clear()


async def _run_offered_load_envelope(
    *,
    source_id: str,
    suite: LoadSuite,
    mode: LoadMode,
    options: LoadStageOptions,
    quota_evidence: tuple[VerifiedQuotaEvidence, ...],
    artifact_path: Path,
) -> LoadArtifact:
    if suite.non_applicability is not None:
        raise ExecutionDriverError(
            "explicitly non-applicable suites have no Provider Lab load "
            "diagnostic",
        )
    registry = build_lab_adapter_registry()
    app = build_provider_lab_app(
        registry=registry,
        fixtures={source_id: [dict(_golden_fixture(source_id))]},
    )
    topology = LoadTopology.from_suite(suite)
    runner = _ProviderLabOfferedLoadRunner(
        source_id=source_id,
        suite=suite,
        topology=topology,
        options=options,
        app=app,
        quota_evidence=quota_evidence,
    )
    calibration = await _calibrate_offered_load_runner(runner)
    return await run_artifact_load_search(
        runner,
        source_id=source_id,
        suite=suite,
        mode=mode,
        topology=topology,
        config=options.search_config(suite),
        clock_mode=options.clock_mode,
        lab_calibration=calibration,
        include_soak=options.include_soak,
        quota_evidence=quota_evidence,
        # This diagnostic never emits executable receipt labels.  The zero is
        # intentional: route traffic cannot stand in for a typed pipeline
        # callable, even when it covers every Provider Lab operation.
        verified_executable_operation_coverage_ratio=0.0,
        required_provider_operation_labels=tuple(
            source_definition(source_id).operation_policy_ids,
        ),
        # This driver measures the Provider Lab boundary. It cannot claim that
        # the same offered load reached raw evidence, Kafka, Observation, and
        # T1; a future injected pipeline runner must supply that proof.
        pipeline_e2e_proven=False,
        artifact_path=artifact_path,
        require_promotion=False,
    )


def _typed_operation_coverage(
    artifact: Mapping[str, object],
    suite: LoadSuite,
) -> tuple[float, float]:
    """Derive operation coverage from canonical typed trial counters only."""

    trials = artifact.get("trials")
    if not isinstance(trials, list):
        return 0.0, 0.0
    counts: dict[str, int] = {}
    for trial in trials:
        if not isinstance(trial, Mapping):
            continue
        trial_counts = trial.get("operation_counts")
        if not isinstance(trial_counts, Mapping):
            continue
        for operation_id, count in trial_counts.items():
            if isinstance(operation_id, str) and isinstance(count, int):
                counts[operation_id] = counts.get(operation_id, 0) + count

    def _coverage(operation_ids: tuple[str, ...]) -> float:
        if not operation_ids:
            return 1.0
        return sum(counts.get(operation_id, 0) > 0 for operation_id in operation_ids) / len(
            operation_ids,
        )

    return (
        _coverage(tuple(operation.operation_id for operation in suite.data_operations)),
        _coverage(
            tuple(operation.operation_id for operation in suite.control_operations),
        ),
    )


def _suite_result_from_pipeline_load_artifact(
    *,
    suite: LoadSuite,
    artifact: Mapping[str, object],
    artifact_uri: str,
) -> SuiteResult:
    """Map a validated typed artifact to a fail-closed stage summary."""

    validate_pipeline_load_artifact(artifact)
    if artifact.get("workload") != suite.execution_workload_dict():
        raise ExecutionDriverError(
            f"{suite.kind} pipeline artifact workload differs from the "
            "catalog declaration",
        )
    state = artifact.get("state")
    if not isinstance(state, str):
        raise ExecutionDriverError("pipeline load artifact state is invalid")
    data_coverage, control_coverage = _typed_operation_coverage(artifact, suite)
    boundary = artifact.get("boundary")
    pipeline_e2e_proven = float(
        isinstance(boundary, Mapping)
        and boundary.get("evidence_class") == "exact_pipeline",
    )
    configuration = artifact.get("configuration")
    topology = (
        configuration.get("topology")
        if isinstance(configuration, Mapping)
        else None
    )
    quota_config_verified = float(artifact.get("quota") is not None)
    metrics = (
        ("typed_workload_declaration_bound", 1.0),
        ("executable_operation_coverage_ratio", data_coverage),
        ("control_operation_coverage_ratio", control_coverage),
        ("pipeline_e2e_proven", pipeline_e2e_proven),
        ("promotion_eligible", float(artifact.get("promotion_eligible") is True)),
        ("quota_config_verified", quota_config_verified),
        (
            "tenants",
            float(topology.get("tenants", suite.tenants))
            if isinstance(topology, Mapping)
            else float(suite.tenants),
        ),
        (
            "installations_per_tenant",
            float(topology.get("installations_per_tenant", suite.installations_per_tenant))
            if isinstance(topology, Mapping)
            else float(suite.installations_per_tenant),
        ),
        (
            "replicas",
            float(topology.get("replicas", suite.replicas))
            if isinstance(topology, Mapping)
            else float(suite.replicas),
        ),
    )
    if suite.non_applicability is not None:
        if state != "not_applicable":
            return SuiteResult(
                kind=suite.kind,
                state="failed",
                artifact_uri=artifact_uri,
                metrics=metrics,
                failures=(
                    "declared non-applicable workload did not produce a "
                    "not_applicable pipeline artifact",
                ),
            )
        return SuiteResult(
            kind=suite.kind,
            state="not_applicable",
            artifact_uri=artifact_uri,
            metrics=metrics,
        )
    if state == "not_applicable":
        return SuiteResult(
            kind=suite.kind,
            state="failed",
            artifact_uri=artifact_uri,
            metrics=metrics,
            failures=(
                "applicable workload produced an undeclared not_applicable "
                "pipeline artifact",
            ),
        )
    reason = artifact.get("reason_code")
    reason_label = reason if isinstance(reason, str) and reason else state
    return SuiteResult(
        kind=suite.kind,
        state="failed" if state == "failed" else "blocked",
        artifact_uri=artifact_uri,
        metrics=metrics,
        failures=(
            "typed pipeline artifact cannot promote through stage schema v3: "
            f"{reason_label}",
        ),
    )


async def _run_typed_pipeline_loads(
    source_id: str,
    *,
    ambient_env: Mapping[str, str],
    artifact_dir: Path,
    adapter_factory: PipelineAdapterFactory | None,
) -> tuple[
    dict[str, dict[str, object]],
    tuple[SuiteResult, ...],
    tuple[SuiteResult, ...],
]:
    """Run every typed suite/mode through the canonical pipeline runner.

    R1 intentionally supplies no concrete adapter.  The runner therefore
    seals truthful blocked artifacts (or the declared WhatsApp
    non-applicability) rather than falling back to Provider Lab route traffic.
    """

    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    artifacts: dict[str, dict[str, object]] = {
        "provider_safe": {},
        "fyralis_ceiling": {},
    }
    provider_safe_results: list[SuiteResult] = []
    ceiling_results: list[SuiteResult] = []
    for suite in spec.load_suites:
        workload = declared_pipeline_workload_from_suite(suite)
        config = diagnostic_pipeline_load_config_from_suite(suite)
        for mode, results in (
            ("provider_safe", provider_safe_results),
            ("fyralis_ceiling", ceiling_results),
        ):
            artifact = await run_pipeline_load(
                source_id=source_id,
                mode=mode,
                workload=workload,
                ambient_env=ambient_env,
                adapter_factory=adapter_factory,
                quota=None,
                config=config,
            )
            validate_pipeline_load_artifact(artifact)
            path = artifact_dir / "pipeline_load" / suite.kind / f"{mode}.json"
            write_pipeline_load_artifact(path, artifact)
            artifacts[mode][suite.kind] = artifact
            results.append(
                _suite_result_from_pipeline_load_artifact(
                    suite=suite,
                    artifact=artifact,
                    artifact_uri=(
                        "evidence-file:pipeline_load/"
                        f"{suite.kind}/{mode}.json"
                    ),
                ),
            )
    return (
        artifacts,
        tuple(provider_safe_results),
        tuple(ceiling_results),
    )


async def _offered_load_diagnostic(
    source_id: str,
    *,
    ambient_env: Mapping[str, str],
    artifact_dir: Path,
    options: LoadStageOptions,
) -> dict[str, object]:
    """Measure Provider Lab route capacity without certifying Fyralis load.

    This remains a useful request-boundary diagnostic, but it is deliberately
    isolated from ``CertificationInput``.  Typed pipeline artifacts emitted by
    :func:`run_pipeline_load` are the active load result path.
    """

    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    routes = build_lab_adapter_registry().require(source_id).routes
    quota_evidence, quota_note = _load_quota_evidence(
        source_id,
        routes,
        ambient_env=ambient_env,
    )
    suite_summaries: list[dict[str, object]] = []

    for suite in spec.load_suites:
        summary: dict[str, object] = {
            "kind": suite.kind,
            "workload": suite.execution_workload_dict(),
            "compatibility_operation_mix": list(suite.operation_mix),
            "topology": dataclasses.asdict(LoadTopology.from_suite(suite)),
        }
        if suite.non_applicability is not None:
            summary["provider_lab_diagnostic"] = {
                "state": "not_applicable",
                "reason": suite.non_applicability.reason,
                "evidence_id": suite.non_applicability.evidence_id,
            }
            suite_summaries.append(summary)
            continue

        suite_dir = artifact_dir / "provider_lab_load" / suite.kind
        ceiling_path = suite_dir / "fyralis_ceiling.json"
        provider_path = suite_dir / "provider_safe.json"
        ceiling_artifact: LoadArtifact | None = None
        provider_artifact: LoadArtifact | None = None
        ceiling_error: str | None = None
        provider_error: str | None = None

        try:
            ceiling_artifact = await _run_offered_load_envelope(
                source_id=source_id,
                suite=suite,
                mode="fyralis_ceiling",
                options=options,
                quota_evidence=(),
                artifact_path=ceiling_path,
            )
        except Exception as exc:  # noqa: BLE001 - source-confined evidence
            ceiling_error = (
                f"Fyralis-ceiling offered-load search failed: "
                f"{type(exc).__name__}: {exc}"
            )

        if quota_evidence:
            try:
                provider_artifact = await _run_offered_load_envelope(
                    source_id=source_id,
                    suite=suite,
                    mode="provider_safe",
                    options=options,
                    quota_evidence=quota_evidence,
                    artifact_path=provider_path,
                )
            except Exception as exc:  # noqa: BLE001 - source-confined evidence
                provider_error = (
                    f"provider-safe offered-load search failed: "
                    f"{type(exc).__name__}: {exc}"
                )
        else:
            provider_error = quota_note

        comparison_payload: dict[str, object] | None = None
        comparison_failure: str | None = None
        if provider_artifact is not None and ceiling_artifact is not None:
            try:
                comparison = compare_load_envelopes(
                    provider_artifact,
                    ceiling_artifact,
                )
            except Exception as exc:  # noqa: BLE001 - fail-closed headroom
                comparison_failure = (
                    f"envelope comparison failed: {type(exc).__name__}: {exc}"
                )
            else:
                comparison_payload = dataclasses.asdict(comparison)
        summary["provider_lab_diagnostic"] = {
            "provider_safe": (
                provider_artifact.as_dict()
                if provider_artifact is not None
                else {"state": "blocked", "reason": provider_error}
            ),
            "fyralis_ceiling": (
                ceiling_artifact.as_dict()
                if ceiling_artifact is not None
                else {"state": "blocked", "reason": ceiling_error}
            ),
            "comparison": (
                comparison_payload
                if comparison_payload is not None
                else {
                    "state": "blocked",
                    "reason": (
                        comparison_failure
                        or provider_error
                        or ceiling_error
                    ),
                }
            ),
        }
        suite_summaries.append(summary)

    return {
        "state": "diagnostic_only",
        "clock_mode": options.clock_mode,
        "promotion_durations_requested": options.promotion,
        "quota_configuration": {
            "state": "verified" if quota_evidence else "blocked",
            "reason": quota_note,
            "evidence": [item.as_dict() for item in quota_evidence],
        },
        "suites": suite_summaries,
        "claim_boundary": (
            "Typed data-operation scheduling and strict Provider Lab route "
            "coverage were measured across the declared topology. This "
            "diagnostic does not invoke typed Fyralis callables and cannot "
            "prove raw evidence, Kafka, Observation, T1, cursor, quota, or "
            "non-HTTP protocol throughput."
        ),
    }


async def _declared_surface_load_probe(source_id: str) -> dict[str, object]:
    """Measure one concurrent request for every exact HTTP operation binding.

    The probe is intentionally finite.  It demonstrates the exact operation
    mix and four installation scopes that a future maximum-stable-rate runner
    must drive; it does not claim to have found a throughput ceiling.
    """

    registry = build_lab_adapter_registry()
    adapter = registry.require(source_id)
    app = build_provider_lab_app(
        registry=registry,
        fixtures={source_id: [dict(_golden_fixture(source_id))]},
    )
    request_plan = [
        (
            operation,
            (
                f"tenant-{index % 2 + 1}:"
                f"installation-{index % 4 // 2 + 1}"
            ),
        )
        for index, operation in enumerate(
            _provider_lab_http_operation_plan(source_id),
        )
    ]
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
        raise_app_exceptions=False,
    )
    latencies: list[float] = []
    started = time.perf_counter()
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
        timeout=30.0,
    ) as client:
        async def _one(
            operation: _ProviderLabHttpOperation,
            scope: str,
        ) -> dict[str, object]:
            request_started = time.perf_counter()
            response = await _provider_request(
                client,
                source_id=source_id,
                route=operation.route,
                method=operation.method,
                scope=scope,
                binding=operation.binding,
            )
            latency = (time.perf_counter() - request_started) * 1_000
            latencies.append(latency)
            if response.status_code >= 500:
                raise ExecutionDriverError(
                    f"{source_id}.{operation.route.route_id}."
                    f"{operation.method} load probe "
                    f"returned {response.status_code}",
                )
            return {
                "route_id": operation.route.route_id,
                "method": operation.method,
                "operation_id": operation.operation_id,
                "scope": scope,
                "status_code": response.status_code,
                "latency_ms": latency,
                "response_bytes": len(response.content),
            }

        results = list(
            await asyncio.gather(
                *(
                    _one(operation, scope)
                    for operation, scope in request_plan
                )
            )
        )
    elapsed = max(time.perf_counter() - started, 1e-9)
    ledger = app.state.provider_lab.ledger.list(
        source=source_id,
        limit=1_000,
    )
    expected_operations = set(source_definition(source_id).operation_policy_ids)
    exercised_operations: set[str] = set()
    for result in results:
        operation_id = result["operation_id"]
        status_code = result["status_code"]
        if (
            isinstance(operation_id, str)
            and isinstance(status_code, int)
            and status_code < 400
        ):
            exercised_operations.add(operation_id)
    declared_http_operations = {
        operation_id
        for route in adapter.routes
        for operation_id in route.operation_ids
    }
    protocol_operations = {
        operation_id
        for surface in adapter.protocol_surfaces
        for operation_id in surface.operation_ids
    }
    if declared_http_operations | protocol_operations != expected_operations:
        raise ExecutionDriverError(
            f"{source_id} load operation plan drifted from the catalog",
        )
    uncovered_operations = sorted(
        declared_http_operations - exercised_operations
    )
    return {
        "state": "blocked" if uncovered_operations else "diagnostic_only",
        "request_count": len(results),
        "elapsed_seconds": elapsed,
        "requests_per_second": len(results) / elapsed,
        "p50_latency_ms": statistics.median(latencies),
        "p95_latency_ms": _percentile(latencies, 0.95),
        "p99_latency_ms": _percentile(latencies, 0.99),
        "results": results,
        "http_operation_ids": sorted(exercised_operations),
        "uncovered_http_operation_ids": uncovered_operations,
        "protocol_operation_ids_not_load_exercised": sorted(
            protocol_operations,
        ),
        "declared_operation_ids": sorted(expected_operations),
        "ledger_entries": len(ledger),
        "scope_values": sorted(
            {str(item["scope"]) for item in ledger}
        ),
        "claim_boundary": (
            "One concurrent request per exact route/method/operation binding "
            "is a declared-mix diagnostic, not a stepped "
            "maximum-stable-rate search."
        ),
    }


async def _load_diagnostic(
    source_id: str,
    *,
    request_count: int,
    ambient_env: Mapping[str, str],
) -> dict[str, object]:
    distributed_transport = (
        await run_distributed_transport_diagnostic_from_env(
            source_id,
            ambient_env=ambient_env,
        )
    )
    representative = _representative_route(source_id)
    if representative is None:
        adapter = build_lab_adapter_registry().require(source_id)
        ingress_route = next(
            (
                route
                for route in adapter.routes
                if route.quota_bucket is not None
            ),
            None,
        )
        if ingress_route is None:
            return {
                "state": "blocked",
                "reason": (
                    "no quota-bearing HTTP or ingress surface is declared "
                    f"for {source_id}"
                ),
                "distributed_provider_transport": distributed_transport,
            }
        route = ingress_route
        operation_id: str | None = None
    else:
        route, operation_id = representative
    registry = build_lab_adapter_registry()
    app = build_provider_lab_app(
        registry=registry,
        fixtures={source_id: [dict(_golden_fixture(source_id))]},
    )
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
        raise_app_exceptions=False,
    )
    latencies_ms: list[float] = []
    started = time.perf_counter()
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
        timeout=30.0,
    ) as client:
        async def _no_sleep(_delay: float) -> None:
            return None

        provider_transport = ProviderTransport(
            sleep=_no_sleep,
            random_fn=lambda: 0.0,
        )

        async def _measured_request() -> int:
            request_started = time.perf_counter()
            if operation_id is None:
                response = await _provider_request(
                    client,
                    source_id=source_id,
                    route=route,
                    method=route.methods[0],
                )
                status = response.status_code
            else:
                status = await _run_transport_call(
                    client=client,
                    source_id=source_id,
                    route=route,
                    operation_id=operation_id,
                    transport=provider_transport,
                )
            latencies_ms.append(
                (time.perf_counter() - request_started) * 1_000,
            )
            return status

        statuses = list(
            await asyncio.gather(
                *(_measured_request() for _index in range(request_count)),
            )
        )
    elapsed = max(time.perf_counter() - started, 1e-9)
    policy = (
        effective_request_policy(source_id, operation_id)
        if operation_id is not None
        else None
    )
    disabled = {
        "request_count": request_count,
        "elapsed_seconds": elapsed,
        "requests_per_second": request_count / elapsed,
        "p50_latency_ms": statistics.median(latencies_ms),
        "p95_latency_ms": _percentile(latencies_ms, 0.95),
        "p99_latency_ms": _percentile(latencies_ms, 0.99),
        "status_counts": {
            str(status): statuses.count(status)
            for status in sorted(set(statuses))
        },
        "route_id": route.route_id,
        "operation_id": operation_id,
        "execution_boundary": (
            "provider_transport"
            if operation_id is not None
            else "provider_lab_live_ingress"
        ),
        "quota_mode": "disabled",
        "policy_max_concurrency": (
            policy.max_concurrency if policy is not None else None
        ),
    }

    quota, quota_cost, quota_note = _load_quota_config(
        source_id,
        route,
        ambient_env=ambient_env,
    )
    provider_safe: dict[str, object] = {
        "state": "blocked",
        "reason": quota_note,
    }
    if quota is not None:
        if quota_cost is None:
            raise ExecutionDriverError(
                "loaded quota configuration is missing its weighted cost"
            )
        operation_binding = (
            route.binding_for(operation_id)
            if operation_id is not None
            else None
        )
        app.state.provider_lab.quotas.configure(quota)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://provider-lab",
            timeout=30.0,
        ) as client:
            async def _quota_request() -> int:
                response = await _provider_request(
                    client,
                    source_id=source_id,
                    route=route,
                    method=(
                        operation_binding.method
                        if operation_binding is not None
                        else route.methods[0]
                    ),
                    scope=quota.scope,
                    binding=operation_binding,
                    quota_requirements=(
                        QuotaRequirement(
                            scope=quota.scope,
                            bucket=quota.bucket,
                            limit_id=quota.limit_id,
                            cost=quota_cost,
                        ),
                    ),
                )
                return response.status_code

            quota_statuses = list(
                await asyncio.gather(
                    *(_quota_request() for _index in range(request_count)),
                )
            )
        provider_safe = {
            "state": "diagnostic_only",
            "reason": quota_note,
            "request_count": request_count,
            "status_counts": {
                str(status): quota_statuses.count(status)
                for status in sorted(set(quota_statuses))
            },
            "quota_snapshot": app.state.provider_lab.quotas.snapshot(),
        }

    surface_mix = await _declared_surface_load_probe(source_id)
    return {
        "state": (
            "failed"
            if distributed_transport["state"] == "failed"
            else "diagnostic_only"
        ),
        "quota_disabled": disabled,
        "declared_surface_mix": surface_mix,
        "provider_safe": provider_safe,
        "distributed_provider_transport": distributed_transport,
        "claim_boundary": (
            "This is a short Provider Lab/ProviderTransport boundary "
            "diagnostic. A passing shared-Redis sub-diagnostic proves only "
            "its exact quota/cooldown assertions, not a full pipeline "
            "maximum-stable-rate search, 15-minute validation, or 60-minute "
            "soak."
        ),
    }


async def _fault_diagnostic(
    source_id: str,
    *,
    ambient_env: Mapping[str, str],
) -> dict[str, object]:
    distributed_transport = (
        await run_distributed_transport_diagnostic_from_env(
            source_id,
            ambient_env=ambient_env,
        )
    )
    route_operations = _retryable_route_operations(source_id)
    adapter = build_lab_adapter_registry().require(source_id)
    if not route_operations:
        return {
            "state": "blocked",
            "reason": (
                "no retry-safe quota-bearing HTTP operation is declared for "
                f"{source_id}"
            ),
            "protocol_surfaces": [
                {
                    "surface_id": surface.surface_id,
                    "operation_ids": list(surface.operation_ids),
                    "state": "not_exercised_by_http_fault_driver",
                }
                for surface in adapter.protocol_surfaces
            ],
            "distributed_provider_transport": distributed_transport,
        }
    registry = build_lab_adapter_registry()
    app = build_provider_lab_app(
        registry=registry,
        fixtures={source_id: [dict(_golden_fixture(source_id))]},
    )
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
        raise_app_exceptions=False,
    )
    recovered: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
        timeout=30.0,
    ) as client:
        for route, operation_id in route_operations:
            for status_code, headers in (
                (503, {}),
                (429, {"Retry-After": "0"}),
            ):
                before = len(
                    app.state.provider_lab.ledger.list(
                        source=source_id,
                        route_id=route.route_id,
                        limit=1_000,
                    )
                )
                rule = app.state.provider_lab.faults.create(
                    source=source_id,
                    route_id=route.route_id,
                    status_code=status_code,
                    headers=headers,
                    max_hits=1,
                )
                rule_id = str(rule["rule_id"])
                try:
                    terminal_status = await _run_transport_call(
                        client=client,
                        source_id=source_id,
                        route=route,
                        operation_id=operation_id,
                    )
                    rule_after = next(
                        item
                        for item in app.state.provider_lab.faults.snapshot()
                        if item["rule_id"] == rule_id
                    )
                    after = len(
                        app.state.provider_lab.ledger.list(
                            source=source_id,
                            route_id=route.route_id,
                            limit=1_000,
                        )
                    )
                    result = {
                        "route_id": route.route_id,
                        "operation_id": operation_id,
                        "injected_status": status_code,
                        "fault_rule_id": rule_id,
                        "fault_hits": rule_after["hits"],
                        "terminal_status": terminal_status,
                        "ledger_attempts": after - before,
                    }
                    if (
                        rule_after["hits"] == 1
                        and after - before >= 2
                        and terminal_status not in {
                            429,
                            500,
                            502,
                            503,
                            504,
                        }
                    ):
                        recovered.append(result)
                    else:
                        failures.append(
                            {
                                **result,
                                "error": "retry did not reach a terminal response",
                            }
                        )
                except Exception as exc:  # noqa: BLE001 - retain failed operation
                    failures.append(
                        {
                            "route_id": route.route_id,
                            "operation_id": operation_id,
                            "injected_status": status_code,
                            "fault_rule_id": rule_id,
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )
                finally:
                    app.state.provider_lab.faults.remove(rule_id)

    return {
        "state": (
            "failed"
            if failures or distributed_transport["state"] == "failed"
            else "diagnostic_only"
        ),
        "retry_safe_operation_count": len(route_operations),
        "recovered_faults": recovered,
        "failed_faults": failures,
        "distributed_provider_transport": distributed_transport,
        "protocol_surfaces": [
            {
                "surface_id": surface.surface_id,
                "operation_ids": list(surface.operation_ids),
                "state": "not_exercised_by_http_fault_driver",
            }
            for surface in adapter.protocol_surfaces
        ],
        "claim_boundary": (
            "Every retry-safe catalog HTTP operation was subjected to one "
            "503 and one 429 through ProviderTransport. "
            "Historical/live/combined pipeline recovery, "
            "cursor integrity, and backlog recovery remain unproven. A "
            "passing shared-Redis sub-diagnostic proves only its exact "
            "two-replica quota/cooldown assertions."
        ),
    }


async def run_stage(
    *,
    source_id: str,
    stage: Stage,
    result_path: Path,
    artifact_dir: Path,
    ambient_env: Mapping[str, str] | None = None,
    load_request_count: int = _DEFAULT_LOAD_REQUESTS,
    load_options: LoadStageOptions | None = None,
    pipeline_adapter_factory: PipelineAdapterFactory | None = None,
    expected_plan_sha256: str | None = None,
) -> CertificationInput:
    """Run one source-isolated stage and write its strict result + artifact."""

    if source_id not in SOURCE_CERTIFICATION_CATALOG:
        raise ExecutionDriverError(f"unknown canonical source {source_id!r}")
    if stage not in _STAGES:
        raise ExecutionDriverError(f"unknown certification stage {stage!r}")
    if load_request_count <= 0:
        raise ExecutionDriverError("load_request_count must be positive")
    effective_load_options = load_options or LoadStageOptions()

    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    execution_plan = build_declared_execution_plan(source_id)
    plan_sha256 = _sha256(
        _canonical_json(execution_plan).encode("utf-8"),
    )
    if (
        expected_plan_sha256 is not None
        and expected_plan_sha256 != plan_sha256
    ):
        raise ExecutionDriverError(
            f"{source_id} execution plan hash is stale: "
            f"expected {expected_plan_sha256}, current {plan_sha256}",
        )
    env = dict(os.environ if ambient_env is None else ambient_env)
    artifact: dict[str, object] = {
        "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
        "source_id": source_id,
        "stage": stage,
        "spec_hash": spec.declaration_hash(),
        "execution_plan_sha256": plan_sha256,
        "execution_plan": execution_plan,
        "generated_at": _utc_now().isoformat(),
        "synthetic_promotion_allowed": False,
    }
    reason: str
    supplied = _base_input(
        spec,
        reason="stage has not supplied complete release evidence",
    )

    if stage == "local_correctness":
        fixture_probe = _fixture_and_binding_probe(source_id)
        used_surface, _app = await _probe_used_surface(source_id)
        pipeline = await run_pipeline_probe(
            source_id=source_id,
            ambient_env=env,
        )
        (
            scenario_diagnostics,
            scenario_failures,
            scenario_states,
        ) = (
            _local_scenario_diagnostics(
                spec,
                fixture_probe=fixture_probe,
                used_surface=used_surface,
                pipeline_probe=pipeline,
            )
        )
        pipeline_state = pipeline.get("state")
        if pipeline_state == "passed":
            reason = (
                "The isolated raw-S3/Kafka-to-Observation/T1 and replay "
                "scenarios passed; unrelated source lifecycle, multi-install, "
                "cursor-failure, and distributed scenarios remain blocked as "
                "detailed in the scenario execution ledger"
            )
        elif pipeline_state == "failed":
            reason = (
                "The isolated raw-to-T1 pipeline was attempted and failed; "
                "unrelated scenarios remain blocked as detailed in the "
                "scenario execution ledger"
            )
        else:
            reason = (
                "Source-owned fixtures/callables/targets and Provider Lab "
                "routing were measured, but isolated Postgres/Kafka/S3 was "
                "not supplied and full raw-to-Observation-to-T1 scenarios "
                "remain blocked as detailed in the execution ledger"
            )
        artifact["fixture_and_binding_probe"] = fixture_probe
        artifact["provider_lab_used_surface"] = used_surface
        artifact["pipeline_probe"] = pipeline
        artifact["scenario_execution_ledger"] = scenario_diagnostics
        artifact["claim_boundary"] = reason
        supplied = _base_input(
            spec,
            reason=reason,
            scenario_failures=scenario_failures,
            scenario_states=scenario_states,
            local_correctness=(
                "failed" if pipeline_state == "failed" else "blocked"
            ),
        )
    elif stage == "load":
        diagnostic = await _load_diagnostic(
            source_id,
            request_count=load_request_count,
            ambient_env=env,
        )
        (
            pipeline_load_artifacts,
            provider_safe_suites,
            fyralis_ceiling_suites,
        ) = await _run_typed_pipeline_loads(
            source_id,
            ambient_env=env,
            artifact_dir=artifact_dir,
            adapter_factory=pipeline_adapter_factory,
        )
        offered_load = await _offered_load_diagnostic(
            source_id,
            ambient_env=env,
            artifact_dir=artifact_dir,
            options=effective_load_options,
        )
        reason = (
            "Every typed historical/live/combined workload was delegated to "
            "the pipeline load runner. Without the R3 exact-pipeline adapter "
            "and R5 verified quota input, applicable workloads remain "
            "fail-closed; WhatsApp historical is recorded as declared "
            "non-applicability. Provider Lab route measurements remain "
            "diagnostic-only and cannot promote a load claim."
        )
        artifact["load_diagnostic"] = diagnostic
        artifact["offered_load"] = offered_load
        artifact["pipeline_load_artifacts"] = pipeline_load_artifacts
        artifact["declared_load_suites"] = execution_plan["load_suites"]
        artifact["claim_boundary"] = reason
        supplied = _base_input(spec, reason=reason)
        supplied = CertificationInput(
            spec_hash=supplied.spec_hash,
            local_correctness=supplied.local_correctness,
            local_correctness_artifact=supplied.local_correctness_artifact,
            scenario_results=supplied.scenario_results,
            provider_safe_suites=provider_safe_suites,
            fyralis_ceiling_suites=fyralis_ceiling_suites,
            fault_recovery_suites=supplied.fault_recovery_suites,
            canary=supplied.canary,
            legacy_reference_count=0,
        )
    elif stage == "fault_recovery":
        diagnostic = await _fault_diagnostic(
            source_id,
            ambient_env=env,
        )
        distributed = diagnostic.get("distributed_provider_transport")
        distributed_passed = (
            isinstance(distributed, Mapping)
            and distributed.get("state") == "passed"
            and distributed.get("exact_assertions_passed") is True
        )
        reason = (
            "ProviderTransport retry and exact two-replica shared-Redis "
            "quota/cooldown diagnostics ran, but historical/live/combined "
            "pipeline recovery remains unexecuted"
            if distributed_passed
            else (
                "A representative ProviderTransport retry diagnostic ran, "
                "but historical/live/combined pipeline recovery and exact "
                "two-replica shared-Redis quota/cooldown proof remain "
                "unexecuted"
            )
        )
        artifact["fault_recovery_diagnostic"] = diagnostic
        artifact["declared_fault_targets"] = [
            {
                "route_id": route.route_id,
                "operation_id": operation_id,
            }
            for route, operation_id in _retryable_route_operations(source_id)
        ]
        artifact["claim_boundary"] = reason
        supplied = _base_input(spec, reason=reason)
        supplied = CertificationInput(
            spec_hash=supplied.spec_hash,
            local_correctness=supplied.local_correctness,
            local_correctness_artifact=supplied.local_correctness_artifact,
            scenario_results=supplied.scenario_results,
            provider_safe_suites=supplied.provider_safe_suites,
            fyralis_ceiling_suites=supplied.fyralis_ceiling_suites,
            fault_recovery_suites=_blocked_suites(reason=reason),
            canary=supplied.canary,
            legacy_reference_count=0,
        )
    else:
        canary_started_at = _utc_now()
        credential_names = sorted(
            name
            for name, value in env.items()
            if value
            and (
                name == spec.canary.credential_env_prefix
                or name.startswith(f"{spec.canary.credential_env_prefix}_")
            )
        )
        reason = (
            f"{source_id} has no committed real-provider canary executable; "
            "credentials alone can never become passing canary evidence"
        )
        artifact["credential_environment_names_present"] = credential_names
        artifact["credential_values_recorded"] = False
        artifact["real_provider_requests_sent"] = 0
        canary_completed_at = _utc_now()
        artifact["canary_execution"] = {
            "schema_version": CANARY_EXECUTION_SCHEMA_VERSION,
            "source_id": source_id,
            "canary_id": spec.canary.canary_id,
            "promotion_eligible": False,
            "account_identity_sha256": None,
            "account_type": spec.canary.account_type,
            "api_version": spec.provider_api_version,
            "started_at": canary_started_at.isoformat(),
            "completed_at": canary_completed_at.isoformat(),
            "request_ledger": [],
            "cleanup": {
                "required": False,
                "state": "not_required",
                "completed_at": None,
                "actions": [],
            },
        }
        artifact["claim_boundary"] = reason
        supplied = _base_input(spec, reason=reason)

    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / "stage.json"
    artifact_path.write_text(_canonical_json(artifact), encoding="utf-8")
    write_certification_input(result_path, supplied)
    return supplied


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        required=True,
        choices=CANONICAL_SOURCE_IDS,
    )
    parser.add_argument(
        "--stage",
        required=True,
        choices=_STAGES,
    )
    parser.add_argument(
        "--plan-sha256",
        required=True,
        help="Exact source-specific local execution plan digest.",
    )
    parser.add_argument(
        "--load-promotion",
        action="store_true",
        help=(
            "Use declared wall-clock warmup/validation/soak durations. "
            "The stage still fails closed without pipeline evidence."
        ),
    )
    parser.add_argument(
        "--load-initial-rate",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--load-offer-limit-rate",
        type=float,
        default=_DEFAULT_DIAGNOSTIC_OFFER_LIMIT,
    )
    parser.add_argument(
        "--load-diagnostic-seconds",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--load-soak",
        action="store_true",
        help="Include the short soak in diagnostic mode.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result_raw = os.environ.get("FYRALIS_CERTIFICATION_RESULT_PATH")
    artifact_raw = os.environ.get("FYRALIS_CERTIFICATION_ARTIFACT_DIR")
    if not result_raw or not artifact_raw:
        raise SystemExit(
            "FYRALIS_CERTIFICATION_RESULT_PATH and "
            "FYRALIS_CERTIFICATION_ARTIFACT_DIR are required",
        )
    load_options = LoadStageOptions(
        initial_rate=args.load_initial_rate,
        offer_limit_rate=args.load_offer_limit_rate,
        clock_mode="wall" if args.load_promotion else "virtual",
        include_soak=args.load_promotion or args.load_soak,
        promotion=args.load_promotion,
        diagnostic_duration_seconds=args.load_diagnostic_seconds,
        calibration_probe_seconds=30.0 if args.load_promotion else 0.1,
        calibration_concurrency=32 if args.load_promotion else 4,
        calibration_minimum_samples=100 if args.load_promotion else 1,
    )
    asyncio.run(
        run_stage(
            source_id=args.source,
            stage=args.stage,
            result_path=Path(result_raw),
            artifact_dir=Path(artifact_raw),
            load_options=load_options,
            expected_plan_sha256=args.plan_sha256,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    raise SystemExit(main())


__all__ = [
    "ExecutionDriverError",
    "LoadStageOptions",
    "STAGE_ARTIFACT_SCHEMA_VERSION",
    "build_declared_execution_plan",
    "declared_execution_plan_sha256",
    "run_stage",
]
