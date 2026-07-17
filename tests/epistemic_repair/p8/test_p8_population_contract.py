from lib.evaluation.epistemic_repair.p8_population import (
    build_characterization_manifests,
    build_fault_schedule,
    build_scale_matrix,
    fault_injection_points,
)


def test_fault_schedule_is_exact_complete_and_sealed() -> None:
    schedule = build_fault_schedule()
    assert len(schedule.cases) == 12
    assert len({case.case_id for case in schedule.cases}) == 12
    assert set(fault_injection_points()) == {case.boundary for case in schedule.cases}
    assert len(schedule.digest) == 64
    assert schedule == build_fault_schedule()


def test_scale_matrix_is_exact_cartesian_product() -> None:
    cells = build_scale_matrix()
    assert len(cells) == 27
    assert {(x.batch_size, x.memory_horizon_batches, x.tenant_concurrency) for x in cells} == {
        (batch, horizon, tenants)
        for batch in (10, 25, 50)
        for horizon in (12, 50, 100)
        for tenants in (1, 5, 20)
    }


def test_component_manifests_preserve_exact_denominators_and_slices() -> None:
    manifests = {x.population: x for x in build_characterization_manifests()}
    assert {name: row.exact_size for name, row in manifests.items()} == {
        "boundary_discovery": 1200,
        "context_selection": 600,
        "entity_grounding": 2400,
        "retrieval": 600,
        "feedback": 360,
    }
    assert dict(manifests["boundary_discovery"].required_composition)["episodes"] == 240
    assert dict(manifests["entity_grounding"].required_composition)["near_name_collision"] == 200
    assert dict(manifests["retrieval"].required_composition)["mature"] == 200
    assert len({x.sealed_digest for x in manifests.values()}) == 5
