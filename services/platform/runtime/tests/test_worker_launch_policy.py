from __future__ import annotations

from pathlib import Path

from services.platform.runtime.process_manifest import process_by_name
from services.platform.runtime.worker_launch_policy import worker_launch_policies
from services.workers.housekeeper.worker import build_housekeeper_descriptors


ROOT = Path(__file__).resolve().parents[4]


def test_every_services_worker_package_has_launch_policy() -> None:
    worker_dirs = {
        path.name
        for path in (ROOT / "services/workers").iterdir()
        if path.is_dir()
        and not path.name.startswith("__")
        and (path / "__init__.py").exists()
    }
    policies = worker_launch_policies()
    policy_packages = {policy.package for policy in policies}

    assert len(policy_packages) == len(policies)
    assert worker_dirs == policy_packages


def test_production_process_policies_reference_manifest_processes() -> None:
    for policy in worker_launch_policies():
        if policy.status != "production_process":
            continue
        assert policy.runtime_process is not None
        process = process_by_name(policy.runtime_process)
        assert "production" in process.modes
        assert process.compose_service
        assert process.has_healthcheck is True


def test_housekeeper_launch_policies_match_default_enablement(monkeypatch) -> None:
    flags = {
        flag
        for policy in worker_launch_policies()
        for flag in policy.enable_flags
    }
    for flag in flags | {"HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS"}:
        monkeypatch.delenv(flag, raising=False)

    descriptors = {d.name: d for d in build_housekeeper_descriptors()}

    for policy in worker_launch_policies():
        for job in policy.housekeeper_jobs:
            assert job in descriptors
            if policy.status == "housekeeper_default":
                assert descriptors[job].enabled is True
            elif policy.status == "housekeeper_flag_gated":
                assert descriptors[job].enabled is False


def test_flag_gated_housekeeper_jobs_enable_through_expensive_gate(monkeypatch) -> None:
    for policy in worker_launch_policies():
        for flag in policy.enable_flags:
            monkeypatch.delenv(flag, raising=False)
    monkeypatch.setenv("HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS", "1")

    descriptors = {d.name: d for d in build_housekeeper_descriptors()}

    for policy in worker_launch_policies():
        if policy.status != "housekeeper_flag_gated":
            continue
        for job in policy.housekeeper_jobs:
            assert descriptors[job].enabled is True
