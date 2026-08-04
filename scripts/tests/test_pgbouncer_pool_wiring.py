from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

PRODUCTION_POOL_FILES = (
    "services/app/gateway/db_bootstrap.py",
    "scripts/run_think_worker.py",
    "scripts/run_post_commit_worker.py",
    "scripts/run_housekeeper_worker.py",
    "scripts/run_sage_structural_features_worker.py",
    "scripts/run_sage_topology_optimizer_worker.py",
    "scripts/run_relationship_ontology_proposals_worker.py",
    "scripts/run_topology_sweeper.py",
    "scripts/run_connector_gateway_worker.py",
    "scripts/run_connector_subscription_scheduler.py",
    "scripts/run_connector_poll_worker.py",
)


def _is_asyncpg_create_pool_call(node: ast.Call) -> bool:
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "create_pool"
        and isinstance(func.value, ast.Name)
        and func.value.id == "asyncpg"
    )


def test_production_launchers_thread_pgbouncer_runtime_kwargs() -> None:
    missing: list[str] = []
    for relative in PRODUCTION_POOL_FILES:
        path = REPO_ROOT / relative
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and _is_asyncpg_create_pool_call(node)
        ]
        if not calls:
            missing.append(f"{relative}: no asyncpg.create_pool call found")
            continue
        if "asyncpg_pool_runtime_kwargs" not in source:
            missing.append(f"{relative}: does not use asyncpg_pool_runtime_kwargs")
            continue
        for call in calls:
            if not any(keyword.arg is None for keyword in call.keywords):
                missing.append(
                    f"{relative}:{call.lineno}: create_pool lacks **runtime kwargs"
                )

    assert missing == []
