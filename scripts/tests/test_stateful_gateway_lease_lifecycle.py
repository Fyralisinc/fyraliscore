from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_gateway_launcher_delegates_session_ownership_to_contract_router() -> None:
    source = (REPO_ROOT / "scripts/run_connector_gateway_worker.py").read_text(
        encoding="utf-8"
    )
    assert "await router.run_gateway(source, install, stop_event)" in source
    assert "FROM source_connector_installations" in source
    assert "provider_installations" not in source
