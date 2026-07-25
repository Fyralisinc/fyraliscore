from __future__ import annotations

import pytest

from services.ingest.synthetic.provider_lab import __main__ as cli


def test_cli_runs_on_loopback_with_production_guarded_app(monkeypatch) -> None:
    captured: dict[str, object] = {}
    sentinel = object()
    monkeypatch.setattr(cli, "build_provider_lab_app", lambda: sentinel)

    def fake_run(app, **kwargs) -> None:  # noqa: ANN001
        captured["app"] = app
        captured.update(kwargs)

    monkeypatch.setattr(cli.uvicorn, "run", fake_run)

    assert cli.main(["--host", "127.0.0.2", "--port", "9876"]) == 0
    assert captured == {
        "app": sentinel,
        "host": "127.0.0.2",
        "port": 9876,
        "log_level": "info",
        "access_log": False,
    }


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "provider-lab.local"])
def test_cli_rejects_non_loopback_or_hostname(host: str) -> None:
    with pytest.raises(SystemExit):
        cli.main(["--host", host])


@pytest.mark.parametrize("port", ["0", "65536"])
def test_cli_rejects_invalid_port(port: str) -> None:
    with pytest.raises(SystemExit):
        cli.main(["--port", port])
