"""Agent daemon behavior: heartbeat record, buffering+retry (I3), license gate."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

from agent import Agent
from config import load_agent_config
from conftest import make_config_bundle, make_license
from health_probe import HealthProbe, static_probe
from license_check import LicenseChecker
from lib import DeploymentRecord, Health, TelemetryTier


def _build_agent(
    fabric,
    tmp_path: Path,
    fake_console,
    *,
    healthy_sli: bool = True,
    expires_in_days: int = 365,
    license_signed: bool = True,
):
    version_file = tmp_path / "VERSION"
    version_file.write_text("1.4.2\n", encoding="utf-8")
    lic = make_license(
        fabric,
        tmp_path / "license.json",
        expires_in_days=expires_in_days,
        sign=license_signed,
    )
    cfg = load_agent_config(
        console_url="https://console:8080",
        tenant_id="acme",
        deployment_id="acme-use1-0001",
        region="us-east-1",
        telemetry_tier="T1",
        version_file=version_file,
        license_path=lic,
        trust_root_path=fabric.trust_root_path,
        config_dir=tmp_path / "applied",
        healthz_url="http://127.0.0.1:9/healthz",
        buffer_path=tmp_path / "buffer.jsonl",
        backoff_base_s=0.01,
        backoff_max_s=0.5,
        interval_s=0.05,
    )
    return Agent(
        cfg,
        sender=fake_console.sender,
        probe=HealthProbe(static_probe(healthy_sli)),
        license_checker=LicenseChecker(lic, trust_root_path=str(fabric.trust_root_path)),
    )


# --------------------------------------------------------------------------- #
# Heartbeat record validity (C4)                                              #
# --------------------------------------------------------------------------- #


def test_tick_emits_valid_deployment_record(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)
    result = agent.tick()
    assert result.delivered and not result.buffered

    assert len(fake_console.received) == 1
    rec = fake_console.received[0]
    # It parses back into a strict DeploymentRecord (extra=forbid) — proving the
    # wire shape is exactly the C4 contract.
    parsed = DeploymentRecord(**rec)
    assert parsed.tenant_id == "acme"
    assert parsed.deployment_id == "acme-use1-0001"
    assert parsed.version == "1.4.2"  # from the VERSION file
    assert parsed.region == "us-east-1"
    assert parsed.telemetry_tier == TelemetryTier.T1
    assert parsed.health == Health.GREEN  # fresh heartbeat + healthy SLI + valid license
    # license_expiry came from the verified license.
    assert parsed.license_expiry > _dt.datetime.now(_dt.timezone.utc)


def test_version_falls_back_to_env_then_unknown(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)
    # Remove the VERSION file; with no env fallback -> "unknown".
    agent.config.version_file.unlink()
    assert agent.read_version() == "unknown"


def test_sli_breach_degrades_to_yellow(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console, healthy_sli=False)
    result = agent.tick()
    parsed = DeploymentRecord(**fake_console.received[0])
    assert result.sli_breached
    assert parsed.health == Health.YELLOW  # local SLI breach degrades green->yellow


def test_expired_license_forces_red_and_blocks_privileged(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console, expires_in_days=-1)
    result = agent.tick()
    assert result.licensed is False
    # The deployment still heartbeats (so the console can SEE it's unlicensed)...
    assert len(fake_console.received) == 1
    # ...but health is red because the license is expired.
    parsed = DeploymentRecord(**fake_console.received[0])
    assert parsed.health == Health.RED


# --------------------------------------------------------------------------- #
# I3: buffer when console unreachable, flush on reconnect, never crash         #
# --------------------------------------------------------------------------- #


def test_buffers_when_console_unreachable(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)
    fake_console.up = False  # console is down

    # Several ticks while down: each buffers, none crash, nothing delivered.
    for _ in range(3):
        result = agent.tick()
        assert not result.delivered and result.buffered
    assert fake_console.received == []
    assert agent.buffer.count() == 3  # all parked durably


def test_flushes_backlog_on_reconnect_oldest_first(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)

    fake_console.up = False
    agent.tick()  # buffered #1
    agent.tick()  # buffered #2
    assert agent.buffer.count() == 2

    fake_console.up = True
    result = agent.tick()  # flush backlog (2) + send live (1)
    assert result.delivered
    assert result.flushed == 2
    assert agent.buffer.is_empty()
    # 3 heartbeats total reached the console, oldest-first preserved.
    assert len(fake_console.received) == 3


def test_backoff_grows_while_down_and_resets_on_success(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)
    base = agent.config.backoff_base_s

    fake_console.up = False
    agent.tick()
    s1 = agent.next_sleep_s()
    agent.tick()
    s2 = agent.next_sleep_s()
    assert s2 >= s1  # backoff grows (capped) while buffering

    fake_console.up = True
    agent.tick()
    # Backlog drained -> steady-state interval, backoff reset.
    assert agent._backoff_s == base
    assert agent.next_sleep_s() == agent.config.interval_s


def test_run_forever_bounded_survives_outage(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)
    fake_console.up = False
    # The loop must not raise even though every delivery fails.
    ticks = agent.run_forever(max_ticks=4)
    assert ticks == 4
    assert agent.buffer.count() == 4


def test_sender_exception_does_not_crash_tick(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)

    def exploding_sender(_rec):
        raise RuntimeError("kaboom")

    agent.sender = exploding_sender
    result = agent.tick()  # must be caught, buffered, no raise
    assert not result.delivered and result.buffered
    assert agent.buffer.count() == 1


# --------------------------------------------------------------------------- #
# License-gated privileged action: config pull (I6)                            #
# --------------------------------------------------------------------------- #


def test_config_pull_refused_when_unlicensed(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console, expires_in_days=-1)
    bundle = make_config_bundle(signing_fabric, tmp_path / "bundle.json")

    # Wire the puller's fetcher to serve the (valid) bundle from disk...
    sig = bundle.parent / (bundle.name + ".sig")
    man = bundle.parent / (bundle.name + ".manifest.json")
    agent.config_puller._fetcher = lambda _u: (
        bundle.read_bytes(),
        sig.read_text(),
        man.read_bytes(),
    )
    # ...but the agent is unlicensed, so it refuses to even attempt the pull.
    assert agent.pull_config("https://console/config") is False
    assert agent.config_puller.load_applied_config() is None


def test_config_pull_applies_when_licensed(signing_fabric, tmp_path, fake_console):
    agent = _build_agent(signing_fabric, tmp_path, fake_console)
    bundle = make_config_bundle(
        signing_fabric, tmp_path / "bundle.json", payload={"interval_s": 42}
    )
    sig = bundle.parent / (bundle.name + ".sig")
    man = bundle.parent / (bundle.name + ".manifest.json")
    agent.config_puller._fetcher = lambda _u: (
        bundle.read_bytes(),
        sig.read_text(),
        man.read_bytes(),
    )
    assert agent.pull_config("https://console/config") is True
    assert agent.config_puller.load_applied_config() == {"interval_s": 42}
