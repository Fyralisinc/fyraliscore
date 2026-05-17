"""Tests for the M2 local dev stack (docker-compose.dev.yml +
create-kafka-topics.sh).

These tests assume the dev stack is already running locally
(`bash scripts/dev/m2-up.sh`). They skip cleanly if Docker isn't
available or the Kafka container isn't healthy — running them with
the stack down is a no-op rather than a failure.

`@pytest.mark.requires_docker` per the marker contract in
pyproject.toml. Run via `pytest -m requires_docker`.
"""
from __future__ import annotations

import pathlib
import shutil
import subprocess

import pytest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
COMPOSE_FILE = REPO_ROOT / "docker-compose.dev.yml"
CREATE_TOPICS = REPO_ROOT / "scripts" / "dev" / "create-kafka-topics.sh"
# M3.1 added `ingestion.dlq`. Per-topic retention introduced here:
# raw + normalized stay at 7 days (LLD §10), dlq is 30 days because
# ops triage windows can run longer than a week (LLD §1.3).
EXPECTED_TOPICS = ["ingestion.raw", "ingestion.normalized", "ingestion.dlq"]
EXPECTED_PARTITIONS_DEV = 4
EXPECTED_RETENTION_BY_TOPIC = {
    "ingestion.raw":        7 * 24 * 60 * 60 * 1000,   # 7 days
    "ingestion.normalized": 7 * 24 * 60 * 60 * 1000,   # 7 days
    "ingestion.dlq":       30 * 24 * 60 * 60 * 1000,   # 30 days
}
KAFKA_CONTAINER = "fyralis_dev_kafka"


pytestmark = pytest.mark.requires_docker


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _kafka_container_healthy() -> bool:
    """`docker inspect` succeeds AND .State.Health.Status == 'healthy'."""
    if not _docker_available():
        return False
    try:
        out = subprocess.run(
            [
                "docker", "inspect",
                "-f", "{{if .State.Health}}{{.State.Health.Status}}{{end}}",
                KAFKA_CONTAINER,
            ],
            check=True, capture_output=True, text=True, timeout=5,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    return out.stdout.strip() == "healthy"


@pytest.fixture(autouse=True)
def _require_running_kafka():
    if not _docker_available():
        pytest.skip("docker CLI not available")
    if not _kafka_container_healthy():
        pytest.skip(
            f"{KAFKA_CONTAINER} not running healthy — "
            "start with `bash scripts/dev/m2-up.sh`"
        )


def _list_topics() -> list[str]:
    out = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--list",
        ],
        check=True, capture_output=True, text=True, timeout=15,
    )
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]


def _describe_topic(name: str) -> str:
    out = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--topic", name,
        ],
        check=True, capture_output=True, text=True, timeout=15,
    )
    return out.stdout


# ---------------------------------------------------------------------
# Test 1: setup script is idempotent and produces the expected topics.
# Per the M2.0 work order — "The script should be idempotent
# (use --if-not-exists flag)."
# ---------------------------------------------------------------------

def test_kafka_topics_exist_after_setup_script():
    """Run the topic-creation script. Assert the two M2 topics show up
    in `kafka-topics.sh --list`. Run a second time to prove
    idempotency.
    """
    # First run — may create or be a no-op.
    subprocess.run(
        ["bash", str(CREATE_TOPICS)],
        check=True, capture_output=True, text=True, timeout=60,
    )
    topics = _list_topics()
    for t in EXPECTED_TOPICS:
        assert t in topics, f"topic {t!r} missing after script run; saw {topics}"

    # Second run — must not error (idempotency: --if-not-exists).
    second = subprocess.run(
        ["bash", str(CREATE_TOPICS)],
        capture_output=True, text=True, timeout=60,
    )
    assert second.returncode == 0, (
        f"second run of create-kafka-topics.sh failed (idempotency "
        f"contract broken): stderr={second.stderr}"
    )


# ---------------------------------------------------------------------
# Test 2: topic configuration matches the spec — partition count,
# compression type, and retention. Per the M2.0 + M3.1 work orders.
# The dev stack's retention is the contract, NOT the broker default —
# bind the assertion to the promise, not to behaviour that happens to
# coincide.
# ---------------------------------------------------------------------


def _topic_config(topic: str) -> dict[str, str]:
    """Return the key/value config overrides set on `topic`.

    `kafka-configs.sh --describe` emits lines like:
        Dynamic configs for topic ingestion.raw are:
          retention.ms=604800000 sensitive=false synonyms={...}
          compression.type=zstd sensitive=false synonyms={...}
    We strip everything after the first whitespace per line and parse
    the `key=value` head.
    """
    out = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--entity-type", "topics",
            "--entity-name", topic,
        ],
        check=True, capture_output=True, text=True, timeout=15,
    )
    cfg: dict[str, str] = {}
    for raw_line in out.stdout.splitlines():
        line = raw_line.strip()
        if "=" not in line:
            continue
        # Take the leading `key=value` token; ignore trailing metadata
        # ("sensitive=false synonyms={...}") which follows whitespace.
        head = line.split()[0]
        key, _, value = head.partition("=")
        if key and value:
            cfg[key] = value
    return cfg


def test_kafka_topic_config_matches_spec():
    """Per M2.0 + M3.1 work-order spec:
      - ingestion.raw         : 4 partitions, 7-day retention,  zstd.
      - ingestion.normalized  : 4 partitions, 7-day retention,  zstd.
      - ingestion.dlq         : 4 partitions, 30-day retention, zstd.

    Three assertions per topic (partition count, retention.ms,
    compression.type). All three bind to what the dev stack promises,
    not to broker defaults.
    """
    for topic in EXPECTED_TOPICS:
        # ---- partition count ----
        desc = _describe_topic(topic)
        partition_count: int | None = None
        for line in desc.splitlines():
            if "PartitionCount:" in line:
                parts = line.split()
                for i, tok in enumerate(parts):
                    if tok == "PartitionCount:" and i + 1 < len(parts):
                        partition_count = int(parts[i + 1])
                        break
        assert partition_count == EXPECTED_PARTITIONS_DEV, (
            f"{topic} has {partition_count} partitions; expected "
            f"{EXPECTED_PARTITIONS_DEV} per dev spec. Describe "
            f"output:\n{desc}"
        )

        # ---- per-topic config overrides (compression, retention) ----
        cfg = _topic_config(topic)
        assert cfg.get("compression.type") == "zstd", (
            f"{topic} compression.type must be 'zstd' per dev spec; "
            f"got {cfg.get('compression.type')!r}. Full config: {cfg}"
        )
        expected_retention = EXPECTED_RETENTION_BY_TOPIC[topic]
        retention_ms = int(cfg.get("retention.ms", "0"))
        assert retention_ms == expected_retention, (
            f"{topic} retention must be {expected_retention}ms per "
            f"work-order spec; got {retention_ms}ms. Bind to what we "
            f"promise, not what the broker defaults to."
        )
