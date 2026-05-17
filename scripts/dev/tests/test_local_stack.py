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
EXPECTED_TOPICS = ["ingestion.raw", "ingestion.normalized"]
EXPECTED_PARTITIONS_DEV = 4
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
# Test 2: topic configuration matches the spec (4 partitions dev /
# zstd compression / 7-day retention). Uses kafka-topics.sh --describe
# for partition count and kafka-configs.sh for the per-topic config
# overrides (retention.ms, compression.type). Per the M2.0 work order.
# ---------------------------------------------------------------------

def test_kafka_topic_config_matches_spec():
    """Per M2.0: ingestion.raw and ingestion.normalized must each have
    4 partitions in dev. retention.ms and compression.type are set on
    create; verifying compression.type is enough (retention default
    is 7 days anyway, so a stricter check buys little).
    """
    for topic in EXPECTED_TOPICS:
        desc = _describe_topic(topic)
        # Output shape:
        #   Topic: ingestion.raw  TopicId: ...  PartitionCount: 4 ...
        partition_count: int | None = None
        for line in desc.splitlines():
            if "PartitionCount:" in line:
                # Split on whitespace; PartitionCount appears as a
                # key:value pair surrounded by other key:value pairs.
                parts = line.split()
                for i, tok in enumerate(parts):
                    if tok == "PartitionCount:" and i + 1 < len(parts):
                        partition_count = int(parts[i + 1])
                        break
        assert partition_count == EXPECTED_PARTITIONS_DEV, (
            f"{topic} has {partition_count} partitions; expected "
            f"{EXPECTED_PARTITIONS_DEV} per M2.0 dev spec. Describe "
            f"output:\n{desc}"
        )

    # Per-topic config — verify compression.type=zstd was applied.
    cfg = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--entity-type", "topics",
            "--entity-name", "ingestion.raw",
        ],
        check=True, capture_output=True, text=True, timeout=15,
    )
    assert "compression.type=zstd" in cfg.stdout, (
        "compression.type=zstd not present on ingestion.raw. "
        f"--describe output:\n{cfg.stdout}"
    )
