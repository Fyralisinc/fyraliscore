"""Static gates for the contract-driven source-catalog migration.

The integration migration suite exercises the SQL against Postgres. These
tests also run in lightweight PR jobs and catch the two costly regressions:
losing a canonical source while editing the seed, or reintroducing a copied
``CHECK (source IN (...))`` list.
"""
from __future__ import annotations

import re
from pathlib import Path

from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


_ROOT = Path(__file__).resolve().parents[4]
_MIGRATION = (
    _ROOT / "db/migrations/0193_contract_driven_source_catalog.sql"
)


def _sql() -> str:
    return _MIGRATION.read_text(encoding="utf-8")


def test_catalog_migration_seeds_exactly_the_27_canonical_sources() -> None:
    source_ids = set(
        re.findall(r"^\s+\('([a-z_]+)',\s+'[^']+',", _sql(), re.MULTILINE)
    )
    assert source_ids == set(CANONICAL_SOURCE_IDS)


def test_catalog_migration_replaces_source_checks_with_foreign_keys() -> None:
    sql = _sql()
    assert "CHECK (source IN (" not in sql
    for table in (
        "source_onboarding_runs",
        "onboarding_shards",
        "ingestion_failures",
        "onboarding_triggers",
    ):
        assert f"{table}_source_catalog_fk" in sql
    assert sql.count("REFERENCES ingestion_source_catalog(id)") == 4


def test_catalog_migration_adds_durable_retry_and_lease_state() -> None:
    sql = _sql()
    for column in (
        "next_attempt_at",
        "attempt_count",
        "retry_reason",
        "lease_owner",
        "lease_version",
        "lease_expires_at",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
