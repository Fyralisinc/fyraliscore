"""IN-13 substrate-precondition assertions (T004–T006).

Read-only checks that the GitHub integration's prerequisites — schema
shape, dependency presence — are in place before deeper integration
tests run. Live Postgres required for the column-presence assertions
(mark `integration`); pyjwt import is a pure-Python check.
"""
from __future__ import annotations

import pytest


pytestmark = pytest.mark.integration


async def test_provider_installations_has_selected_repositories_column(
    db_pool,
) -> None:
    """T004: confirm migration 0042 ran in the test DB."""
    row = await db_pool.fetchrow(
        """
        SELECT data_type
          FROM information_schema.columns
         WHERE table_name = 'provider_installations'
           AND column_name = 'selected_repositories'
        """
    )
    assert row is not None, (
        "migration 0042_provider_installations_selected_repositories.sql "
        "did not run against the test DB"
    )
    assert row["data_type"] == "jsonb"


async def test_oauth_install_states_provider_column_present(db_pool) -> None:
    """T005 / R4: state-token nonce table has the provider column so
    Slack-issued state tokens cannot be replayed against GitHub callbacks."""
    row = await db_pool.fetchrow(
        """
        SELECT data_type, is_nullable
          FROM information_schema.columns
         WHERE table_name = 'oauth_install_states'
           AND column_name = 'provider'
        """
    )
    assert row is not None, "oauth_install_states.provider column missing"
    assert row["data_type"] == "text"
    assert row["is_nullable"] == "NO"


async def test_installation_audit_log_action_check_widened(db_pool) -> None:
    """T005 extension: migration 0043 widened the action CHECK to
    include the GitHub lifecycle vocabulary."""
    row = await db_pool.fetchrow(
        """
        SELECT pg_get_constraintdef(c.oid) AS def
          FROM pg_constraint c
         WHERE c.conrelid = 'installation_audit_log'::regclass
           AND c.conname = 'installation_audit_log_action_check'
        """
    )
    assert row is not None, (
        "installation_audit_log_action_check constraint missing"
    )
    cdef = row["def"]
    for expected in (
        "'reinstall'",
        "'suspend'",
        "'unsuspend'",
        "'repo_change'",
        "'repository_fetch_failed'",
        "'installation_created_noop'",
    ):
        assert expected in cdef, (
            f"action CHECK missing {expected} after migration 0043"
        )


async def test_observations_unique_index_present(db_pool) -> None:
    """T006: confirm dedup index on (source_channel, external_id,
    occurred_at) exists so the existing GitHub handler's idempotency
    contract holds."""
    row = await db_pool.fetchrow(
        """
        SELECT indexdef
          FROM pg_indexes
         WHERE tablename = 'observations'
           AND indexname = 'observations_source_channel_external_id_occurred_at_key'
        """
    )
    assert row is not None, (
        "observations dedup unique index missing"
    )
    assert "source_channel" in row["indexdef"]
    assert "external_id" in row["indexdef"]


def test_pyjwt_importable() -> None:
    """T004 (cousin): the new dep is installed and supports RS256."""
    import jwt as pyjwt
    # No exception means the crypto extra is available.
    assert hasattr(pyjwt, "encode")
    assert hasattr(pyjwt, "decode")
