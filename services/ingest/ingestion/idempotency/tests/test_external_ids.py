"""Byte-for-byte guard for the centralized external_id constructors.

These strings ARE the dedup key the observations repo enforces via
`UNIQUE (source_channel, external_id)`. A single changed byte either
splits one logical event into two observations or merges two distinct
events into one — both silent data-integrity bugs. So this test pins the
exact output of every constructor, including the null / version-branch
encodings. If a format must legitimately change, that is a dedup-breaking
migration and this test is the place it gets noticed.

Parity with the live handlers (webhook vs. backfill vs. poll) is covered
separately by `normalizer/tests/test_backfill_external_id_parity.py`.
"""
from __future__ import annotations

from services.ingest.ingestion import idempotency as idem


# --- Immutable / namespaced keys -------------------------------------
def test_slack_message() -> None:
    assert idem.slack_message("C123", "1700000000.000100") == "C123:1700000000.000100"


def test_gmail_message_namespaced_by_install() -> None:
    assert idem.gmail_message("inst-1", "abc@mail.example") == "gmail:inst-1:abc@mail.example"


def test_discord_event_shared_by_interaction_and_message() -> None:
    assert idem.discord_event("9001") == "discord:9001"


def test_notion_object_each_type() -> None:
    assert idem.notion_object("page", "p1") == "notion:page:p1"
    assert idem.notion_object("block", "b1") == "notion:block:b1"
    assert idem.notion_object("comment", "c1") == "notion:comment:c1"


def test_github_push_and_none_guard() -> None:
    assert idem.github_push("octo/repo", "deadbeef") == "octo/repo@deadbeef"
    assert idem.github_push(None, "deadbeef") is None
    assert idem.github_push("octo/repo", None) is None
    assert idem.github_push("", "") is None


def test_google_drive_revision_immutable() -> None:
    assert idem.google_drive_revision("f1", "r7") == "gdrive-revision:f1:r7"


def test_jira_transition_immutable() -> None:
    assert idem.jira_transition("acme.atlassian.net", "10001", "h42") == (
        "jira:acme.atlassian.net:transition:10001:h42"
    )


# --- Versioned keys — the mutable-source dedup lesson ----------------
def test_grafana_annotation_versioned_by_time_with_none_fallback() -> None:
    assert idem.grafana_annotation("graf.acme", "55", 1700000000000) == (
        "grafana:graf.acme:annotation:55:1700000000000"
    )
    # Falsy time (0 / None / "") collapses to the literal "none".
    assert idem.grafana_annotation("graf.acme", "55", 0) == "grafana:graf.acme:annotation:55:none"
    assert idem.grafana_annotation("graf.acme", "55", None) == "grafana:graf.acme:annotation:55:none"


def test_grafana_alert_versioned_by_status_and_ts() -> None:
    assert idem.grafana_alert("graf.acme", "h9", "firing", "2026-06-02T00:00:00+00:00") == (
        "grafana:graf.acme:alert:h9:firing:2026-06-02T00:00:00+00:00"
    )


def test_google_calendar_event_versioned_by_status_and_start() -> None:
    assert idem.google_calendar_event("alice@acme.com", "ev1", "confirmed", "2026-06-02T10:00:00+00:00") == (
        "gcal:alice@acme.com:ev1:confirmed:2026-06-02T10:00:00+00:00"
    )
    assert idem.google_calendar_event("alice@acme.com", "ev1", "cancelled", "none") == (
        "gcal:alice@acme.com:ev1:cancelled:none"
    )


def test_google_drive_file_version_branches() -> None:
    # Normal file: keyed on Drive's monotonic version.
    assert idem.google_drive_file("f1", version=12, removed=False, change_time=None) == "gdrive:f1:12"
    # Missing version → v0.
    assert idem.google_drive_file("f1", version=None, removed=False, change_time=None) == "gdrive:f1:v0"
    # Removal with no version → keyed on change time.
    assert idem.google_drive_file("f1", version=None, removed=True, change_time="2026-06-02T00:00:00Z") == (
        "gdrive:f1:removed:2026-06-02T00:00:00Z"
    )
    # Removal with no version and no/blank change time → "now".
    assert idem.google_drive_file("f1", version=None, removed=True, change_time=None) == "gdrive:f1:removed:now"
    assert idem.google_drive_file("f1", version=None, removed=True, change_time="") == "gdrive:f1:removed:now"
    # A removal that still carries a version takes the normal version path.
    assert idem.google_drive_file("f1", version=4, removed=True, change_time=None) == "gdrive:f1:4"


def test_google_drive_comment_versioned_by_modified() -> None:
    assert idem.google_drive_comment("f1", "c2", "2026-06-02T00:00:00Z") == (
        "gdrive-comment:f1:c2:2026-06-02T00:00:00Z"
    )
    assert idem.google_drive_comment("f1", "c2", "none") == "gdrive-comment:f1:c2:none"


def test_jira_issue_and_comment_versioned_by_updated_with_none_fallback() -> None:
    assert idem.jira_issue("acme.atlassian.net", "10001", "2026-06-02T00:00:00.000+0000") == (
        "jira:acme.atlassian.net:issue:10001:2026-06-02T00:00:00.000+0000"
    )
    assert idem.jira_issue("acme.atlassian.net", "10001", None) == (
        "jira:acme.atlassian.net:issue:10001:none"
    )
    assert idem.jira_comment("acme.atlassian.net", "555", None) == (
        "jira:acme.atlassian.net:comment:555:none"
    )


def test_mercury_keys() -> None:
    assert idem.mercury_transaction("acct1", "txn9", "posted") == "mercury:acct1:txn:txn9:posted"
    assert idem.mercury_balance("acct1", "2026-06-02") == "mercury:acct1:balance:2026-06-02"


def test_quickbooks_entity_and_thin_change() -> None:
    assert idem.quickbooks_entity("realm1", "invoice", "42", "3") == "qbo:realm1:invoice:42:3"
    assert idem.quickbooks_change("realm1", "invoice", "42", "2026-06-02T00:00:00Z") == (
        "qbo:realm1:invoice:42:chg:2026-06-02T00:00:00Z"
    )


# --- Surface invariants ----------------------------------------------
def test_all_exports_callable() -> None:
    for name in idem.__all__:
        assert callable(getattr(idem, name)), name
