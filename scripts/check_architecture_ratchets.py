#!/usr/bin/env python3
"""Mechanical architecture ratchets for Fyralis Core.

These checks intentionally start small. They encode contracts that are already
mostly true so future cleanup can remove allowlist entries instead of rediscovering
the same drift.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOTS = ("services", "lib", "scripts", "tests", "benchmarks")
CLIENT_ASSET_ROOTS = ("ui", "services/app", "services/product")
CLIENT_ASSET_SUFFIXES = (".html", ".js", ".jsx", ".ts", ".tsx", ".py")
IGNORED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "node_modules",
    "site",
    "truss_run",
    "truss_run_2",
}

RAW_THINK_TRIGGER_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+think_trigger_queue\b",
    re.IGNORECASE,
)
RAW_MODEL_REEVAL_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+model_reeval_queue\b",
    re.IGNORECASE,
)
RAW_PENDING_POST_COMMIT_ACTION_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+pending_post_commit_actions\b",
    re.IGNORECASE,
)
RAW_THINK_OBLIGATION_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+think_obligations\b",
    re.IGNORECASE,
)
MIGRATION_FILENAME_RE = re.compile(r"^(?P<prefix>\d{4})_.+\.sql$")
STRICT_RLS_BASELINE_MIGRATION_PREFIX = 164
PLAINTEXT_SECRET_COLUMN_BASELINE_MIGRATION_PREFIX = 166
PERMISSIVE_UNBOUND_TENANT_POLICY_RE = re.compile(
    r"(?:NULLIF\s*\(\s*)?current_setting\s*\(\s*'app\.current_tenant'[^)]*\)"
    r"(?:\s*,\s*''\s*\))?\s+IS\s+NULL",
    re.IGNORECASE,
)
PLAINTEXT_SECRET_COLUMN_RE = re.compile(
    r"""
    (?:\bADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?|^\s*,?\s*)
    (?P<name>"?[A-Za-z_][A-Za-z0-9_]*"?)
    \s+
    (?:TEXT|VARCHAR|BYTEA|JSONB|JSON|CHARACTER\s+VARYING)
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
SECRET_LIKE_COLUMN_NAME_RE = re.compile(
    r"(?:secret|token|password|api_key|private_key|client_secret|signing_secret)",
    re.IGNORECASE,
)
SECRET_COLUMN_ALLOWED_SUFFIXES = (
    "_ref",
    "_refs",
    "_hash",
    "_digest",
    "_fingerprint",
    "_ciphertext",
    "_encrypted",
    "_last4",
    "_scope",
    "_scopes",
    "_status",
    "_type",
)
SECRET_REF_KEYWORD_NAME_RE = re.compile(
    r"(?:^secret_ref$|_secret_ref$|_token_ref$|_session_ref$|_public_key_ref$)",
    re.IGNORECASE,
)
SECRET_REF_UNSAFE_VALUE_NAME_RE = re.compile(
    r"(?:secret|token|api_key|api_hash|client_secret|signing_secret|session|"
    r"public_key)",
    re.IGNORECASE,
)
SECRET_REF_SAFE_VALUE_SUFFIXES = ("_ref", "_refs")
DESTRUCTIVE_MIGRATION_MARKER = "destructive-migration-approved:"
DESTRUCTIVE_MIGRATION_REQUIRED_MARKER_FIELDS = ("backup=", "rollback=", "owner=")
DESTRUCTIVE_MIGRATION_ALLOWED_FILES = {
    Path("db/migrations/0026_single_demo_company.sql"),
    Path("db/migrations/0048_four_stance_propositions.sql"),
    Path("db/migrations/0093_drop_demo_scaffolding.sql"),
    Path("db/migrations/0111_sage_query_text_indexes.sql"),
    Path("db/migrations/0113_compact_model_search_documents.sql"),
    Path("db/migrations/0115_model_belief_addresses.sql"),
    Path("db/migrations/0116_search_document_full_text_indexes.sql"),
    Path("db/migrations/0123_sage_sparse_lookup_indexes.sql"),
    Path("db/migrations/0127_drop_retired_routing_topology_queues.sql"),
    Path("db/migrations/0159_customer_commitments_revenue_precision.sql"),
}
DESTRUCTIVE_MIGRATION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE),
        "DROP TABLE",
    ),
    (
        re.compile(r"\bDROP\s+COLUMN\b", re.IGNORECASE),
        "DROP COLUMN",
    ),
    (
        re.compile(r"\bDROP\s+INDEX\b", re.IGNORECASE),
        "DROP INDEX",
    ),
    (
        re.compile(r"\bTRUNCATE\b", re.IGNORECASE),
        "TRUNCATE",
    ),
    (
        re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE),
        "DELETE FROM",
    ),
    (
        re.compile(r"\bALTER\s+COLUMN\b.*\bTYPE\b", re.IGNORECASE),
        "ALTER COLUMN TYPE",
    ),
)
ACCESS_READ_CALL_NAMES = {"can_read", "can_read_by_id"}

NETWORK_CALL_MODULE_PREFIXES = (
    "httpx",
    "requests",
    "openai",
    "anthropic",
    "aioboto3",
    "boto3",
)
NETWORK_CALL_SUFFIXES = (
    ".embed",
    ".embed_many",
    ".embed_text",
    ".complete",
    ".complete_json",
    ".file_text",
    ".get_object",
    ".get_verified",
    ".put_if_absent",
    ".put_object",
    ".render_card",
    ".render_card_reasoning",
    ".render_close_line",
    ".render_conversation_turn",
    ".render_greeting",
    ".render_query_grid",
    ".submit_jsonl",
)
NETWORK_CALL_FUNCTION_NAMES = {
    "fetch_page_aws",
    "fetch_page_ashby",
    "fetch_page_brex",
    "fetch_page_carta",
    "fetch_page_deel",
    "fetch_page_discord",
    "fetch_page_figma",
    "fetch_page_fireflies",
    "fetch_page_github",
    "fetch_page_gmail",
    "fetch_page_google_calendar",
    "fetch_page_google_drive",
    "fetch_page_grafana",
    "fetch_page_gusto",
    "fetch_page_hibob",
    "fetch_page_jira",
    "fetch_page_linkedin",
    "fetch_page_mercury",
    "fetch_page_miro",
    "fetch_page_notion",
    "fetch_page_quickbooks",
    "fetch_page_ramp",
    "fetch_page_signal",
    "fetch_page_slack",
    "fetch_page_telegram",
    "publish_dlq",
    "publish_embedding_request",
    "publish_progress_event",
    "publish_progress_events",
    "publish_summarization_request",
}
NETWORK_CALL_METHOD_NAMES = {
    "get_messages",
    "produce",
    "request_bytes",
    "retrieve_bot_user",
    "retrieve_page",
    "submit_jsonl",
    "watch",
    "watch_changes",
    "watch_events",
}
NETWORK_CALL_OBJECT_METHODS = {
    "batch_client.retrieve",
    "client.retrieve",
    "raw_s3.get",
    "s3.get",
    "s3_client.get",
}
METRIC_CREATION_CALL_NAMES = {
    "counter",
    "gauge",
    "histogram",
    "Counter",
    "Gauge",
    "Histogram",
}
FORBIDDEN_METRIC_LABEL_NAMES = {
    "tenant",
    "tenant_id",
    "actor_id",
    "user_id",
    "installation_id",
    "account_id",
    "external_id",
    "email",
    "owner_email",
    "query",
    "prompt",
    "payload",
    "body",
    "channel",
    "channel_name",
    "path",
    "url",
    "object_key",
    "source_payload",
    "source_channel",
}
FORBIDDEN_METRIC_LABEL_SUFFIXES = ("_id", "_email", "_url", "_path")
CLIENT_TOKEN_STORAGE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\b(?:localStorage|sessionStorage)\s*\.", re.IGNORECASE),
        "browser auth/session tokens must not be stored in localStorage/sessionStorage",
    ),
    (
        re.compile(r"\?\s*token\s*=", re.IGNORECASE),
        "browser WebSocket/API auth must not put tokens in query strings",
    ),
    (
        re.compile(r"\bsearchParams\s*\.\s*set\s*\(\s*['\"]token['\"]", re.IGNORECASE),
        "browser WebSocket/API auth must not put tokens in query strings",
    ),
)
PRODUCT_DEFAULT_TENANT_FALLBACK_RE = re.compile(
    r"(?:\breturn\s+default_tenant_id\b|\btenant_id\s*=\s*default_tenant_id\b)",
    re.IGNORECASE,
)
BYOC_MANIFEST_PRIVACY_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"^\s*exposure\s*:\s*public\b", re.IGNORECASE),
        "BYOC manifests must not require public endpoint exposure",
    ),
    (
        re.compile(
            r"^\s*(?:raw_logs_allowed|raw_payloads_allowed|raw_prompts_allowed|"
            r"pii_allowed|control_plane_inbound_allowed|"
            r"raw_payloads_leave_boundary|prompts_leave_boundary|"
            r"embeddings_leave_boundary|logs_leave_boundary|pii_leaves_boundary|"
            r"provider_secrets_leave_boundary)\s*:\s*true\b",
            re.IGNORECASE,
        ),
        (
            "BYOC manifests must not allow customer data, logs, prompts, PII, "
            "or inbound control-plane access to leave the data plane"
        ),
    ),
)
BYOC_MANIFEST_DIRECTION_RE = re.compile(
    r"^\s*direction\s*:\s*(?P<value>\S+)",
    re.IGNORECASE,
)
BYOC_AGENT_CONTRACT_PATH = Path("services/platform/runtime/byoc_agent_contract.py")
BYOC_AGENT_TOKEN_ROTATION_PATH = Path(
    "services/platform/runtime/byoc_agent_token_rotation.py"
)
BYOC_LIVE_CREDENTIAL_REHEARSAL_PATH = Path(
    "services/platform/runtime/byoc_live_credential_rehearsal.py"
)
BYOC_CONTROL_PLANE_READ_SMOKE_SUMMARY_PATH = Path(
    "services/platform/runtime/byoc_control_plane_read_smoke_summary.py"
)
BYOC_CONTROL_PANEL_STATE_PATH = Path(
    "services/platform/runtime/byoc_control_panel_state.py"
)
BYOC_CONTROL_PANEL_ACCESS_PATH = Path(
    "services/platform/runtime/byoc_control_panel_access.py"
)
BYOC_CONTROL_PANEL_ACCESS_GRANT_MIGRATION_PATH = Path(
    "db/migrations/0185_byoc_control_panel_access_grants.sql"
)
BYOC_PRODUCT_HEALTH_PATH = Path("services/platform/runtime/byoc_product_health.py")
BYOC_PRODUCT_HEALTH_COLLECTOR_PATH = Path(
    "services/platform/runtime/byoc_product_health_collector.py"
)
BYOC_PRODUCT_HEALTH_MIGRATION_PATH = Path(
    "db/migrations/0186_byoc_product_health_snapshots.sql"
)
BYOC_LAUNCH_READINESS_SUMMARY_PATH = Path(
    "services/platform/runtime/byoc_launch_readiness_summary.py"
)
BYOC_CUSTOMER_PILOT_PACKAGE_PATH = Path(
    "services/platform/runtime/byoc_customer_pilot_package.py"
)
BYOC_CUSTOMER_PILOT_REHEARSAL_PATH = Path(
    "services/platform/runtime/byoc_customer_pilot_rehearsal.py"
)
BYOC_AWS_LIVE_PREFLIGHT_PATH = Path(
    "services/platform/runtime/byoc_aws_live_preflight.py"
)
BYOC_EVIDENCE_RECEIPT_MIGRATION_PATH = Path(
    "db/migrations/0180_byoc_evidence_package_receipts.sql"
)
BYOC_AGENT_REGISTRATION_MIGRATION_PATH = Path(
    "db/migrations/0181_byoc_agent_registrations.sql"
)
BYOC_RUNNER_EVIDENCE_RECEIPT_MIGRATION_PATH = Path(
    "db/migrations/0182_byoc_runner_evidence_receipts.sql"
)
BYOC_PREFLIGHT_REPORT_RECEIPT_MIGRATION_PATH = Path(
    "db/migrations/0183_byoc_preflight_report_receipts.sql"
)
BYOC_AGENT_FALSE_TELEMETRY_FLAGS = (
    "raw_logs_allowed",
    "raw_payloads_allowed",
    "raw_prompts_allowed",
    "pii_allowed",
)
BYOC_AWS_LIVE_PREFLIGHT_FALSE_PRIVACY_FLAGS = (
    "account_id_included",
    "caller_arn_included",
    "role_arn_included",
    "aws_profile_included",
    "aws_endpoint_urls_included",
    "credentials_included",
    "command_output_included",
    "policy_documents_included",
    "raw_customer_data_included",
)
BYOC_AWS_LIVE_PREFLIGHT_FORBIDDEN_REPORT_FIELD_FRAGMENTS = (
    "account_id",
    "arn",
    "aws_profile",
    "endpoint_url",
    "policy_document",
    "principal",
    "secret",
    "token",
)
BYOC_AGENT_TOKEN_ROTATION_FALSE_PRIVACY_FLAGS = (
    "raw_token_material_included",
    "secret_refs_included",
    "signatures_included",
    "request_bodies_included",
    "command_output_included",
    "cloud_credentials_included",
    "account_ids_included",
    "arns_included",
    "urls_included",
    "raw_payloads_included",
    "prompts_included",
    "logs_included",
    "pii_included",
)
BYOC_AGENT_TOKEN_ROTATION_TRUE_PRIVACY_FLAGS = (
    "secret_ref_digests_included",
)
BYOC_AGENT_TOKEN_ROTATION_FORBIDDEN_REPORT_FIELD_FRAGMENTS = (
    "raw_token",
    "token_material",
    "token_value",
    "install_token_value",
    "signature",
    "request_body",
    "response_body",
    "command_output",
    "account_id",
    "arn",
    "url",
    "credential",
)
BYOC_LIVE_CREDENTIAL_REHEARSAL_FALSE_PRIVACY_FLAGS = (
    "raw_payloads_included",
    "prompts_included",
    "embeddings_included",
    "raw_logs_included",
    "pii_included",
    "credentials_included",
    "account_ids_included",
    "arns_included",
    "urls_included",
    "policy_documents_included",
    "command_output_included",
    "child_report_details_included",
    "artifact_paths_included",
)
BYOC_LIVE_CREDENTIAL_REHEARSAL_FORBIDDEN_REPORT_FIELD_FRAGMENTS = (
    "account_id",
    "arn",
    "aws_profile",
    "principal_arn",
    "policy_document",
    "command_output",
    "child_report",
    "artifact_path",
    "url",
)
BYOC_LAUNCH_READINESS_SUMMARY_FALSE_PRIVACY_FLAGS = (
    "child_report_bodies_included",
    "artifact_bodies_included",
    "raw_reports_included",
    "raw_payloads_included",
    "request_bodies_included",
    "response_bodies_included",
    "signed_headers_included",
    "endpoint_urls_included",
    "raw_auth_material_included",
    "credentials_included",
    "account_ids_included",
    "arns_included",
    "command_output_included",
    "logs_included",
    "prompts_included",
    "embeddings_included",
    "pii_included",
)
BYOC_LAUNCH_READINESS_SUMMARY_FORBIDDEN_REPORT_FIELD_FRAGMENTS = (
    "child_report",
    "raw_report",
    "artifact_body",
    "artifact_ref",
    "request_body",
    "response_body",
    "signed_header",
    "endpoint_url",
    "auth_material",
    "credential",
    "account_id",
    "arn",
    "command_output",
    "log_text",
    "prompt",
    "embedding",
    "pii",
)
BYOC_CONTROL_PLANE_READ_SMOKE_SUMMARY_FALSE_PRIVACY_FLAGS = (
    "request_bodies_included",
    "response_bodies_included",
    "signed_headers_included",
    "endpoint_urls_included",
    "endpoint_paths_included",
    "query_strings_included",
    "raw_auth_material_included",
    "credentials_included",
    "account_ids_included",
    "arns_included",
    "command_output_included",
    "logs_included",
    "prompts_included",
    "embeddings_included",
    "pii_included",
)
BYOC_CONTROL_PLANE_READ_SMOKE_SUMMARY_FORBIDDEN_REPORT_FIELD_FRAGMENTS = (
    "request_body",
    "response_body",
    "signed_header",
    "endpoint_url",
    "endpoint_path",
    "query_string",
    "auth_material",
    "credential",
    "account_id",
    "arn",
    "command_output",
    "log_text",
    "prompt",
    "embedding",
    "pii",
)
BYOC_CONTROL_PANEL_STATE_FORBIDDEN_FIELD_FRAGMENTS = (
    "raw_report",
    "raw_payload",
    "raw_prompt",
    "request_body",
    "response_body",
    "signed_header",
    "endpoint_url",
    "auth_material",
    "credential",
    "secret_ref",
    "account_id",
    "arn",
    "command_output",
    "log_text",
    "prompt",
    "embedding",
    "pii",
)
BYOC_CONTROL_PANEL_ACCESS_FORBIDDEN_FIELD_FRAGMENTS = (
    "raw_report",
    "raw_payload",
    "raw_prompt",
    "request_body",
    "response_body",
    "signed_header",
    "read_key",
    "endpoint_url",
    "auth_material",
    "credential",
    "secret_ref",
    "account_id",
    "arn",
    "command_output",
    "log_text",
    "prompt",
    "embedding",
    "pii",
)
BYOC_PRODUCT_HEALTH_FALSE_PRIVACY_FLAGS = (
    "raw_payloads_included",
    "raw_prompts_included",
    "raw_logs_included",
    "pii_included",
    "source_records_included",
    "model_contents_included",
    "vector_values_included",
)
BYOC_PRODUCT_HEALTH_FORBIDDEN_FIELD_FRAGMENTS = (
    "raw_report",
    "raw_payload",
    "raw_prompt",
    "request_body",
    "response_body",
    "signed_header",
    "endpoint_url",
    "auth_material",
    "credential",
    "secret_ref",
    "account_id",
    "arn",
    "command_output",
    "log_text",
    "prompt",
    "pii",
)
BYOC_PRODUCT_HEALTH_COLLECTOR_FORBIDDEN_SQL_PATTERNS = (
    (r"\bselect\s+\*", "collector SQL must not select arbitrary columns"),
    (r"\bcontent\b", "collector SQL must not select observation payload content"),
    (r"\bcontent_text\b", "collector SQL must not select observation text"),
    (r"\berror_summary\b", "collector SQL must not select raw error summaries"),
    (r"\berror_context\b", "collector SQL must not select raw error context"),
    (r"\braw_s3_key\b", "collector SQL must not select raw object pointers"),
    (r"\bproposition\b", "collector SQL must not select model contents"),
    (r'"natural"', "collector SQL must not select model prose"),
    (r"\btoken\b", "collector SQL must not select token material"),
    (r"\bcredential\b", "collector SQL must not select credentials"),
    (r"\bsecret_ref\b", "collector SQL must not select secret references"),
)
BYOC_CUSTOMER_PILOT_PACKAGE_FALSE_PRIVACY_FLAGS = (
    "artifact_bodies_included",
    "child_report_bodies_included",
    "raw_reports_included",
    "raw_payloads_included",
    "request_bodies_included",
    "response_bodies_included",
    "signed_headers_included",
    "endpoint_urls_included",
    "raw_auth_material_included",
    "credentials_included",
    "account_ids_included",
    "arns_included",
    "command_output_included",
    "logs_included",
    "prompts_included",
    "embeddings_included",
    "pii_included",
)
BYOC_CUSTOMER_PILOT_PACKAGE_FORBIDDEN_REPORT_FIELD_FRAGMENTS = (
    "child_report",
    "raw_report",
    "artifact_body",
    "request_body",
    "response_body",
    "signed_header",
    "endpoint_url",
    "auth_material",
    "credential",
    "account_id",
    "arn",
    "command_output",
    "log_text",
    "prompt",
    "embedding",
    "pii",
)
BYOC_CUSTOMER_PILOT_REHEARSAL_FALSE_PRIVACY_FLAGS = (
    "artifact_bodies_included",
    "child_report_bodies_included",
    "raw_reports_included",
    "raw_payloads_included",
    "request_bodies_included",
    "response_bodies_included",
    "signed_headers_included",
    "endpoint_urls_included",
    "raw_auth_material_included",
    "credentials_included",
    "account_ids_included",
    "arns_included",
    "command_output_included",
    "logs_included",
    "prompts_included",
    "embeddings_included",
    "pii_included",
    "cloud_credentials_required",
    "mutating_cloud_commands_executed",
)
BYOC_CUSTOMER_PILOT_REHEARSAL_FORBIDDEN_REPORT_FIELD_FRAGMENTS = (
    "child_report",
    "raw_report",
    "artifact_body",
    "request_body",
    "response_body",
    "signed_header",
    "endpoint_url",
    "auth_material",
    "credential",
    "account_id",
    "arn",
    "command_output",
    "log_text",
    "prompt",
    "embedding",
    "pii",
)
BYOC_EVIDENCE_RECEIPT_FORBIDDEN_STORAGE_PATTERNS: tuple[
    tuple[re.Pattern[str], str],
    ...,
] = (
    (
        re.compile(r"\b(?:JSONB|JSON|BYTEA)\b", re.IGNORECASE),
        "BYOC evidence receipt storage must not store JSON or byte payload bodies",
    ),
    (
        re.compile(
            r"\b(?:raw_report|report_body|report_json|package_body|package_json|"
            r"ledger_body|ledger_json|source_artifacts|prompt|payload)\b",
            re.IGNORECASE,
        ),
        "BYOC evidence receipt storage must not include package/report body columns",
    ),
)
BYOC_AGENT_REGISTRATION_FORBIDDEN_STORAGE_PATTERNS: tuple[
    tuple[re.Pattern[str], str],
    ...,
] = (
    (
        re.compile(r"\b(?:JSONB|JSON|BYTEA)\b", re.IGNORECASE),
        "BYOC agent registration storage must not store JSON or byte payload bodies",
    ),
    (
        re.compile(
            r"\b(?:raw_[a-z0-9_]*|enrollment_body|heartbeat_body|request_body|"
            r"response_body|payload|prompt|log_text|pii|secret_value|"
            r"token_value|install_token_value|private_key|client_cert_body)\b",
            re.IGNORECASE,
        ),
        "BYOC agent registration storage must not include raw agent body columns",
    ),
)
BYOC_RUNNER_EVIDENCE_RECEIPT_FORBIDDEN_STORAGE_PATTERNS: tuple[
    tuple[re.Pattern[str], str],
    ...,
] = (
    (
        re.compile(r"\b(?:JSONB|JSON|BYTEA)\b", re.IGNORECASE),
        "BYOC runner evidence receipt storage must not store JSON or byte payload bodies",
    ),
    (
        re.compile(
            r"\b(?:raw_[a-z0-9_]*|runner_report|report_body|report_json|"
            r"checks|iterations|apply_plan_ids|artifact_verification_ids|"
            r"artifact_inventory|artifact_digest|request_body|response_body|"
            r"payload|prompt|log_text|pii|secret_value|token_value)\b",
            re.IGNORECASE,
        ),
        "BYOC runner evidence receipt storage must not include raw runner body columns",
    ),
)
BYOC_PREFLIGHT_REPORT_RECEIPT_FORBIDDEN_STORAGE_PATTERNS: tuple[
    tuple[re.Pattern[str], str],
    ...,
] = (
    (
        re.compile(r"\b(?:JSONB|JSON|BYTEA)\b", re.IGNORECASE),
        "BYOC preflight report receipt storage must not store JSON or byte payload bodies",
    ),
    (
        re.compile(
            r"\b(?:raw_[a-z0-9_]*|preflight_report|report_body|report_json|"
            r"child_report|section_details|checks|command_output|artifact_refs|"
            r"request_body|response_body|payload|prompt|log_text|pii|"
            r"secret_value|token_value)\b",
            re.IGNORECASE,
        ),
        "BYOC preflight report receipt storage must not include raw report body columns",
    ),
)
BYOC_CONTROL_PANEL_ACCESS_GRANT_FORBIDDEN_STORAGE_PATTERNS: tuple[
    tuple[re.Pattern[str], str],
    ...,
] = (
    (
        re.compile(r"\b(?:JSONB|JSON|BYTEA)\b", re.IGNORECASE),
        "BYOC control-panel access grants must not store JSON or byte payload bodies",
    ),
    (
        re.compile(
            r"\b(?:raw_[a-z0-9_]*|grant_body|request_body|response_body|"
            r"payload|prompt|log_text|pii|secret_value|token_value|read_key|"
            r"signature|signed_header|endpoint_url|auth_material|credential|"
            r"secret_ref|account_id|arn)\b",
            re.IGNORECASE,
        ),
        "BYOC control-panel access grants must not include sensitive columns",
    ),
)
BYOC_AGENT_NO_RAW_TOKEN_MODELS = (
    "ByocAgentEnrollmentPayload",
    "ByocAgentEnrollmentRequest",
    "ByocAgentHeartbeat",
)

RAW_THINK_TRIGGER_INSERT_ALLOWED_FILES = {
    Path("services/domain/triggers.py"),
    # Scale/probe harness that intentionally hand-builds batched rows.
    Path("scripts/run_1000_signal_model_layer_probe.py"),
}
RAW_MODEL_REEVAL_INSERT_ALLOWED_FILES = {
    Path("services/domain/triggers.py"),
    # Registry callbacks live in lib/shared to avoid lib -> services imports.
    Path("lib/shared/edge_registry.py"),
}
RAW_PENDING_POST_COMMIT_ACTION_INSERT_ALLOWED_FILES = {
    Path("services/reasoning/think/post_commit.py"),
}
RAW_THINK_OBLIGATION_INSERT_ALLOWED_FILES = {
    Path("services/domain/obligations.py"),
}
IMPORT_LINTER_IGNORE_IMPORT_LIMITS = {
    "core never imports the demo / simulation overlays": 0,
    "lib is independent of services (shared libraries never import app code)": 8,
    "reasoning core does not directly import the app, product, or ingest layers": 0,
    "domain does not add new imports of reasoning internals": 15,
    "domain does not add new imports of product code": 1,
    "ingest does not add new imports of app code": 47,
}
ACCESS_READ_AUDIT_EXEMPT_FILES = {
    Path("services/platform/access_control/checks.py"),
    Path("services/platform/access_control/extension_caps.py"),
}
PRODUCTION_ROLLBACK_AUTOMATION_FILES = (
    Path(".github/workflows/deploy-production.yml"),
    Path(".github/workflows/deploy-staging.yml"),
    Path("scripts/deploy_compose_release.sh"),
)
ROLLBACK_DATA_DELETION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\bdocker\s+compose\s+down\b[^\n]*(?:-v|--volumes)\b"),
        "docker compose volume wipe",
    ),
    (
        re.compile(r"\bdocker\s+volume\s+(?:rm|prune)\b"),
        "docker volume deletion",
    ),
    (
        re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE),
        "DROP TABLE",
    ),
    (
        re.compile(r"\bTRUNCATE\b", re.IGNORECASE),
        "TRUNCATE",
    ),
    (
        re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE),
        "DELETE FROM",
    ),
)


@dataclass(frozen=True)
class Violation:
    check: str
    path: Path
    line_number: int
    message: str

    def render(self) -> str:
        return f"{self.path}:{self.line_number}: {self.check}: {self.message}"


def _is_test_path(path: Path) -> bool:
    return (
        "tests" in path.parts
        or path.name.startswith("test_")
        or path.name.endswith("_test.py")
    )


def _iter_python_files(
    *,
    repo_root: Path,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> Iterable[Path]:
    for root_name in roots:
        root = repo_root / root_name
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            rel = path.relative_to(repo_root)
            if any(part in IGNORED_PARTS for part in rel.parts):
                continue
            yield rel


def _iter_client_asset_files(
    *,
    repo_root: Path,
    roots: Sequence[str] = CLIENT_ASSET_ROOTS,
) -> Iterable[Path]:
    for root_name in roots:
        root = repo_root / root_name
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in CLIENT_ASSET_SUFFIXES:
                continue
            rel = path.relative_to(repo_root)
            if any(part in IGNORED_PARTS for part in rel.parts):
                continue
            if _is_test_path(rel):
                continue
            yield rel


def _full_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _full_name(node.value)
        if prefix:
            return f"{prefix}.{node.attr}"
        return node.attr
    if isinstance(node, ast.Call):
        return _full_name(node.func)
    return None


def _is_transaction_context(expr: ast.AST) -> bool:
    name = _full_name(expr)
    if name is None:
        return False
    return (
        name == "transaction"
        or name.endswith(".transaction")
        or name == "tenant_transaction"
        or name.endswith(".tenant_transaction")
    )


def _network_call_name(call: ast.Call) -> str | None:
    name = _full_name(call.func)
    if name is None:
        return None
    first = name.split(".", 1)[0]
    short = name.rsplit(".", 1)[-1]
    if first in NETWORK_CALL_MODULE_PREFIXES:
        return name
    if name in NETWORK_CALL_OBJECT_METHODS:
        return name
    if short in NETWORK_CALL_FUNCTION_NAMES or short in NETWORK_CALL_METHOD_NAMES:
        return name
    if any(name.endswith(suffix) for suffix in NETWORK_CALL_SUFFIXES):
        return name
    return None


def _metric_call_name(call: ast.Call) -> str | None:
    name = _full_name(call.func)
    if name is None:
        return None
    short = name.rsplit(".", 1)[-1]
    return short if short in METRIC_CREATION_CALL_NAMES else None


def _literal_metric_label_names(call: ast.Call) -> list[tuple[str, int]]:
    label_node: ast.AST | None = call.args[2] if len(call.args) >= 3 else None
    for keyword in call.keywords:
        if keyword.arg == "label_names":
            label_node = keyword.value
            break
    if not isinstance(label_node, (ast.Tuple, ast.List)):
        return []
    labels: list[tuple[str, int]] = []
    for element in label_node.elts:
        if isinstance(element, ast.Constant) and isinstance(element.value, str):
            labels.append((element.value, element.lineno))
    return labels


def _metric_label_forbidden_reason(label_name: str) -> str | None:
    normalized = label_name.strip().lower()
    if normalized in FORBIDDEN_METRIC_LABEL_NAMES:
        return "forbidden metric label name"
    if normalized.endswith(FORBIDDEN_METRIC_LABEL_SUFFIXES):
        return "metric labels must not carry raw ids/emails/urls/paths"
    return None


def _access_read_call_line(tree: ast.AST) -> int | None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _full_name(node.func)
        if name is None:
            continue
        if name.rsplit(".", 1)[-1] in ACCESS_READ_CALL_NAMES:
            return node.lineno
    return None


class _NetworkInTransactionVisitor(ast.NodeVisitor):
    def __init__(self, *, path: Path) -> None:
        self.path = path
        self.transaction_depth = 0
        self.violations: list[Violation] = []

    def visit_With(self, node: ast.With) -> None:  # noqa: N802 - ast API
        self._visit_with_body(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:  # noqa: N802 - ast API
        self._visit_with_body(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 - ast API
        if self.transaction_depth > 0:
            call_name = _network_call_name(node)
            if call_name is not None:
                self.violations.append(
                    Violation(
                        check="network-call-in-transaction",
                        path=self.path,
                        line_number=node.lineno,
                        message=(
                            f"move {call_name} outside the database "
                            "transaction block"
                        ),
                    )
                )
        self.generic_visit(node)

    def _visit_with_body(self, node: ast.With | ast.AsyncWith) -> None:
        in_transaction = any(
            _is_transaction_context(item.context_expr) for item in node.items
        )
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self.visit(item.optional_vars)
        if in_transaction:
            self.transaction_depth += 1
        try:
            for statement in node.body:
                self.visit(statement)
        finally:
            if in_transaction:
                self.transaction_depth -= 1


def _find_raw_insert_violations(
    *,
    repo_root: Path,
    roots: Sequence[str],
    pattern: re.Pattern[str],
    allowed_files: set[Path],
    check: str,
    message: str,
) -> list[Violation]:
    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if rel in allowed_files or _is_test_path(rel):
            continue
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line):
                violations.append(
                    Violation(
                        check=check,
                        path=rel,
                        line_number=line_number,
                        message=message,
                    )
                )
    return violations


def find_network_call_in_transaction_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = ("services", "lib"),
) -> list[Violation]:
    """Return obvious network/LLM/embed calls inside transaction blocks.

    This is intentionally lexical and conservative. It prevents newly placing
    direct HTTP, object-storage, LLM, rendering, or embedding calls inside
    ``async with conn.transaction()`` / ``tenant_transaction()`` blocks without
    pretending to solve interprocedural transaction analysis.
    """

    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if _is_test_path(rel):
            continue
        try:
            tree = ast.parse(
                (repo_root / rel).read_text(encoding="utf-8"),
                filename=str(rel),
            )
        except SyntaxError as exc:
            violations.append(
                Violation(
                    check="network-call-in-transaction",
                    path=rel,
                    line_number=exc.lineno or 1,
                    message=f"could not parse Python file: {exc.msg}",
                )
            )
            continue
        visitor = _NetworkInTransactionVisitor(path=rel)
        visitor.visit(tree)
        violations.extend(visitor.violations)
    return violations


def find_access_read_without_override_audit_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = ("services",),
) -> list[Violation]:
    """Return read-access checks in production code without audit wiring.

    Admin/leadership/first-person override reads are sensitive enough that any
    production file calling ``can_read`` or ``can_read_by_id`` must also carry an
    override-audit path. This is a file-level ratchet because several routers
    centralize audit writes in local helper functions.
    """

    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if rel in ACCESS_READ_AUDIT_EXEMPT_FILES or _is_test_path(rel):
            continue
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(text, filename=str(rel))
        except SyntaxError as exc:
            violations.append(
                Violation(
                    check="access-read-override-audit",
                    path=rel,
                    line_number=exc.lineno or 1,
                    message=f"could not parse Python file: {exc.msg}",
                )
            )
            continue
        line = _access_read_call_line(tree)
        if line is None:
            continue
        if "record_override" in text:
            continue
        violations.append(
            Violation(
                check="access-read-override-audit",
                path=rel,
                line_number=line,
                message=(
                    "files that call can_read/can_read_by_id must also audit "
                    "override grants via record_override_if_needed or "
                    "record_override"
                ),
            )
        )
    return violations


def find_forbidden_metric_label_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = ("services", "lib", "scripts"),
) -> list[Violation]:
    """Return metric families declared with privacy-unsafe label names."""

    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if _is_test_path(rel):
            continue
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(text, filename=str(rel))
        except SyntaxError as exc:
            violations.append(
                Violation(
                    check="forbidden-metric-label",
                    path=rel,
                    line_number=exc.lineno or 1,
                    message=f"could not parse Python file: {exc.msg}",
                )
            )
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or _metric_call_name(node) is None:
                continue
            for label_name, line_number in _literal_metric_label_names(node):
                reason = _metric_label_forbidden_reason(label_name)
                if reason is None:
                    continue
                violations.append(
                    Violation(
                        check="forbidden-metric-label",
                        path=rel,
                        line_number=line_number,
                        message=f"{reason}: {label_name!r}",
                    )
                )
    return violations


def find_browser_token_storage_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = CLIENT_ASSET_ROOTS,
) -> list[Violation]:
    """Return first-party browser/client code that stores or URL-carries tokens."""

    violations: list[Violation] = []
    for rel in _iter_client_asset_files(repo_root=repo_root, roots=roots):
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        for line_number, line in enumerate(text.splitlines(), start=1):
            for pattern, message in CLIENT_TOKEN_STORAGE_PATTERNS:
                if rel.suffix == ".py" and "query strings" in message:
                    continue
                if pattern.search(line):
                    violations.append(
                        Violation(
                            check="browser-token-storage",
                            path=rel,
                            line_number=line_number,
                            message=message,
                        )
                    )
                    break
    return violations


def find_product_default_tenant_without_production_guard_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = ("services/product",),
) -> list[Violation]:
    """Return product routes that expose dogfood default tenants in production.

    Backend-owned product/browser routes may keep ``default_tenant_id`` for
    tests and local dogfood, but any file that falls back to it must also have
    an explicit production-mode check so miswired production mounts fail closed.
    """

    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if _is_test_path(rel):
            continue
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        match = PRODUCT_DEFAULT_TENANT_FALLBACK_RE.search(text)
        if match is None:
            continue
        if "_request_is_production" in text or "is_production" in text:
            continue
        line_number = text[: match.start()].count("\n") + 1
        violations.append(
            Violation(
                check="product-default-tenant-production-guard",
                path=rel,
                line_number=line_number,
                message=(
                    "product routes that fall back to default_tenant_id must "
                    "explicitly reject that fallback in production"
                ),
            )
        )
    return violations


def find_byoc_manifest_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    manifest_dir: Path = Path("deploy/byoc"),
) -> list[Violation]:
    """Return BYOC manifest examples that loosen egress/privacy defaults."""

    root = repo_root / manifest_dir
    if not root.exists():
        return []

    violations: list[Violation] = []
    manifest_paths = sorted(root.glob("*.yml"))
    manifest_paths.extend(sorted(root.glob("*.yaml")))
    manifest_paths.extend(sorted(root.glob("*.json")))
    for path in manifest_paths:
        rel = path.relative_to(repo_root)
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8", errors="ignore").splitlines(),
            start=1,
        ):
            direction_match = BYOC_MANIFEST_DIRECTION_RE.search(line)
            if (
                direction_match is not None
                and direction_match.group("value").lower() != "egress_only"
            ):
                violations.append(
                    Violation(
                        check="byoc-manifest-privacy",
                        path=rel,
                        line_number=line_number,
                        message=(
                            "BYOC control-plane connectivity must remain "
                            "egress_only"
                        ),
                    )
                )
                continue
            for pattern, message in BYOC_MANIFEST_PRIVACY_PATTERNS:
                if pattern.search(line):
                    violations.append(
                        Violation(
                            check="byoc-manifest-privacy",
                            path=rel,
                            line_number=line_number,
                            message=message,
                        )
                    )
                    break
    return violations


def find_byoc_agent_contract_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_AGENT_CONTRACT_PATH,
) -> list[Violation]:
    """Return BYOC agent contract drift that could expose raw customer data."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-agent-contract-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC agent contract module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    telemetry_class = classes.get("ByocAgentTelemetryState")
    telemetry_fields = _class_field_assignments(telemetry_class)
    violations: list[Violation] = []

    for field_name in BYOC_AGENT_FALSE_TELEMETRY_FLAGS:
        assignment = telemetry_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-agent-contract-privacy",
                    path=contract_path,
                    line_number=telemetry_class.lineno if telemetry_class else 1,
                    message=(
                        f"BYOC agent telemetry must keep {field_name} pinned "
                        "to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-agent-contract-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        f"BYOC agent telemetry must keep {field_name} pinned "
                        "to Literal[False] = False"
                    ),
                )
            )

    for class_name in BYOC_AGENT_NO_RAW_TOKEN_MODELS:
        fields = _class_field_assignments(classes.get(class_name))
        assignment = fields.get("install_token")
        if assignment is not None:
            violations.append(
                Violation(
                    check="byoc-agent-contract-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        f"{class_name} must not serialize raw install_token; "
                        "only install_token_secret_ref may leave the data plane"
                    ),
                )
            )

    return violations


def find_byoc_agent_token_rotation_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_AGENT_TOKEN_ROTATION_PATH,
) -> list[Violation]:
    """Return BYOC token-rotation report drift that could leak secrets."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-agent-token-rotation-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC agent token rotation module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    report_class = classes.get("ByocAgentTokenRotationPlanReport")
    report_fields = _class_field_assignments(report_class)
    for field_name, assignment in sorted(report_fields.items()):
        lowered = field_name.lower()
        if "secret_ref" in lowered and not lowered.endswith("secret_ref_digest"):
            violations.append(
                Violation(
                    check="byoc-agent-token-rotation-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC token rotation reports must not serialize raw "
                        f"secret-ref field {field_name!r}; use salted digests"
                    ),
                )
            )
            continue
        if any(
            fragment in lowered
            for fragment in (
                BYOC_AGENT_TOKEN_ROTATION_FORBIDDEN_REPORT_FIELD_FRAGMENTS
            )
        ):
            violations.append(
                Violation(
                    check="byoc-agent-token-rotation-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC token rotation reports must not serialize "
                        f"secret-sensitive field {field_name!r}"
                    ),
                )
            )

    privacy_class = classes.get("ByocAgentTokenRotationPrivacyContract")
    privacy_fields = _class_field_assignments(privacy_class)
    for field_name in BYOC_AGENT_TOKEN_ROTATION_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-agent-token-rotation-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        f"BYOC token rotation privacy must keep {field_name} "
                        "pinned to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-agent-token-rotation-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        f"BYOC token rotation privacy must keep {field_name} "
                        "pinned to Literal[False] = False"
                    ),
                )
            )

    for field_name in BYOC_AGENT_TOKEN_ROTATION_TRUE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-agent-token-rotation-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        f"BYOC token rotation privacy must keep {field_name} "
                        "pinned to Literal[True] = True"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[True]"
            or not isinstance(value, ast.Constant)
            or value.value is not True
        ):
            violations.append(
                Violation(
                    check="byoc-agent-token-rotation-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        f"BYOC token rotation privacy must keep {field_name} "
                        "pinned to Literal[True] = True"
                    ),
                )
            )

    return violations


def find_byoc_live_credential_rehearsal_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_LIVE_CREDENTIAL_REHEARSAL_PATH,
) -> list[Violation]:
    """Return live-credential rehearsal drift that could leak cloud metadata."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-live-credential-rehearsal-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC live credential rehearsal module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    report_class = classes.get("ByocLiveCredentialRehearsalReport")
    report_fields = _class_field_assignments(report_class)
    for field_name, assignment in sorted(report_fields.items()):
        lowered = field_name.lower()
        if any(
            fragment in lowered
            for fragment in (
                BYOC_LIVE_CREDENTIAL_REHEARSAL_FORBIDDEN_REPORT_FIELD_FRAGMENTS
            )
        ):
            violations.append(
                Violation(
                    check="byoc-live-credential-rehearsal-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC live credential rehearsal reports must not "
                        f"serialize sensitive field {field_name!r}"
                    ),
                )
            )

    privacy_class = classes.get("ByocLiveCredentialRehearsalPrivacyContract")
    privacy_fields = _class_field_assignments(privacy_class)
    for field_name in BYOC_LIVE_CREDENTIAL_REHEARSAL_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-live-credential-rehearsal-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        f"BYOC live credential rehearsal privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-live-credential-rehearsal-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        f"BYOC live credential rehearsal privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )

    return violations


def find_byoc_control_plane_read_smoke_summary_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_CONTROL_PLANE_READ_SMOKE_SUMMARY_PATH,
) -> list[Violation]:
    """Return control-plane read smoke summary drift that could leak headers."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-control-plane-read-smoke-summary-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC control-plane read smoke summary module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    report_class = classes.get("ByocControlPlaneReadSmokeSummary")
    report_fields = _class_field_assignments(report_class)
    for field_name, assignment in sorted(report_fields.items()):
        lowered = field_name.lower()
        if any(
            fragment in lowered
            for fragment in (
                BYOC_CONTROL_PLANE_READ_SMOKE_SUMMARY_FORBIDDEN_REPORT_FIELD_FRAGMENTS
            )
        ):
            violations.append(
                Violation(
                    check="byoc-control-plane-read-smoke-summary-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC control-plane read smoke summaries must not "
                        f"serialize sensitive field {field_name!r}"
                    ),
                )
            )

    stored_scope = report_fields.get("stored_scope")
    if stored_scope is None:
        violations.append(
            Violation(
                check="byoc-control-plane-read-smoke-summary-privacy",
                path=contract_path,
                line_number=report_class.lineno if report_class else 1,
                message=(
                    "BYOC control-plane read smoke summary must pin stored_scope "
                    "to sanitized_control_plane_read_smoke_metadata_only"
                ),
            )
        )
    elif (
        not isinstance(stored_scope.value, ast.Constant)
        or stored_scope.value.value
        != "sanitized_control_plane_read_smoke_metadata_only"
    ):
        violations.append(
            Violation(
                check="byoc-control-plane-read-smoke-summary-privacy",
                path=contract_path,
                line_number=stored_scope.lineno,
                message=(
                    "BYOC control-plane read smoke summary must pin stored_scope "
                    "to sanitized_control_plane_read_smoke_metadata_only"
                ),
            )
        )

    privacy_class = classes.get("ByocControlPlaneReadSmokePrivacyContract")
    privacy_fields = _class_field_assignments(privacy_class)
    for field_name in BYOC_CONTROL_PLANE_READ_SMOKE_SUMMARY_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-control-plane-read-smoke-summary-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        "BYOC control-plane read smoke privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-control-plane-read-smoke-summary-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC control-plane read smoke privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )

    return violations


def find_byoc_control_panel_state_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_CONTROL_PANEL_STATE_PATH,
) -> list[Violation]:
    """Return control-panel state drift that could leak raw customer material."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-control-panel-state-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC control-panel state module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    for class_name in (
        "ByocControlPanelStateQuery",
        "ByocControlPanelState",
        "ByocControlPanelSection",
        "ByocControlPanelAction",
    ):
        fields = _class_field_assignments(classes.get(class_name))
        for field_name, assignment in sorted(fields.items()):
            lowered = field_name.lower()
            if any(
                fragment in lowered
                for fragment in BYOC_CONTROL_PANEL_STATE_FORBIDDEN_FIELD_FRAGMENTS
            ):
                violations.append(
                    Violation(
                        check="byoc-control-panel-state-privacy",
                        path=contract_path,
                        line_number=assignment.lineno,
                        message=(
                            "BYOC control-panel state must not serialize "
                            f"sensitive field {field_name!r}"
                        ),
                    )
                )

    state_class = classes.get("ByocControlPanelState")
    state_fields = _class_field_assignments(state_class)
    stored_scope = state_fields.get("stored_scope")
    if stored_scope is None:
        violations.append(
            Violation(
                check="byoc-control-panel-state-privacy",
                path=contract_path,
                line_number=state_class.lineno if state_class else 1,
                message=(
                    "BYOC control-panel state must pin stored_scope to "
                    "sanitized_control_panel_metadata_only"
                ),
            )
        )
    elif (
        not isinstance(stored_scope.value, ast.Constant)
        or stored_scope.value.value != "sanitized_control_panel_metadata_only"
    ):
        violations.append(
            Violation(
                check="byoc-control-panel-state-privacy",
                path=contract_path,
                line_number=stored_scope.lineno,
                message=(
                    "BYOC control-panel state must pin stored_scope to "
                    "sanitized_control_panel_metadata_only"
                ),
            )
        )

    return violations


def find_byoc_product_health_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_PRODUCT_HEALTH_PATH,
) -> list[Violation]:
    """Return product-health contract drift that could leak customer material."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-product-health-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC product-health module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    for class_name in (
        "ByocProductHealthQuery",
        "ByocProductSourceHealth",
        "ByocProductPipelineHealth",
        "ByocProductThinkHealth",
        "ByocProductModelHealth",
        "ByocProductVectorHealth",
        "ByocProductHealthIssue",
        "ByocProductHealthSnapshotPayload",
        "ByocProductHealthReceipt",
        "ByocProductHealth",
    ):
        fields = _class_field_assignments(classes.get(class_name))
        for field_name, assignment in sorted(fields.items()):
            lowered = field_name.lower()
            if any(
                fragment in lowered
                for fragment in BYOC_PRODUCT_HEALTH_FORBIDDEN_FIELD_FRAGMENTS
            ):
                violations.append(
                    Violation(
                        check="byoc-product-health-privacy",
                        path=contract_path,
                        line_number=assignment.lineno,
                        message=(
                            "BYOC product-health state must not serialize "
                            f"sensitive field {field_name!r}"
                        ),
                    )
                )

    privacy_fields = _class_field_assignments(
        classes.get("ByocProductHealthPrivacyBoundary")
    )
    for field_name in BYOC_PRODUCT_HEALTH_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-product-health-privacy",
                    path=contract_path,
                    line_number=classes["ByocProductHealthPrivacyBoundary"].lineno
                    if "ByocProductHealthPrivacyBoundary" in classes
                    else 1,
                    message=(
                        "BYOC product-health privacy boundary must include "
                        f"{field_name}"
                    ),
                )
            )
            continue
        if (
            not isinstance(assignment.value, ast.Constant)
            or assignment.value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-product-health-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC product-health privacy boundary must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )

    for class_name in (
        "ByocProductHealthSnapshotPayload",
        "ByocProductHealthReceipt",
        "ByocProductHealth",
    ):
        class_node = classes.get(class_name)
        stored_scope = _class_field_assignments(class_node).get("stored_scope")
        if stored_scope is None:
            violations.append(
                Violation(
                    check="byoc-product-health-privacy",
                    path=contract_path,
                    line_number=class_node.lineno if class_node else 1,
                    message=(
                        f"{class_name} must pin stored_scope to "
                        "sanitized_product_health_metadata_only"
                    ),
                )
            )
            continue
        if (
            not isinstance(stored_scope.value, ast.Constant)
            or stored_scope.value.value != "sanitized_product_health_metadata_only"
        ):
            violations.append(
                Violation(
                    check="byoc-product-health-privacy",
                    path=contract_path,
                    line_number=stored_scope.lineno,
                    message=(
                        f"{class_name} must pin stored_scope to "
                        "sanitized_product_health_metadata_only"
                    ),
                )
            )

    return violations


def find_byoc_product_health_storage_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migration_path: Path = BYOC_PRODUCT_HEALTH_MIGRATION_PATH,
) -> list[Violation]:
    """Return product-health storage drift that could persist raw data."""

    path = repo_root / migration_path
    if not path.exists():
        return [
            Violation(
                check="byoc-product-health-storage",
                path=migration_path,
                line_number=1,
                message="BYOC product-health migration is missing",
            )
        ]

    text = path.read_text(encoding="utf-8")
    violations: list[Violation] = []
    for table_name in (
        "byoc_product_health_snapshots",
        "byoc_product_health_sources",
        "byoc_product_health_issues",
    ):
        if f"CREATE TABLE IF NOT EXISTS {table_name}" not in text:
            violations.append(
                Violation(
                    check="byoc-product-health-storage",
                    path=migration_path,
                    line_number=1,
                    message=f"BYOC product-health table {table_name} is missing",
                )
            )
    if "stored_scope = 'sanitized_product_health_metadata_only'" not in text:
        violations.append(
            Violation(
                check="byoc-product-health-storage",
                path=migration_path,
                line_number=1,
                message=(
                    "BYOC product-health tables must pin sanitized metadata scope"
                ),
            )
        )
    for pattern in (r"\bJSONB?\b", r"\bBYTEA\b"):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            line_number = text[: match.start()].count("\n") + 1
            violations.append(
                Violation(
                    check="byoc-product-health-storage",
                    path=migration_path,
                    line_number=line_number,
                    message=(
                        "BYOC product-health storage must not persist JSON/blob "
                        "customer material"
                    ),
                )
            )

    return violations


def find_byoc_product_health_collector_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    collector_path: Path = BYOC_PRODUCT_HEALTH_COLLECTOR_PATH,
) -> list[Violation]:
    """Return product-health collector drift that could select raw data."""

    path = repo_root / collector_path
    if not path.exists():
        return [
            Violation(
                check="byoc-product-health-collector-privacy",
                path=collector_path,
                line_number=1,
                message="BYOC product-health collector module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations: list[Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        call_name = _full_name(node.func)
        if call_name is None or call_name.rsplit(".", 1)[-1] not in {
            "fetch",
            "fetchrow",
        }:
            continue
        if not node.args:
            continue
        sql_text = _static_string_text(node.args[0])
        if sql_text is None:
            violations.append(
                Violation(
                    check="byoc-product-health-collector-privacy",
                    path=collector_path,
                    line_number=node.lineno,
                    message=(
                        "BYOC product-health collector DB calls must use "
                        "literal SQL so privacy ratchets can inspect them"
                    ),
                )
            )
            continue
        lowered = sql_text.lower()
        for pattern, message in BYOC_PRODUCT_HEALTH_COLLECTOR_FORBIDDEN_SQL_PATTERNS:
            if re.search(pattern, lowered, re.IGNORECASE):
                violations.append(
                    Violation(
                        check="byoc-product-health-collector-privacy",
                        path=collector_path,
                        line_number=node.lineno,
                        message=message,
                    )
                )
                break

    return violations


def find_byoc_control_panel_access_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_CONTROL_PANEL_ACCESS_PATH,
) -> list[Violation]:
    """Return control-panel access drift that could leak raw control material."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-control-panel-access-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC control-panel access module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    for class_name in (
        "ByocControlPanelAccessGrant",
        "ByocControlPanelAccessGrantList",
        "ByocControlPanelAccessQuery",
        "ByocControlPanelAccessDecision",
    ):
        fields = _class_field_assignments(classes.get(class_name))
        for field_name, assignment in sorted(fields.items()):
            lowered = field_name.lower()
            if any(
                fragment in lowered
                for fragment in BYOC_CONTROL_PANEL_ACCESS_FORBIDDEN_FIELD_FRAGMENTS
            ):
                violations.append(
                    Violation(
                        check="byoc-control-panel-access-privacy",
                        path=contract_path,
                        line_number=assignment.lineno,
                        message=(
                            "BYOC control-panel access must not serialize "
                            f"sensitive field {field_name!r}"
                        ),
                    )
                )

    for class_name in (
        "ByocControlPanelAccessGrant",
        "ByocControlPanelAccessGrantList",
        "ByocControlPanelAccessDecision",
    ):
        model_class = classes.get(class_name)
        fields = _class_field_assignments(model_class)
        stored_scope = fields.get("stored_scope")
        if stored_scope is None:
            violations.append(
                Violation(
                    check="byoc-control-panel-access-privacy",
                    path=contract_path,
                    line_number=model_class.lineno if model_class else 1,
                    message=(
                        "BYOC control-panel access must pin stored_scope to "
                        "sanitized_control_panel_access_metadata_only"
                    ),
                )
            )
        elif (
            not isinstance(stored_scope.value, ast.Constant)
            or stored_scope.value.value
            != "sanitized_control_panel_access_metadata_only"
        ):
            violations.append(
                Violation(
                    check="byoc-control-panel-access-privacy",
                    path=contract_path,
                    line_number=stored_scope.lineno,
                    message=(
                        "BYOC control-panel access must pin stored_scope to "
                        "sanitized_control_panel_access_metadata_only"
                    ),
                )
            )

    return violations


def find_byoc_control_panel_access_storage_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migration_path: Path = BYOC_CONTROL_PANEL_ACCESS_GRANT_MIGRATION_PATH,
) -> list[Violation]:
    """Return control-panel access storage drift that could persist raw data."""

    path = repo_root / migration_path
    if not path.exists():
        return [
            Violation(
                check="byoc-control-panel-access-storage",
                path=migration_path,
                line_number=1,
                message="BYOC control-panel access grant migration is missing",
            )
        ]

    text = _strip_sql_comments_preserving_lines(
        path.read_text(encoding="utf-8", errors="ignore")
    )
    violations: list[Violation] = []
    if "CREATE TABLE IF NOT EXISTS byoc_control_panel_access_grants" not in text:
        violations.append(
            Violation(
                check="byoc-control-panel-access-storage",
                path=migration_path,
                line_number=1,
                message="BYOC control-panel access grant table must be created explicitly",
            )
        )
    if "stored_scope = 'sanitized_control_panel_access_metadata_only'" not in text:
        violations.append(
            Violation(
                check="byoc-control-panel-access-storage",
                path=migration_path,
                line_number=1,
                message=(
                    "BYOC control-panel access grants must pin sanitized metadata scope"
                ),
            )
        )

    migration_paths: set[Path] = {migration_path}
    migrations_dir = repo_root / "db" / "migrations"
    if migrations_dir.exists():
        for candidate in sorted(migrations_dir.glob("*.sql")):
            candidate_text = _strip_sql_comments_preserving_lines(
                candidate.read_text(encoding="utf-8", errors="ignore")
            )
            if "byoc_control_panel_access_grants" in candidate_text:
                migration_paths.add(candidate.relative_to(repo_root))

    for scanned_path in sorted(migration_paths):
        scanned_text = _strip_sql_comments_preserving_lines(
            (repo_root / scanned_path).read_text(
                encoding="utf-8",
                errors="ignore",
            )
        )
        for line_number, line in enumerate(scanned_text.splitlines(), start=1):
            for pattern, message in (
                BYOC_CONTROL_PANEL_ACCESS_GRANT_FORBIDDEN_STORAGE_PATTERNS
            ):
                if pattern.search(line):
                    violations.append(
                        Violation(
                            check="byoc-control-panel-access-storage",
                            path=scanned_path,
                            line_number=line_number,
                            message=message,
                        )
                    )
                    break
    return violations


def find_byoc_launch_readiness_summary_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_LAUNCH_READINESS_SUMMARY_PATH,
) -> list[Violation]:
    """Return launch-readiness summary drift that could leak child reports."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-launch-readiness-summary-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC launch readiness summary module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    report_class = classes.get("ByocLaunchReadinessSummary")
    report_fields = _class_field_assignments(report_class)
    for field_name, assignment in sorted(report_fields.items()):
        lowered = field_name.lower()
        if any(
            fragment in lowered
            for fragment in (
                BYOC_LAUNCH_READINESS_SUMMARY_FORBIDDEN_REPORT_FIELD_FRAGMENTS
            )
        ):
            violations.append(
                Violation(
                    check="byoc-launch-readiness-summary-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC launch readiness summaries must not serialize "
                        f"sensitive field {field_name!r}"
                    ),
                )
            )

    stored_scope = report_fields.get("stored_scope")
    if stored_scope is None:
        violations.append(
            Violation(
                check="byoc-launch-readiness-summary-privacy",
                path=contract_path,
                line_number=report_class.lineno if report_class else 1,
                message=(
                    "BYOC launch readiness summary must pin stored_scope to "
                    "sanitized_launch_readiness_metadata_only"
                ),
            )
        )
    elif (
        not isinstance(stored_scope.value, ast.Constant)
        or stored_scope.value.value != "sanitized_launch_readiness_metadata_only"
    ):
        violations.append(
            Violation(
                check="byoc-launch-readiness-summary-privacy",
                path=contract_path,
                line_number=stored_scope.lineno,
                message=(
                    "BYOC launch readiness summary must pin stored_scope to "
                    "sanitized_launch_readiness_metadata_only"
                ),
            )
        )

    privacy_class = classes.get("ByocLaunchReadinessPrivacyContract")
    privacy_fields = _class_field_assignments(privacy_class)
    for field_name in BYOC_LAUNCH_READINESS_SUMMARY_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-launch-readiness-summary-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        f"BYOC launch readiness privacy must keep {field_name} "
                        "pinned to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-launch-readiness-summary-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        f"BYOC launch readiness privacy must keep {field_name} "
                        "pinned to Literal[False] = False"
                    ),
                )
            )

    return violations


def find_byoc_customer_pilot_package_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_CUSTOMER_PILOT_PACKAGE_PATH,
) -> list[Violation]:
    """Return customer-pilot package manifest drift that could leak artifacts."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-customer-pilot-package-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC customer pilot package module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    report_scopes = {
        "ByocCustomerPilotPackageManifest": (
            "sanitized_customer_pilot_package_manifest_only"
        ),
        "ByocCustomerPilotPackageValidationResult": (
            "sanitized_customer_pilot_package_validation_metadata_only"
        ),
    }
    for class_name, expected_scope in report_scopes.items():
        report_class = classes.get(class_name)
        report_fields = _class_field_assignments(report_class)
        for field_name, assignment in sorted(report_fields.items()):
            lowered = field_name.lower()
            if any(
                fragment in lowered
                for fragment in (
                    BYOC_CUSTOMER_PILOT_PACKAGE_FORBIDDEN_REPORT_FIELD_FRAGMENTS
                )
            ):
                violations.append(
                    Violation(
                        check="byoc-customer-pilot-package-privacy",
                        path=contract_path,
                        line_number=assignment.lineno,
                        message=(
                            "BYOC customer-pilot package models must not "
                            f"serialize sensitive field {field_name!r}"
                        ),
                    )
                )

        stored_scope = report_fields.get("stored_scope")
        if stored_scope is None:
            violations.append(
                Violation(
                    check="byoc-customer-pilot-package-privacy",
                    path=contract_path,
                    line_number=report_class.lineno if report_class else 1,
                    message=(
                        f"{class_name} must pin stored_scope to {expected_scope}"
                    ),
                )
            )
        elif (
            not isinstance(stored_scope.value, ast.Constant)
            or stored_scope.value.value != expected_scope
        ):
            violations.append(
                Violation(
                    check="byoc-customer-pilot-package-privacy",
                    path=contract_path,
                    line_number=stored_scope.lineno,
                    message=(
                        f"{class_name} must pin stored_scope to {expected_scope}"
                    ),
                )
            )

    privacy_class = classes.get("ByocCustomerPilotPackagePrivacyContract")
    privacy_fields = _class_field_assignments(privacy_class)
    for field_name in BYOC_CUSTOMER_PILOT_PACKAGE_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-customer-pilot-package-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        "BYOC customer-pilot package privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-customer-pilot-package-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC customer-pilot package privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )

    return violations


def find_byoc_customer_pilot_rehearsal_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_CUSTOMER_PILOT_REHEARSAL_PATH,
) -> list[Violation]:
    """Return customer-pilot rehearsal summary drift that could leak evidence."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-customer-pilot-rehearsal-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC customer pilot rehearsal module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    report_class = classes.get("ByocCustomerPilotRehearsalReport")
    report_fields = _class_field_assignments(report_class)
    for field_name, assignment in sorted(report_fields.items()):
        lowered = field_name.lower()
        if any(
            fragment in lowered
            for fragment in (
                BYOC_CUSTOMER_PILOT_REHEARSAL_FORBIDDEN_REPORT_FIELD_FRAGMENTS
            )
        ):
            violations.append(
                Violation(
                    check="byoc-customer-pilot-rehearsal-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC customer-pilot rehearsal reports must not "
                        f"serialize sensitive field {field_name!r}"
                    ),
                )
            )

    stored_scope = report_fields.get("stored_scope")
    expected_scope = "sanitized_customer_pilot_rehearsal_metadata_only"
    if stored_scope is None:
        violations.append(
            Violation(
                check="byoc-customer-pilot-rehearsal-privacy",
                path=contract_path,
                line_number=report_class.lineno if report_class else 1,
                message=(
                    "ByocCustomerPilotRehearsalReport must pin stored_scope "
                    f"to {expected_scope}"
                ),
            )
        )
    elif (
        not isinstance(stored_scope.value, ast.Constant)
        or stored_scope.value.value != expected_scope
    ):
        violations.append(
            Violation(
                check="byoc-customer-pilot-rehearsal-privacy",
                path=contract_path,
                line_number=stored_scope.lineno,
                message=(
                    "ByocCustomerPilotRehearsalReport must pin stored_scope "
                    f"to {expected_scope}"
                ),
            )
        )

    privacy_class = classes.get("ByocCustomerPilotRehearsalPrivacyContract")
    privacy_fields = _class_field_assignments(privacy_class)
    for field_name in BYOC_CUSTOMER_PILOT_REHEARSAL_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-customer-pilot-rehearsal-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        "BYOC customer-pilot rehearsal privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-customer-pilot-rehearsal-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC customer-pilot rehearsal privacy must keep "
                        f"{field_name} pinned to Literal[False] = False"
                    ),
                )
            )

    return violations


def find_byoc_aws_live_preflight_privacy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    contract_path: Path = BYOC_AWS_LIVE_PREFLIGHT_PATH,
) -> list[Violation]:
    """Return AWS live preflight report drift that could leak AWS metadata."""

    path = repo_root / contract_path
    if not path.exists():
        return [
            Violation(
                check="byoc-aws-live-preflight-privacy",
                path=contract_path,
                line_number=1,
                message="BYOC AWS live preflight module is missing",
            )
        ]

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    violations: list[Violation] = []

    report_class = classes.get("ByocAwsLivePreflightReport")
    report_fields = _class_field_assignments(report_class)
    for field_name, assignment in sorted(report_fields.items()):
        lowered = field_name.lower()
        if any(
            fragment in lowered
            for fragment in BYOC_AWS_LIVE_PREFLIGHT_FORBIDDEN_REPORT_FIELD_FRAGMENTS
        ):
            violations.append(
                Violation(
                    check="byoc-aws-live-preflight-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        "BYOC AWS live preflight reports must not serialize "
                        f"AWS-sensitive field {field_name!r}"
                    ),
                )
            )

    privacy_class = classes.get("ByocAwsLivePreflightPrivacyContract")
    privacy_fields = _class_field_assignments(privacy_class)
    for field_name in BYOC_AWS_LIVE_PREFLIGHT_FALSE_PRIVACY_FLAGS:
        assignment = privacy_fields.get(field_name)
        if assignment is None:
            violations.append(
                Violation(
                    check="byoc-aws-live-preflight-privacy",
                    path=contract_path,
                    line_number=privacy_class.lineno if privacy_class else 1,
                    message=(
                        f"BYOC AWS live preflight privacy must keep {field_name} "
                        "pinned to Literal[False] = False"
                    ),
                )
            )
            continue
        annotation = ast.unparse(assignment.annotation)
        value = assignment.value
        if (
            annotation != "Literal[False]"
            or not isinstance(value, ast.Constant)
            or value.value is not False
        ):
            violations.append(
                Violation(
                    check="byoc-aws-live-preflight-privacy",
                    path=contract_path,
                    line_number=assignment.lineno,
                    message=(
                        f"BYOC AWS live preflight privacy must keep {field_name} "
                        "pinned to Literal[False] = False"
                    ),
                )
            )

    return violations


def find_byoc_evidence_receipt_storage_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migration_path: Path = BYOC_EVIDENCE_RECEIPT_MIGRATION_PATH,
) -> list[Violation]:
    """Return BYOC receipt storage drift that could persist raw evidence bodies."""

    path = repo_root / migration_path
    if not path.exists():
        return [
            Violation(
                check="byoc-evidence-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC evidence receipt migration is missing",
            )
        ]

    text = _strip_sql_comments_preserving_lines(
        path.read_text(encoding="utf-8", errors="ignore")
    )
    violations: list[Violation] = []
    if "CREATE TABLE IF NOT EXISTS byoc_evidence_package_receipts" not in text:
        violations.append(
            Violation(
                check="byoc-evidence-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC evidence receipt table must be created explicitly",
            )
        )
    if "stored_scope = 'sanitized_metadata_only'" not in text:
        violations.append(
            Violation(
                check="byoc-evidence-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC evidence receipts must pin sanitized metadata scope",
            )
        )

    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern, message in BYOC_EVIDENCE_RECEIPT_FORBIDDEN_STORAGE_PATTERNS:
            if pattern.search(line):
                violations.append(
                    Violation(
                        check="byoc-evidence-receipt-storage",
                        path=migration_path,
                        line_number=line_number,
                        message=message,
                    )
                )
                break
    return violations


def find_byoc_agent_registration_storage_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migration_path: Path = BYOC_AGENT_REGISTRATION_MIGRATION_PATH,
) -> list[Violation]:
    """Return BYOC agent registry drift that could persist raw agent data."""

    path = repo_root / migration_path
    if not path.exists():
        return [
            Violation(
                check="byoc-agent-registration-storage",
                path=migration_path,
                line_number=1,
                message="BYOC agent registration migration is missing",
            )
        ]

    text = _strip_sql_comments_preserving_lines(
        path.read_text(encoding="utf-8", errors="ignore")
    )
    violations: list[Violation] = []
    if "CREATE TABLE IF NOT EXISTS byoc_agent_registrations" not in text:
        violations.append(
            Violation(
                check="byoc-agent-registration-storage",
                path=migration_path,
                line_number=1,
                message="BYOC agent registration table must be created explicitly",
            )
        )
    if "stored_scope = 'sanitized_agent_metadata_only'" not in text:
        violations.append(
            Violation(
                check="byoc-agent-registration-storage",
                path=migration_path,
                line_number=1,
                message="BYOC agent registrations must pin sanitized metadata scope",
            )
        )

    migration_paths: set[Path] = {migration_path}
    migrations_dir = repo_root / "db" / "migrations"
    if migrations_dir.exists():
        for candidate in sorted(migrations_dir.glob("*.sql")):
            candidate_text = _strip_sql_comments_preserving_lines(
                candidate.read_text(encoding="utf-8", errors="ignore")
            )
            if "byoc_agent_registrations" in candidate_text:
                migration_paths.add(candidate.relative_to(repo_root))

    for scanned_path in sorted(migration_paths):
        scanned_text = _strip_sql_comments_preserving_lines(
            (repo_root / scanned_path).read_text(encoding="utf-8", errors="ignore")
        )
        for line_number, line in enumerate(scanned_text.splitlines(), start=1):
            for pattern, message in BYOC_AGENT_REGISTRATION_FORBIDDEN_STORAGE_PATTERNS:
                if pattern.search(line):
                    violations.append(
                        Violation(
                            check="byoc-agent-registration-storage",
                            path=scanned_path,
                            line_number=line_number,
                            message=message,
                        )
                    )
                    break
    return violations


def find_byoc_runner_evidence_receipt_storage_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migration_path: Path = BYOC_RUNNER_EVIDENCE_RECEIPT_MIGRATION_PATH,
) -> list[Violation]:
    """Return BYOC runner receipt drift that could persist raw runner reports."""

    path = repo_root / migration_path
    if not path.exists():
        return [
            Violation(
                check="byoc-runner-evidence-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC runner evidence receipt migration is missing",
            )
        ]

    text = _strip_sql_comments_preserving_lines(
        path.read_text(encoding="utf-8", errors="ignore")
    )
    violations: list[Violation] = []
    if "CREATE TABLE IF NOT EXISTS byoc_runner_evidence_receipts" not in text:
        violations.append(
            Violation(
                check="byoc-runner-evidence-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC runner evidence receipt table must be created explicitly",
            )
        )
    if "stored_scope = 'sanitized_metadata_only'" not in text:
        violations.append(
            Violation(
                check="byoc-runner-evidence-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC runner evidence receipts must pin sanitized metadata scope",
            )
        )

    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern, message in BYOC_RUNNER_EVIDENCE_RECEIPT_FORBIDDEN_STORAGE_PATTERNS:
            if pattern.search(line):
                violations.append(
                    Violation(
                        check="byoc-runner-evidence-receipt-storage",
                        path=migration_path,
                        line_number=line_number,
                        message=message,
                    )
                )
                break
    return violations


def find_byoc_preflight_report_receipt_storage_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migration_path: Path = BYOC_PREFLIGHT_REPORT_RECEIPT_MIGRATION_PATH,
) -> list[Violation]:
    """Return BYOC preflight receipt drift that could persist raw reports."""

    path = repo_root / migration_path
    if not path.exists():
        return [
            Violation(
                check="byoc-preflight-report-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC preflight report receipt migration is missing",
            )
        ]

    text = _strip_sql_comments_preserving_lines(
        path.read_text(encoding="utf-8", errors="ignore")
    )
    violations: list[Violation] = []
    if "CREATE TABLE IF NOT EXISTS byoc_preflight_report_receipts" not in text:
        violations.append(
            Violation(
                check="byoc-preflight-report-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC preflight report receipt table must be created explicitly",
            )
        )
    if "stored_scope = 'sanitized_metadata_only'" not in text:
        violations.append(
            Violation(
                check="byoc-preflight-report-receipt-storage",
                path=migration_path,
                line_number=1,
                message="BYOC preflight report receipts must pin sanitized metadata scope",
            )
        )

    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern, message in BYOC_PREFLIGHT_REPORT_RECEIPT_FORBIDDEN_STORAGE_PATTERNS:
            if pattern.search(line):
                violations.append(
                    Violation(
                        check="byoc-preflight-report-receipt-storage",
                        path=migration_path,
                        line_number=line_number,
                        message=message,
                    )
                )
                break
    return violations


def _class_field_assignments(
    node: ast.ClassDef | None,
) -> dict[str, ast.AnnAssign]:
    if node is None:
        return {}
    fields: dict[str, ast.AnnAssign] = {}
    for child in node.body:
        if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            fields[child.target.id] = child
    return fields


def _static_string_text(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        fragments: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                fragments.append(value.value)
        return "".join(fragments)
    return None


def find_migration_filename_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migrations_dir: Path = Path("db/migrations"),
) -> list[Violation]:
    """Return migration files without a unique numeric prefix."""

    root = repo_root / migrations_dir
    if not root.exists():
        return [
            Violation(
                check="migration-filename-ratchet",
                path=migrations_dir,
                line_number=1,
                message="migration directory is missing",
            )
        ]

    violations: list[Violation] = []
    by_prefix: dict[str, list[Path]] = {}
    for path in sorted(root.glob("*.sql")):
        rel = path.relative_to(repo_root)
        match = MIGRATION_FILENAME_RE.match(path.name)
        if match is None:
            violations.append(
                Violation(
                    check="migration-filename-ratchet",
                    path=rel,
                    line_number=1,
                    message=(
                        "migration filenames must start with a unique "
                        "four-digit prefix, e.g. 0156_example.sql"
                    ),
                )
            )
            continue
        by_prefix.setdefault(match.group("prefix"), []).append(rel)

    for prefix, paths in sorted(by_prefix.items()):
        if len(paths) <= 1:
            continue
        joined = ", ".join(str(path) for path in paths)
        for path in paths:
            violations.append(
                Violation(
                    check="migration-filename-ratchet",
                    path=path,
                    line_number=1,
                    message=f"duplicate migration prefix {prefix}: {joined}",
                )
            )
    return violations


def _strip_sql_comments_preserving_lines(text: str) -> str:
    """Remove SQL comments while keeping line numbers stable enough for CI."""

    stripped_lines: list[str] = []
    in_block_comment = False
    for line in text.splitlines():
        index = 0
        output: list[str] = []
        while index < len(line):
            if in_block_comment:
                end = line.find("*/", index)
                if end == -1:
                    index = len(line)
                    continue
                in_block_comment = False
                index = end + 2
                continue
            if line.startswith("/*", index):
                in_block_comment = True
                index += 2
                continue
            if line.startswith("--", index):
                break
            output.append(line[index])
            index += 1
        stripped_lines.append("".join(output))
    return "\n".join(stripped_lines)


def _destructive_migration_marker_missing_fields(
    text: str,
) -> tuple[int, list[str]] | None:
    for line_number, line in enumerate(text.splitlines(), start=1):
        marker_offset = line.find(DESTRUCTIVE_MIGRATION_MARKER)
        if marker_offset == -1:
            continue
        marker = line[marker_offset:]
        missing = [
            field
            for field in DESTRUCTIVE_MIGRATION_REQUIRED_MARKER_FIELDS
            if field not in marker
        ]
        return line_number, missing
    return None


def find_destructive_migration_without_approval_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migrations_dir: Path = Path("db/migrations"),
) -> list[Violation]:
    """Return new destructive migrations without backup/rollback evidence.

    Historical migrations are baselined explicitly. New destructive schema/data
    changes must carry a marker with operator evidence so release reviewers can
    find the backup verification, rollback or forward-fix plan, and owner.
    """

    root = repo_root / migrations_dir
    if not root.exists():
        return []

    violations: list[Violation] = []
    marker_format = (
        "-- destructive-migration-approved: "
        "backup=<snapshot-or-ticket> rollback=<runbook-or-ticket> owner=<name>"
    )
    for path in sorted(root.glob("*.sql")):
        rel = path.relative_to(repo_root)
        if rel in DESTRUCTIVE_MIGRATION_ALLOWED_FILES:
            continue

        raw_text = path.read_text(encoding="utf-8", errors="ignore")
        text = _strip_sql_comments_preserving_lines(raw_text)
        destructive_hits: list[tuple[int, str]] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            for pattern, operation in DESTRUCTIVE_MIGRATION_PATTERNS:
                if pattern.search(line):
                    destructive_hits.append((line_number, operation))
                    break

        if not destructive_hits:
            continue

        marker_state = _destructive_migration_marker_missing_fields(raw_text)
        if marker_state is not None:
            marker_line, missing_fields = marker_state
            if not missing_fields:
                continue
            violations.append(
                Violation(
                    check="destructive-migration-approval",
                    path=rel,
                    line_number=marker_line,
                    message=(
                        "destructive migration approval marker is missing "
                        f"required field(s): {', '.join(missing_fields)}; "
                        f"use `{marker_format}`"
                    ),
                )
            )
            continue

        for line_number, operation in destructive_hits:
            violations.append(
                Violation(
                    check="destructive-migration-approval",
                    path=rel,
                    line_number=line_number,
                    message=(
                        f"{operation} requires backup verification, a rollback "
                        f"or forward-fix plan, and an owner; use `{marker_format}`"
                    ),
                )
            )
    return violations


def find_new_permissive_rls_policy_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migrations_dir: Path = Path("db/migrations"),
    baseline_prefix: int = STRICT_RLS_BASELINE_MIGRATION_PREFIX,
) -> list[Violation]:
    """Return post-baseline migrations that add no-tenant RLS bypasses."""

    root = repo_root / migrations_dir
    if not root.exists():
        return []

    violations: list[Violation] = []
    for path in sorted(root.glob("*.sql")):
        match = MIGRATION_FILENAME_RE.match(path.name)
        if match is None or int(match.group("prefix")) <= baseline_prefix:
            continue
        rel = path.relative_to(repo_root)
        raw_text = path.read_text(encoding="utf-8", errors="ignore")
        text = _strip_sql_comments_preserving_lines(raw_text)
        for line_number, line in enumerate(text.splitlines(), start=1):
            if PERMISSIVE_UNBOUND_TENANT_POLICY_RE.search(line):
                violations.append(
                    Violation(
                        check="new-permissive-rls-policy",
                        path=rel,
                        line_number=line_number,
                        message=(
                            "new migrations must not permit access when "
                            "app.current_tenant is unset; bind tenant context "
                            "or use an audited service role/table"
                        ),
                    )
                )
    return violations


def _plaintext_secret_column_reason(column_name: str) -> str | None:
    normalized = column_name.strip('"').lower()
    if not SECRET_LIKE_COLUMN_NAME_RE.search(normalized):
        return None
    if normalized.endswith(SECRET_COLUMN_ALLOWED_SUFFIXES):
        return None
    return (
        "new credential-like columns must store opaque secret refs, hashes, "
        "ciphertext, fingerprints, or metadata; not plaintext secret/token "
        "values"
    )


def find_plaintext_secret_column_migration_violations(
    *,
    repo_root: Path = REPO_ROOT,
    migrations_dir: Path = Path("db/migrations"),
    baseline_prefix: int = PLAINTEXT_SECRET_COLUMN_BASELINE_MIGRATION_PREFIX,
) -> list[Violation]:
    """Return post-baseline migrations that add plaintext credential columns."""

    root = repo_root / migrations_dir
    if not root.exists():
        return []

    violations: list[Violation] = []
    for path in sorted(root.glob("*.sql")):
        match = MIGRATION_FILENAME_RE.match(path.name)
        if match is None or int(match.group("prefix")) <= baseline_prefix:
            continue
        rel = path.relative_to(repo_root)
        raw_text = path.read_text(encoding="utf-8", errors="ignore")
        text = _strip_sql_comments_preserving_lines(raw_text)
        for line_number, line in enumerate(text.splitlines(), start=1):
            for column_match in PLAINTEXT_SECRET_COLUMN_RE.finditer(line):
                column_name = column_match.group("name")
                reason = _plaintext_secret_column_reason(column_name)
                if reason is None:
                    continue
                violations.append(
                    Violation(
                        check="plaintext-secret-column-migration",
                        path=rel,
                        line_number=line_number,
                        message=f"{reason}: {column_name.strip('\"')!r}",
                    )
                )
    return violations


def _is_secret_ref_keyword(name: str | None) -> bool:
    return bool(name and SECRET_REF_KEYWORD_NAME_RE.search(name))


def _unsafe_secret_ref_value_name(node: ast.AST) -> str | None:
    name = _full_name(node)
    if name is None:
        return None
    leaf = name.rsplit(".", 1)[-1]
    normalized = leaf.lower()
    if normalized.endswith(SECRET_REF_SAFE_VALUE_SUFFIXES):
        return None
    if SECRET_REF_UNSAFE_VALUE_NAME_RE.search(normalized):
        return leaf
    return None


def find_raw_secret_ref_argument_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = ("services/ingest/integrations",),
) -> list[Violation]:
    """Return install code that passes raw credentials into ``*_ref`` kwargs."""

    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if _is_test_path(rel):
            continue
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(text, filename=str(rel))
        except SyntaxError as exc:
            violations.append(
                Violation(
                    check="raw-secret-ref-argument",
                    path=rel,
                    line_number=exc.lineno or 1,
                    message=f"could not parse Python file: {exc.msg}",
                )
            )
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if not _is_secret_ref_keyword(keyword.arg):
                    continue
                unsafe_name = _unsafe_secret_ref_value_name(keyword.value)
                if unsafe_name is None:
                    continue
                violations.append(
                    Violation(
                        check="raw-secret-ref-argument",
                        path=rel,
                        line_number=getattr(keyword.value, "lineno", node.lineno),
                        message=(
                            f"{keyword.arg} must receive an opaque secret ref, "
                            f"not raw credential variable {unsafe_name!r}; store "
                            "the credential first and pass the returned *_ref"
                        ),
                    )
                )
    return violations


def find_raw_think_trigger_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into think_trigger_queue.

    Tests may still seed queue rows directly. Production code should use
    services.domain.triggers.enqueue_trigger so the queue contract has one
    owning module.
    """

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_THINK_TRIGGER_INSERT_RE,
        allowed_files=RAW_THINK_TRIGGER_INSERT_ALLOWED_FILES,
        check="raw-think-trigger-insert",
        message=("use services.domain.triggers.enqueue_trigger instead of raw SQL"),
    )


def find_raw_model_reeval_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into model_reeval_queue.

    Most producers should use services.domain.triggers.enqueue_model_reeval.
    The edge registry is explicitly allowlisted because it is shared library
    code and cannot import the services layer without breaking architecture
    contracts.
    """

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_MODEL_REEVAL_INSERT_RE,
        allowed_files=RAW_MODEL_REEVAL_INSERT_ALLOWED_FILES,
        check="raw-model-reeval-insert",
        message=(
            "use services.domain.triggers.enqueue_model_reeval instead of raw SQL"
        ),
    )


def find_raw_pending_post_commit_action_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into pending_post_commit_actions."""

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_PENDING_POST_COMMIT_ACTION_INSERT_RE,
        allowed_files=RAW_PENDING_POST_COMMIT_ACTION_INSERT_ALLOWED_FILES,
        check="raw-pending-post-commit-action-insert",
        message=(
            "use services.reasoning.think.post_commit.enqueue_post_commit_actions "
            "instead of raw SQL"
        ),
    )


def find_raw_think_obligation_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into think_obligations."""

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_THINK_OBLIGATION_INSERT_RE,
        allowed_files=RAW_THINK_OBLIGATION_INSERT_ALLOWED_FILES,
        check="raw-think-obligation-insert",
        message="use services.domain.obligations.open_obligation instead of raw SQL",
    )


def _import_linter_ignore_counts(repo_root: Path) -> dict[str, int]:
    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return {}
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    contracts = data.get("tool", {}).get("importlinter", {}).get("contracts", [])
    counts: dict[str, int] = {}
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        name = contract.get("name")
        if not isinstance(name, str):
            continue
        ignored = contract.get("ignore_imports", [])
        counts[name] = len(ignored) if isinstance(ignored, list) else 0
    return counts


def find_import_linter_allowlist_violations(
    *,
    repo_root: Path = REPO_ROOT,
    limits: Mapping[str, int] = IMPORT_LINTER_IGNORE_IMPORT_LIMITS,
) -> list[Violation]:
    """Return import-linter allowlist counts that grew beyond the baseline."""

    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return []

    counts = _import_linter_ignore_counts(repo_root)
    violations: list[Violation] = []
    for contract_name, limit in sorted(limits.items()):
        if contract_name not in counts:
            violations.append(
                Violation(
                    check="import-linter-allowlist-ratchet",
                    path=Path("pyproject.toml"),
                    line_number=1,
                    message=f"missing tracked contract {contract_name!r}",
                )
            )
            continue
        count = counts[contract_name]
        if count > limit:
            violations.append(
                Violation(
                    check="import-linter-allowlist-ratchet",
                    path=Path("pyproject.toml"),
                    line_number=1,
                    message=(
                        f"{contract_name!r} has {count} ignored imports; "
                        f"limit is {limit}"
                    ),
                )
            )
    return violations


def find_rollback_data_deletion_violations(
    *,
    repo_root: Path = REPO_ROOT,
    files: Sequence[Path] = PRODUCTION_ROLLBACK_AUTOMATION_FILES,
) -> list[Violation]:
    """Return production deploy/rollback automation that can delete data."""

    violations: list[Violation] = []
    for rel in files:
        path = repo_root / rel
        if not path.exists():
            continue
        for line_number, raw_line in enumerate(
            path.read_text(encoding="utf-8", errors="ignore").splitlines(),
            start=1,
        ):
            line = raw_line.split("#", 1)[0]
            for pattern, operation in ROLLBACK_DATA_DELETION_PATTERNS:
                if pattern.search(line):
                    violations.append(
                        Violation(
                            check="rollback-data-deletion",
                            path=rel,
                            line_number=line_number,
                            message=(
                                f"{operation} is forbidden in production "
                                "deploy/rollback automation; roll back code, "
                                "pause/quarantine work, or restore from an "
                                "explicit backup runbook instead"
                            ),
                        )
                    )
                    break
    return violations


def run_checks(repo_root: Path = REPO_ROOT) -> list[Violation]:
    violations: list[Violation] = []
    violations.extend(find_migration_filename_violations(repo_root=repo_root))
    violations.extend(
        find_destructive_migration_without_approval_violations(repo_root=repo_root)
    )
    violations.extend(find_new_permissive_rls_policy_violations(repo_root=repo_root))
    violations.extend(
        find_plaintext_secret_column_migration_violations(repo_root=repo_root)
    )
    violations.extend(find_raw_secret_ref_argument_violations(repo_root=repo_root))
    violations.extend(find_raw_think_trigger_insert_violations(repo_root=repo_root))
    violations.extend(find_raw_model_reeval_insert_violations(repo_root=repo_root))
    violations.extend(
        find_raw_pending_post_commit_action_insert_violations(repo_root=repo_root)
    )
    violations.extend(find_raw_think_obligation_insert_violations(repo_root=repo_root))
    violations.extend(
        find_network_call_in_transaction_violations(repo_root=repo_root)
    )
    violations.extend(
        find_access_read_without_override_audit_violations(repo_root=repo_root)
    )
    violations.extend(find_forbidden_metric_label_violations(repo_root=repo_root))
    violations.extend(find_browser_token_storage_violations(repo_root=repo_root))
    violations.extend(
        find_product_default_tenant_without_production_guard_violations(
            repo_root=repo_root
        )
    )
    violations.extend(find_byoc_manifest_privacy_violations(repo_root=repo_root))
    violations.extend(
        find_byoc_agent_contract_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_agent_token_rotation_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_live_credential_rehearsal_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_control_plane_read_smoke_summary_privacy_violations(
            repo_root=repo_root
        )
    )
    violations.extend(
        find_byoc_control_panel_state_privacy_violations(repo_root=repo_root)
    )
    violations.extend(find_byoc_product_health_privacy_violations(repo_root=repo_root))
    violations.extend(find_byoc_product_health_storage_violations(repo_root=repo_root))
    violations.extend(
        find_byoc_product_health_collector_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_control_panel_access_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_control_panel_access_storage_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_launch_readiness_summary_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_customer_pilot_package_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_customer_pilot_rehearsal_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_aws_live_preflight_privacy_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_evidence_receipt_storage_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_agent_registration_storage_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_runner_evidence_receipt_storage_violations(repo_root=repo_root)
    )
    violations.extend(
        find_byoc_preflight_report_receipt_storage_violations(repo_root=repo_root)
    )
    violations.extend(find_import_linter_allowlist_violations(repo_root=repo_root))
    violations.extend(find_rollback_data_deletion_violations(repo_root=repo_root))
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="Repository root to scan.",
    )
    args = parser.parse_args(argv)

    violations = run_checks(args.repo_root.resolve())
    if violations:
        print("Architecture ratchet violations:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation.render()}", file=sys.stderr)
        return 1
    print("Architecture ratchets passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
