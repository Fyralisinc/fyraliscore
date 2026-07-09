"""Customer-cloud source browser-agent runner.

The runner consumes the sanitized ``browser_agent_run`` contract emitted by the
source setup flow. It executes bounded agent-owned actions, can call native
customer-cloud preflight/finalize endpoints when explicitly provided a local
payload, and pauses at provider-admin gates.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import secrets
import textwrap
import webbrowser
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urljoin, urlparse

import httpx


_SLACK_APP_CONFIG_TOKEN_RE = re.compile(
    r"\bxoxe[-.][A-Za-z0-9][A-Za-z0-9._-]{8,}\b"
)

RunnerStatus = Literal[
    "connected",
    "running",
    "waiting_for_admin",
    "blocked",
    "failed",
]


@dataclass(frozen=True, slots=True)
class SourceBrowserAgentActionResult:
    id: str
    owner: str
    status: str
    detail: str
    endpoint: str | None = None
    http_status: int | None = None


@dataclass(frozen=True, slots=True)
class SourceBrowserAgentRunnerReceipt:
    schema_version: Literal["fyralis.byoc.source.browser_agent_runner_receipt.v1"]
    source: str
    generated_at: str
    status: RunnerStatus
    run_state: str
    handoff_url: str | None
    handoff_opened: bool
    native_connect_kind: str | None
    automated_action_count: int
    human_action_count: int
    completed_action_count: int
    waiting_action_count: int
    generated_artifacts: dict[str, str]
    action_results: list[SourceBrowserAgentActionResult]
    raw_secret_values_included: bool = False
    raw_payloads_exported: bool = False
    stored_scope: str = "sanitized_browser_agent_runner_metadata_only"

    def as_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SourceBrowserAgentRunnerInputs:
    run_path: Path
    gateway_api_base: str | None = None
    bearer_token: str | None = None
    native_payload_path: Path | None = None
    execute_native: bool = False
    admin_approved: bool = False
    open_browser: bool = False
    timeout_s: float = 10.0
    execute_browser_dom: bool = False
    browser_headless: bool = False
    browser_timeout_s: float = 120.0
    browser_slow_mo_ms: int = 0
    browser_storage_state_path: Path | None = None
    interactive_admin: bool = False
    requested_at: datetime | None = None
    extra_headers: dict[str, str] = field(default_factory=dict)


async def run_source_browser_agent(
    inputs: SourceBrowserAgentRunnerInputs,
) -> SourceBrowserAgentRunnerReceipt:
    run = _load_run(inputs.run_path)
    generated_at = (inputs.requested_at or datetime.now(UTC)).isoformat()
    results: list[SourceBrowserAgentActionResult] = []
    generated_secret_values: dict[str, str] = {}
    generated_secret_refs: dict[str, str] = {}
    handoff_opened = False
    native_connect = run.get("native_connect")
    native_payload = _load_json(inputs.native_payload_path) if inputs.native_payload_path else None

    for action in run.get("action_queue") or []:
        action_id = str(action.get("id") or "")
        owner = str(action.get("owner") or "")
        if owner == "provider_admin":
            if inputs.admin_approved:
                results.append(
                    SourceBrowserAgentActionResult(
                        id=action_id,
                        owner=owner,
                        status="acknowledged",
                        detail="Provider-admin gate was explicitly approved locally.",
                    )
                )
            else:
                results.append(
                    SourceBrowserAgentActionResult(
                        id=action_id,
                        owner=owner,
                        status="waiting",
                        detail="Paused for provider-required admin action.",
                    )
                )
            continue

        if action_id == "open_provider_settings":
            url = str(run.get("handoff_url") or "")
            if not _is_external_url(url):
                results.append(
                    SourceBrowserAgentActionResult(
                        id=action_id,
                        owner=owner,
                        status="skipped",
                        detail="No external provider handoff URL is available.",
                    )
                )
                continue
            if inputs.open_browser:
                handoff_opened = bool(webbrowser.open(url, new=2))
                detail = "Provider handoff opened in the local browser."
                status = "completed" if handoff_opened else "failed"
            else:
                detail = "Provider handoff URL prepared; browser opening was not requested."
                status = "ready"
            results.append(
                SourceBrowserAgentActionResult(
                    id=action_id,
                    owner=owner,
                    status=status,
                    detail=detail,
                    endpoint=url,
                )
            )
            continue

        if action_id in {"run_native_preflight", "run_native_finalize"}:
            result = await _run_native_action(
                action_id=action_id,
                owner=owner,
                native_connect=native_connect,
                native_payload=native_payload,
                inputs=inputs,
                generated_secret_values=generated_secret_values,
                generated_secret_refs=generated_secret_refs,
            )
            results.append(result)
            continue

        if _is_provider_setup_materialization(action):
            artifact_paths = _materialize_provider_setup_bundle(run, inputs.run_path)
            results.append(
                SourceBrowserAgentActionResult(
                    id=action_id,
                    owner=owner,
                    status="completed",
                    detail=(
                        "Generated provider setup artifacts locally."
                        if artifact_paths
                        else "No provider setup artifacts were present to generate."
                    ),
                    endpoint=str(_provider_setup_artifact_dir(inputs.run_path)),
                )
            )
            continue

        if _is_browser_dom_plan_materialization(action):
            plan_path = _materialize_browser_dom_plan(run, inputs.run_path)
            results.append(
                SourceBrowserAgentActionResult(
                    id=action_id,
                    owner=owner,
                    status="completed" if plan_path else "skipped",
                    detail=(
                        "Prepared provider browser DOM action plan locally."
                        if plan_path
                        else "No browser DOM action plan was present to prepare."
                    ),
                    endpoint=str(plan_path) if plan_path else None,
                )
            )
            continue

        if _is_browser_dom_plan_execution(action):
            result = await _run_browser_dom_action(
                action_id=action_id,
                owner=owner,
                run=run,
                inputs=inputs,
                generated_secret_values=generated_secret_values,
                generated_secret_refs=generated_secret_refs,
            )
            results.append(result)
            continue

        results.append(
            SourceBrowserAgentActionResult(
                id=action_id,
                owner=owner,
                status="completed",
                detail="Agent-owned metadata action completed locally.",
            )
        )

    return SourceBrowserAgentRunnerReceipt(
        schema_version="fyralis.byoc.source.browser_agent_runner_receipt.v1",
        source=str(run.get("source") or ""),
        generated_at=generated_at,
        status=_receipt_status(run, results),
        run_state=str(run.get("state") or "running"),
        handoff_url=run.get("handoff_url"),
        handoff_opened=handoff_opened,
        native_connect_kind=(
            str(native_connect.get("kind")) if isinstance(native_connect, dict) else None
        ),
        automated_action_count=sum(1 for item in results if item.owner == "fyralis_agent"),
        human_action_count=sum(1 for item in results if item.owner == "provider_admin"),
        completed_action_count=sum(
            1 for item in results if item.status in {"completed", "acknowledged"}
        ),
        waiting_action_count=sum(1 for item in results if item.status == "waiting"),
        generated_artifacts=_generated_provider_setup_artifacts(inputs.run_path),
        action_results=results,
    )


async def _run_native_action(
    *,
    action_id: str,
    owner: str,
    native_connect: Any,
    native_payload: dict[str, Any] | None,
    inputs: SourceBrowserAgentRunnerInputs,
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> SourceBrowserAgentActionResult:
    if not isinstance(native_connect, dict):
        return SourceBrowserAgentActionResult(
            id=action_id,
            owner=owner,
            status="skipped",
            detail="No native connect contract is present.",
        )
    if not inputs.execute_native:
        return SourceBrowserAgentActionResult(
            id=action_id,
            owner=owner,
            status="ready",
            detail="Native endpoint call prepared; execution was not requested.",
            endpoint=_native_endpoint(native_connect, action_id, inputs.gateway_api_base),
        )
    if native_payload is None:
        template_path = _materialize_native_payload_template(
            run_path=inputs.run_path,
            native_connect=native_connect,
            action_id=action_id,
            generated_secret_values=generated_secret_values,
            generated_secret_refs=generated_secret_refs,
        )
        native_payload = _autofilled_native_payload_from_template(
            template_path,
            generated_secret_values=generated_secret_values,
        )
        if native_payload is None:
            return SourceBrowserAgentActionResult(
                id=action_id,
                owner=owner,
                status="blocked",
                detail=(
                    "Native execution requires provider-admin credential material. "
                    "A customer-local native payload template was generated."
                ),
                endpoint=str(template_path),
            )
    if action_id == "run_native_finalize" and not inputs.admin_approved:
        return SourceBrowserAgentActionResult(
            id=action_id,
            owner=owner,
            status="waiting",
            detail="Finalize waits for explicit provider-admin approval.",
            endpoint=_native_endpoint(native_connect, action_id, inputs.gateway_api_base),
        )
    endpoint = _native_endpoint(native_connect, action_id, inputs.gateway_api_base)
    if endpoint is None:
        return SourceBrowserAgentActionResult(
            id=action_id,
            owner=owner,
            status="blocked",
            detail="Native endpoint is missing gateway base or path.",
        )
    headers = dict(inputs.extra_headers)
    if inputs.bearer_token:
        headers["Authorization"] = f"Bearer {inputs.bearer_token}"
    try:
        async with httpx.AsyncClient(timeout=inputs.timeout_s) as client:
            response = await client.post(endpoint, json=native_payload, headers=headers)
    except httpx.HTTPError as exc:
        return SourceBrowserAgentActionResult(
            id=action_id,
            owner=owner,
            status="failed",
            detail=f"Native endpoint call failed: {type(exc).__name__}",
            endpoint=endpoint,
        )
    response_payload: dict[str, Any] = {}
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    if isinstance(parsed, dict):
        response_payload = parsed
    status_name = "completed" if response.status_code < 400 else "failed"
    response_state = str(response_payload.get("state") or "")
    if response.status_code < 400 and response_state.startswith("waiting"):
        status_name = "waiting"
    elif response.status_code < 400 and response_payload.get("ok") is False:
        status_name = "blocked"
    detail = (
        "Native endpoint call completed."
        if response.status_code < 400
        else "Native endpoint returned an error."
    )
    if isinstance(response_payload.get("message"), str):
        detail = str(response_payload["message"])
    return SourceBrowserAgentActionResult(
        id=action_id,
        owner=owner,
        status=status_name,
        detail=detail,
        endpoint=endpoint,
        http_status=response.status_code,
    )


def _native_endpoint(
    native_connect: dict[str, Any],
    action_id: str,
    gateway_api_base: str | None,
) -> str | None:
    if not gateway_api_base:
        return None
    path_key = "preflight_path" if action_id == "run_native_preflight" else "finalize_path"
    path = str(native_connect.get(path_key) or "")
    if not path:
        return None
    return urljoin(gateway_api_base.rstrip("/") + "/", path.lstrip("/"))


def _is_provider_setup_materialization(action: dict[str, Any]) -> bool:
    return str(action.get("kind") or "") == "materialize_provider_setup_bundle"


def _is_browser_dom_plan_materialization(action: dict[str, Any]) -> bool:
    return str(action.get("kind") or "") == "materialize_browser_dom_plan"


def _is_browser_dom_plan_execution(action: dict[str, Any]) -> bool:
    return str(action.get("kind") or "") == "execute_browser_dom_plan"


async def _run_browser_dom_action(
    *,
    action_id: str,
    owner: str,
    run: dict[str, Any],
    inputs: SourceBrowserAgentRunnerInputs,
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> SourceBrowserAgentActionResult:
    artifact_paths = _materialize_provider_setup_bundle(run, inputs.run_path)
    plan_path = _materialize_browser_dom_plan(run, inputs.run_path)
    launch_path = _materialize_browser_agent_launcher(run, inputs.run_path)
    refs_path = _materialize_customer_cloud_refs(run, inputs.run_path)
    if plan_path is None:
        return SourceBrowserAgentActionResult(
            id=action_id,
            owner=owner,
            status="skipped",
            detail="No provider browser DOM action plan is present.",
            endpoint=str(launch_path) if launch_path else None,
        )
    if not inputs.execute_browser_dom:
        return SourceBrowserAgentActionResult(
            id=action_id,
            owner=owner,
            status="ready",
            detail=(
                "Admin-present browser execution is prepared. Run this agent with "
                "--execute-browser-dom inside the customer BYOC runtime to drive the "
                "provider settings page and pause at human-only gates."
            ),
            endpoint=str(launch_path) if launch_path else str(plan_path),
        )
    try:
        receipt_path, status = await _execute_browser_dom_plan(
            plan=json.loads(plan_path.read_text(encoding="utf-8")),
            run_path=inputs.run_path,
            inputs=inputs,
            generated_secret_values=generated_secret_values,
            generated_secret_refs=generated_secret_refs,
        )
    except ModuleNotFoundError as exc:
        if exc.name and exc.name.startswith("playwright"):
            return SourceBrowserAgentActionResult(
                id=action_id,
                owner=owner,
                status="blocked",
                detail=(
                    "Playwright is not installed in this BYOC runtime. Install the "
                    "browser-agent extra and browser binaries, then rerun with "
                    "--execute-browser-dom."
                ),
                endpoint=str(launch_path) if launch_path else str(plan_path),
            )
        raise
    detail = "Provider browser DOM plan executed."
    if status == "waiting":
        detail = (
            "Provider browser DOM plan paused at a human-only gate. Keep an admin "
            "present for sign-in, MFA, credential creation, or final approval."
        )
    elif artifact_paths or refs_path:
        detail = "Provider browser DOM plan executed with local setup artifacts prepared."
    return SourceBrowserAgentActionResult(
        id=action_id,
        owner=owner,
        status=status,
        detail=detail,
        endpoint=str(receipt_path),
    )


def _materialize_provider_setup_bundle(
    run: dict[str, Any],
    run_path: Path,
) -> dict[str, str]:
    bundle = run.get("provider_setup_bundle")
    if not isinstance(bundle, dict):
        return {}
    artifacts = bundle.get("artifacts")
    if not isinstance(artifacts, list):
        return {}
    output_dir = _provider_setup_artifact_dir(run_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    generated: dict[str, str] = {}
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        filename = _safe_artifact_filename(str(artifact.get("filename") or ""))
        if not filename:
            continue
        content = _artifact_text(artifact)
        if content is None:
            continue
        path = output_dir / filename
        path.write_text(content, encoding="utf-8")
        name = str(artifact.get("name") or filename)
        generated[name] = str(path)
    return generated


def _materialize_browser_dom_plan(
    run: dict[str, Any],
    run_path: Path,
) -> Path | None:
    bundle = run.get("provider_setup_bundle")
    if not isinstance(bundle, dict):
        return None
    plan = bundle.get("browser_dom_plan")
    if not isinstance(plan, dict):
        return None
    output_dir = _provider_setup_artifact_dir(run_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "browser-dom-plan.json"
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _materialize_browser_agent_launcher(
    run: dict[str, Any],
    run_path: Path,
) -> Path | None:
    bundle = run.get("provider_setup_bundle")
    if not isinstance(bundle, dict) or not isinstance(bundle.get("browser_dom_plan"), dict):
        return None
    output_dir = _provider_setup_artifact_dir(run_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "run-provider-browser-agent.sh"
    source = str(run.get("source") or "source")
    command = (
        "fyralis byoc source browser-agent "
        f"--source {source.replace('_', '-')} "
        f"--run-artifact {run_path} "
        "--execute-browser-dom --interactive-admin"
    )
    path.write_text(
        "#!/usr/bin/env sh\n"
        "set -eu\n"
        f"{command} \"$@\"\n",
        encoding="utf-8",
    )
    path.chmod(0o700)
    return path


def _materialize_customer_cloud_refs(
    run: dict[str, Any],
    run_path: Path,
) -> Path | None:
    bundle = run.get("provider_setup_bundle")
    if not isinstance(bundle, dict):
        return None
    refs = bundle.get("generated_refs") or run.get("agent_generates") or []
    if not isinstance(refs, list) or not refs:
        return None
    source = str(run.get("source") or bundle.get("source") or "source")
    output_dir = _provider_setup_artifact_dir(run_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "fyralis.byoc.source.customer_cloud_refs.v1",
        "source": source,
        "generated_at": datetime.now(UTC).isoformat(),
        "refs": [
            _customer_cloud_ref_metadata(source, str(ref))
            for ref in refs
            if str(ref).strip()
        ],
        "raw_secret_values_included": False,
        "stored_scope": "customer_cloud_secret_reference_metadata_only",
    }
    path = output_dir / "customer-cloud-generated-refs.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _materialize_native_payload_template(
    *,
    run_path: Path,
    native_connect: dict[str, Any],
    action_id: str = "run_native_finalize",
    generated_secret_values: dict[str, str] | None = None,
    generated_secret_refs: dict[str, str] | None = None,
) -> Path:
    run = _load_run(run_path)
    output_dir = _provider_setup_artifact_dir(run_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    fields = _native_payload_fields_for_action(native_connect, action_id)
    collections = _load_browser_dom_collections(output_dir)
    generated_refs = {
        **_load_generated_secret_refs(output_dir, run),
        **dict(generated_secret_refs or {}),
    }
    payload: dict[str, Any] = {}
    field_status: dict[str, str] = {}
    for payload_field in fields:
        value, status = _native_payload_template_value(
            field=payload_field,
            run=run,
            native_connect=native_connect,
            collections=collections,
            generated_secret_values=generated_secret_values or {},
            generated_secret_refs=generated_refs,
        )
        payload[payload_field] = value
        field_status[payload_field] = status
    template = {
        "schema_version": "fyralis.byoc.source.native_payload_template.v1",
        "source": str(run.get("source") or ""),
        "native_connect_kind": native_connect.get("kind"),
        "preflight_path": native_connect.get("preflight_path"),
        "finalize_path": native_connect.get("finalize_path"),
        "action_id": action_id,
        "payload": payload,
        "field_status": field_status,
        "instructions": (
            "Review inside the customer BYOC runtime. Fill only fields marked "
            "customer_admin_secret_required or customer_admin_value_required, "
            "then pass this file with --native-payload. Fields marked "
            "auto_generated_secret_in_browser_session are generated in memory by "
            "the admin-present browser agent and are shown only as customer-cloud refs."
        ),
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "customer_cloud_native_payload_template_only",
    }
    path = output_dir / "native-payload-template.json"
    path.write_text(json.dumps(template, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _native_payload_fields_for_action(
    native_connect: dict[str, Any],
    action_id: str,
) -> list[str]:
    if action_id == "run_native_preflight":
        fields = native_connect.get("preflight_payload_fields")
        if isinstance(fields, list) and fields:
            return [str(field) for field in fields]
    if action_id == "run_native_finalize":
        fields = native_connect.get("finalize_payload_fields")
        if isinstance(fields, list) and fields:
            return [str(field) for field in fields]
    return [str(field) for field in native_connect.get("payload_fields") or []]


def _autofilled_native_payload_from_template(
    path: Path,
    *,
    generated_secret_values: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    try:
        template = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    payload = template.get("payload")
    field_status = template.get("field_status")
    if not isinstance(payload, dict) or not isinstance(field_status, dict):
        return None
    required_statuses = {
        "customer_admin_secret_required",
        "customer_admin_value_required",
    }
    generated = generated_secret_values or {}
    native_payload: dict[str, Any] = {}
    for payload_field, status in field_status.items():
        field_name = str(payload_field)
        if str(status) in required_statuses:
            return None
        if str(status) == "auto_generated_secret_in_browser_session":
            secret_value = generated.get(field_name)
            if not _native_payload_value_is_filled(secret_value):
                return None
            native_payload[field_name] = secret_value
            continue
        value = payload.get(field_name)
        if not _native_payload_value_is_filled(value):
            return None
        native_payload[field_name] = value
    return native_payload


def _native_payload_value_is_filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _native_payload_template_value(
    *,
    field: str,
    run: dict[str, Any],
    native_connect: dict[str, Any],
    collections: dict[str, str],
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> tuple[Any, str]:
    if field == "oauth_redirect_url":
        return run.get("oauth_redirect_url"), "auto_from_browser_agent_run"
    if field == "events_request_url":
        return run.get("events_request_url"), "auto_from_browser_agent_run"
    if field == "scope":
        aliases = native_connect.get("scope_aliases")
        if aliases:
            selected = str(list(aliases)[0]).strip()
            if selected:
                return selected, "auto_from_native_contract"
        return "", "customer_admin_value_required"
    if field == "include_shared_drives":
        return True, "auto_default"
    if field == "backfill_window_days":
        return 30, "auto_default"
    if field == "credential_kind":
        return "customer_admin_approved", "auto_default"
    if field == "repository_selection":
        return "selected", "customer_admin_value_required"
    if field in {"entities", "approved_channel_ids", "shared_page_ids", "shared_database_ids", "account_ids", "contract_ids", "file_keys", "project_keys", "board_ids", "threads", "dialogs"}:
        return [], "customer_admin_value_required"
    collected = _collection_value_for_field(field, collections)
    if collected:
        return collected, "auto_from_provider_page_collection"
    if _field_can_use_agent_generated_secret(field, run):
        generated_value = generated_secret_values.get(field)
        if generated_value:
            local_ref = generated_secret_refs.get(
                field,
                _generated_secret_ref_uri(str(run.get("source") or "source"), field),
            )
            return (
                {
                    "local_ref_hint": _redacted_ref_hint(local_ref),
                    "local_ref_sha256": hashlib.sha256(local_ref.encode("utf-8")).hexdigest(),
                    "raw_secret_value_included": False,
                },
                "auto_generated_secret_in_browser_session",
            )
        return "", "browser_agent_secret_generation_pending"
    if _native_payload_field_is_secret(field):
        return "", "customer_admin_secret_required"
    return "", "customer_admin_value_required"


def _native_payload_field_is_secret(field: str) -> bool:
    markers = (
        "api_hash",
        "api_key",
        "api_token",
        "app_secret",
        "client_secret",
        "linked_device_session",
        "live_session",
        "password",
        "private_key",
        "refresh_token",
        "secret",
        "service_account_token",
        "token",
    )
    lowered = field.lower()
    return any(marker in lowered for marker in markers)


def _field_can_use_agent_generated_secret(field: str, run: dict[str, Any]) -> bool:
    return any(
        _generated_secret_field_for_ref(str(run.get("source") or "source"), label) == field
        for label in _run_generated_ref_labels(run)
    )


def _run_generated_ref_labels(run: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    bundle = run.get("provider_setup_bundle")
    if isinstance(bundle, dict):
        labels.extend(str(item) for item in bundle.get("generated_refs") or [])
    labels.extend(str(item) for item in run.get("agent_generates") or [])
    return [label for label in labels if label.strip()]


def _generated_secret_field_for_ref(source: str, label: str) -> str | None:
    normalized_source = source.strip().lower().replace("-", "_")
    slug = _slug(label)
    if "app-secret" in slug or "api-hash" in slug or "session" in slug:
        return None
    if "verify-token" in slug:
        return "verify_token"
    if "webhook-verifier-token" in slug:
        return "webhook_verifier_token"
    if "webhook-passcode" in slug:
        return "webhook_secret"
    if "webhook-secret" in slug:
        return "webhook_secret"
    if "webhook-signing-secret" in slug:
        return "webhook_secret"
    if "webhook-verifier" in slug and normalized_source not in {"slack"}:
        return "webhook_verifier_token"
    return None


def _generated_secret_ref_uri(source: str, field: str) -> str:
    return f"customer-cloud://fyralis/sources/{_slug(source)}/{_slug(field)}"


def _ensure_generated_secret(
    *,
    source: str,
    field: str,
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> str:
    if field not in generated_secret_values:
        generated_secret_values[field] = f"fyralis-{secrets.token_urlsafe(32)}"
    generated_secret_refs.setdefault(field, _generated_secret_ref_uri(source, field))
    return generated_secret_values[field]


def _load_generated_secret_refs(output_dir: Path, run: dict[str, Any]) -> dict[str, str]:
    source = str(run.get("source") or "source")
    out: dict[str, str] = {}
    for path in (
        output_dir / "browser-dom-generated-refs.json",
        output_dir / "customer-cloud-generated-refs.json",
    ):
        if not path.is_file():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        refs = payload.get("refs")
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            field = _generated_secret_field_for_ref(source, str(ref.get("label") or ""))
            if not field:
                continue
            local_ref = str(ref.get("local_ref") or "").strip()
            secret_name = str(ref.get("secret_name") or "").strip()
            if local_ref:
                out[field] = local_ref
            elif secret_name:
                out[field] = f"customer-cloud-secret://{secret_name.lstrip('/')}"
            else:
                out[field] = _generated_secret_ref_uri(source, field)
    return out


def _load_browser_dom_collections(output_dir: Path) -> dict[str, str]:
    path = output_dir / "browser-dom-collections.json"
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    fields = payload.get("fields")
    snippets = payload.get("snippets")
    out: dict[str, str] = {}
    if isinstance(snippets, list):
        for snippet in snippets:
            text = str(snippet).strip()
            if not text:
                continue
            for field in fields if isinstance(fields, list) else []:
                field_name = str(field).strip()
                value = _extract_collection_value(text, field_name)
                if value:
                    out[_slug(field_name)] = value
            for field, aliases in _collection_field_aliases().items():
                for label in (field, *aliases):
                    value = _extract_collection_value(text, label)
                    if value:
                        out[_slug(label)] = value
    return out


def _collection_value_for_field(field: str, collections: dict[str, str]) -> str | None:
    aliases = [field, field.replace("_", " ")]
    aliases.extend(_collection_field_aliases().get(field, ()))
    for alias in aliases:
        value = collections.get(_slug(alias))
        if value:
            return value
    return None


def _collection_field_aliases() -> dict[str, tuple[str, ...]]:
    return {
        "account_id": ("account id", "aws account id"),
        "admin_email": ("admin email", "administrator email"),
        "application_id": ("application id", "client id"),
        "base_url": ("base url", "api base url", "instance url", "site url"),
        "business_account_id": ("business account id", "whatsapp business account id"),
        "business_id": ("business id", "ramp business id"),
        "company_id": ("company id",),
        "company_uuid": ("company uuid", "company id"),
        "display_phone_number": ("display phone number", "phone number"),
        "firm_id": ("firm id",),
        "guild_id": ("guild id", "server id"),
        "installation_id": ("installation id",),
        "issuer_id": ("issuer id",),
        "organization": ("organization", "org login"),
        "organization_id": ("organization id", "org id"),
        "organization_urn": ("organization urn",),
        "org_id": ("org id", "organization id"),
        "phone_number_id": ("phone number id",),
        "realm_id": ("realm id", "company realm id"),
        "region": ("region", "aws region"),
        "role_arn": ("role arn",),
        "service_user_id": ("service user id",),
        "site_url": ("site url",),
        "team_id": ("team id",),
        "workspace_domain": ("workspace domain", "domain"),
        "workspace_id": ("workspace id", "team id"),
    }


def _extract_collection_value(text: str, label: str) -> str | None:
    normalized_label = re.escape(label.replace("_", " "))
    patterns = [
        rf"(?i)\b{normalized_label}\b\s*[:=]\s*([^\n\r,;]+)",
        rf"(?i)\b{normalized_label}\b\s+([A-Za-z0-9_.:/@-]{{3,}})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text.replace("_", " "))
        if match:
            return match.group(1).strip().strip('"').strip("'")[:200]
    return None


def _targeted_collection_snippets(texts: list[str], fields: list[str]) -> list[str]:
    snippets: list[str] = []
    seen: set[str] = set()
    for field_name in fields:
        if not _collection_field_can_be_persisted(field_name):
            continue
        labels = [field_name, field_name.replace("_", " ")]
        labels.extend(_collection_field_aliases().get(field_name, ()))
        for text in texts:
            redacted_text = _redact_text(text)
            value = None
            for label in labels:
                value = _extract_collection_value(redacted_text, label)
                if value:
                    break
            if not value or "[redacted]" in value.lower():
                continue
            snippet = f"{field_name}: {value}"
            if snippet in seen:
                continue
            seen.add(snippet)
            snippets.append(snippet)
            break
    return snippets


def _collection_field_can_be_persisted(field: str) -> bool:
    lowered = field.strip().lower()
    if not lowered:
        return False
    blocked_terms = (
        "api hash",
        "api key",
        "api token",
        "app secret",
        "client secret",
        "linked device session",
        "live session",
        "password",
        "private key",
        "refresh token",
        "secret",
        "service account token",
        "token",
    )
    return not any(term in lowered for term in blocked_terms)


async def _execute_browser_dom_plan(
    *,
    plan: dict[str, Any],
    run_path: Path,
    inputs: SourceBrowserAgentRunnerInputs,
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> tuple[Path, str]:
    from playwright.async_api import async_playwright  # type: ignore[import-not-found]

    output_dir = _provider_setup_artifact_dir(run_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    storage_state_path = inputs.browser_storage_state_path
    step_results: list[dict[str, Any]] = []
    status = "completed"
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=inputs.browser_headless,
            slow_mo=inputs.browser_slow_mo_ms,
        )
        context_kwargs: dict[str, Any] = {}
        if storage_state_path is not None and storage_state_path.is_file():
            context_kwargs["storage_state"] = str(storage_state_path)
        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()
        try:
            for step in plan.get("steps") or []:
                if not isinstance(step, dict):
                    continue
                result = await _execute_browser_dom_step(
                    page=page,
                    step=step,
                    source=str(plan.get("source") or "source"),
                    output_dir=output_dir,
                    timeout_ms=int(inputs.browser_timeout_s * 1000),
                    admin_approved=inputs.admin_approved,
                    interactive_admin=inputs.interactive_admin,
                    gateway_api_base=inputs.gateway_api_base,
                    bearer_token=inputs.bearer_token,
                    extra_headers=inputs.extra_headers,
                    http_timeout_s=inputs.timeout_s,
                    generated_secret_values=generated_secret_values,
                    generated_secret_refs=generated_secret_refs,
                )
                step_results.append(result)
                if result["status"] in {"waiting", "failed", "blocked"}:
                    status = result["status"]
                    break
            if storage_state_path is not None:
                await context.storage_state(path=str(storage_state_path))
        finally:
            await browser.close()
    receipt = {
        "schema_version": "fyralis.byoc.source.browser_dom_execution_receipt.v1",
        "source": plan.get("source"),
        "generated_at": datetime.now(UTC).isoformat(),
        "status": status,
        "step_results": step_results,
        "browser_storage_state_persisted": storage_state_path is not None,
        "raw_secret_values_included": False,
        "stored_scope": "sanitized_browser_dom_execution_metadata_only",
    }
    receipt_path = output_dir / "browser-dom-execution-receipt.json"
    receipt_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt_path, status


async def _execute_browser_dom_step(
    *,
    page: Any,
    step: dict[str, Any],
    source: str,
    output_dir: Path,
    timeout_ms: int,
    admin_approved: bool,
    interactive_admin: bool,
    gateway_api_base: str | None,
    bearer_token: str | None,
    extra_headers: dict[str, str],
    http_timeout_s: float,
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> dict[str, Any]:
    step_id = str(step.get("id") or "browser_dom_step")
    action = str(step.get("action") or "")
    try:
        if action == "goto":
            target_url = str(step.get("target_url") or "")
            if not _is_external_url(target_url):
                return _dom_step_result(step, "skipped", "No external provider URL is available.")
            await page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
            return _dom_step_result(step, "completed", "Opened provider settings page.")
        if action == "human_pause":
            reason = str(
                step.get("human_reason")
                or "Provider requires customer-admin sign-in, MFA, credential creation, or approval."
            )
            if interactive_admin:
                await _wait_for_interactive_admin(step_id, reason)
                return _dom_step_result(step, "acknowledged", "Customer admin completed the gate locally.")
            if admin_approved:
                return _dom_step_result(step, "acknowledged", "Customer admin gate was pre-approved locally.")
            return _dom_step_result(step, "waiting", reason)
        if action == "click":
            return await _dom_click(page, step, timeout_ms)
        if action == "set_url":
            return await _dom_fill_values(page, step, [str(step.get("value") or "")], timeout_ms)
        if action == "set_urls":
            values = [str(value) for value in (step.get("values") or {}).values() if value]
            return await _dom_fill_values(page, step, values, timeout_ms)
        if action == "set_config_values":
            return await _dom_set_config_values(
                page=page,
                step=step,
                source=source,
                timeout_ms=timeout_ms,
                generated_secret_values=generated_secret_values,
                generated_secret_refs=generated_secret_refs,
            )
        if action in {"paste_or_upload_manifest", "fill_from_artifact", "apply_generated_artifact"}:
            return await _dom_apply_artifact(page, step, output_dir, timeout_ms)
        if action == "slack_app_config_token_auto_connect":
            return await _dom_slack_app_config_token_auto_connect(
                page=page,
                step=step,
                timeout_ms=timeout_ms,
                admin_approved=admin_approved,
                interactive_admin=interactive_admin,
                gateway_api_base=gateway_api_base,
                bearer_token=bearer_token,
                extra_headers=extra_headers,
                http_timeout_s=http_timeout_s,
            )
        if action == "collect_text":
            return await _dom_collect_text(page, step, output_dir, timeout_ms)
        if action == "generate_refs":
            return _dom_generate_refs(
                step,
                output_dir,
                source=source,
                generated_secret_values=generated_secret_values,
                generated_secret_refs=generated_secret_refs,
            )
        if action == "verify":
            return await _dom_verify(page, step, timeout_ms)
        return _dom_step_result(step, "skipped", f"Unsupported browser DOM action: {action}.")
    except Exception as exc:  # noqa: BLE001
        return _dom_step_result(step, "failed", f"{type(exc).__name__}: {exc}")


async def _dom_click(page: Any, step: dict[str, Any], timeout_ms: int) -> dict[str, Any]:
    for text in step.get("text_targets") or []:
        try:
            await _first_locator(
                page.get_by_text(re.compile(re.escape(str(text)), re.I))
            ).click(
                timeout=timeout_ms
            )
            return _dom_step_result(step, "completed", f"Clicked provider control matching {text}.")
        except Exception:  # noqa: BLE001
            pass
    for selector in step.get("selectors") or []:
        try:
            await _first_locator(page.locator(str(selector))).click(timeout=timeout_ms)
            return _dom_step_result(step, "completed", f"Clicked provider control {selector}.")
        except Exception:  # noqa: BLE001
            pass
    return _dom_step_result(step, "blocked", "No matching provider control was found.")


async def _dom_fill_values(
    page: Any,
    step: dict[str, Any],
    values: list[str],
    timeout_ms: int,
) -> dict[str, Any]:
    values = [value for value in values if value]
    if not values:
        return _dom_step_result(step, "skipped", "No generated URL value is available.")
    selectors = list(step.get("selectors") or ["input[type=url]", "textarea", "input"])
    filled = 0
    for value in values:
        if await _fill_first_match(page, selectors, value, timeout_ms):
            filled += 1
    if filled:
        submitted = await _submit_after_provider_change(page, step, timeout_ms)
        suffix = " and submitted the provider form" if submitted else ""
        return _dom_step_result(
            step,
            "completed",
            f"Filled {filled} provider URL field(s){suffix}.",
        )
    return _dom_step_result(step, "blocked", "No writable provider URL field was found.")


async def _dom_set_config_values(
    *,
    page: Any,
    step: dict[str, Any],
    source: str,
    timeout_ms: int,
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> dict[str, Any]:
    field_specs = step.get("fields")
    if not isinstance(field_specs, list):
        return _dom_step_result(step, "skipped", "No provider configuration fields are defined.")
    filled = 0
    missing_targets: list[str] = []
    for field_spec in field_specs:
        if not isinstance(field_spec, dict):
            continue
        field_name = str(field_spec.get("name") or "provider field")
        generated_field = str(field_spec.get("generated_secret_field") or "").strip()
        if generated_field:
            value = _ensure_generated_secret(
                source=source,
                field=generated_field,
                generated_secret_values=generated_secret_values,
                generated_secret_refs=generated_secret_refs,
            )
        else:
            value = str(field_spec.get("value") or "").strip()
        if not value:
            continue
        selectors = [
            str(selector)
            for selector in (field_spec.get("selectors") or step.get("selectors") or [])
            if str(selector).strip()
        ]
        if await _fill_first_match(page, selectors or ["input", "textarea"], value, timeout_ms):
            filled += 1
            continue
        missing_targets.append(field_name)
    if not filled:
        return _dom_step_result(step, "blocked", "No writable provider configuration field was found.")
    submitted = await _submit_after_provider_change(page, step, timeout_ms)
    suffix = " and submitted the provider form" if submitted else ""
    detail = f"Filled {filled} provider configuration field(s){suffix}."
    if missing_targets:
        detail += f" Missing writable target(s): {', '.join(missing_targets[:3])}."
    return _dom_step_result(step, "completed", detail)


async def _dom_apply_artifact(
    page: Any,
    step: dict[str, Any],
    output_dir: Path,
    timeout_ms: int,
) -> dict[str, Any]:
    artifact_path = _first_existing_artifact(output_dir, step.get("artifacts") or [])
    if artifact_path is None:
        return _dom_step_result(step, "skipped", "No generated provider artifact is available.")
    file_inputs = [selector for selector in step.get("selectors") or [] if "file" in str(selector)]
    for selector in file_inputs or ["input[type=file]"]:
        try:
            await _first_locator(page.locator(str(selector))).set_input_files(
                str(artifact_path),
                timeout=timeout_ms,
            )
            submitted = await _submit_after_provider_change(page, step, timeout_ms)
            suffix = " and submitted the provider form" if submitted else ""
            return _dom_step_result(
                step,
                "completed",
                f"Uploaded {artifact_path.name}{suffix}.",
            )
        except Exception:  # noqa: BLE001
            pass
    content = artifact_path.read_text(encoding="utf-8")
    if await _fill_first_match(page, ["textarea", "input[type=text]", "input"], content, timeout_ms):
        submitted = await _submit_after_provider_change(page, step, timeout_ms)
        suffix = " and submitted the provider form" if submitted else ""
        return _dom_step_result(
            step,
            "completed",
            f"Pasted {artifact_path.name}{suffix}.",
        )
    return _dom_step_result(step, "blocked", "No manifest upload or paste target was found.")


async def _dom_collect_text(
    page: Any,
    step: dict[str, Any],
    output_dir: Path,
    timeout_ms: int,
) -> dict[str, Any]:
    texts: list[str] = []
    for selector in step.get("selectors") or []:
        try:
            selected_texts = await page.locator(str(selector)).all_text_contents(timeout=timeout_ms)
        except TypeError:
            selected_texts = await page.locator(str(selector)).all_text_contents()
        except Exception:  # noqa: BLE001
            continue
        texts.extend(str(text) for text in selected_texts if str(text).strip())
    fields = [str(field).strip() for field in step.get("fields") or [] if str(field).strip()]
    snippets = _targeted_collection_snippets(texts, fields)
    payload = {
        "schema_version": "fyralis.byoc.source.browser_dom_collection.v1",
        "step_id": step.get("id"),
        "fields": fields,
        "snippets": [snippet for snippet in snippets[:25] if snippet],
        "raw_secret_values_included": False,
        "collection_policy": "targeted_non_secret_field_extraction_only",
    }
    path = output_dir / "browser-dom-collections.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return _dom_step_result(step, "completed", f"Collected sanitized provider text into {path.name}.")


async def _dom_slack_app_config_token_auto_connect(
    *,
    page: Any,
    step: dict[str, Any],
    timeout_ms: int,
    admin_approved: bool,
    interactive_admin: bool,
    gateway_api_base: str | None,
    bearer_token: str | None,
    extra_headers: dict[str, str],
    http_timeout_s: float,
) -> dict[str, Any]:
    target_url = str(step.get("target_url") or "")
    if _is_external_url(target_url):
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
        except Exception:  # noqa: BLE001
            pass
    await _dom_click_optional_text_targets(page, step.get("text_targets") or [], timeout_ms)
    await _dom_wait_after_provider_change(page, timeout_ms)

    token = await _dom_find_slack_app_config_token(page, step, timeout_ms)
    if not token and interactive_admin:
        await _wait_for_interactive_admin(
            str(step.get("id") or "slack_app_config_token"),
            str(
                step.get("human_reason")
                or "Generate a Slack app configuration token, then continue."
            ),
        )
        token = await _dom_find_slack_app_config_token(page, step, timeout_ms)
    if not token and not admin_approved:
        return _dom_step_result(
            step,
            "waiting",
            (
                "Slack app configuration token is not visible yet. Sign in to "
                "api.slack.com, generate the token, then rerun the browser agent."
            ),
        )
    if not token:
        return _dom_step_result(
            step,
            "blocked",
            "Slack app configuration token was not visible in the admin browser.",
        )

    endpoint = _slack_app_config_token_endpoint(gateway_api_base, step)
    if endpoint is None:
        return _dom_step_result(
            step,
            "blocked",
            "Gateway endpoint for Slack app configuration token handoff is missing.",
        )
    payload, http_status, submit_error = await _submit_slack_app_config_token(
        endpoint=endpoint,
        token=token,
        bearer_token=bearer_token,
        extra_headers=extra_headers,
        timeout_s=http_timeout_s,
    )
    if submit_error:
        result = _dom_step_result(
            step,
            "failed",
            f"Slack app configuration token handoff failed: {submit_error}.",
        )
        result["endpoint"] = endpoint
        result["http_status"] = http_status
        return result
    install_url = _slack_install_url_from_payload(payload)
    install_opened = False
    if _is_external_url(install_url):
        try:
            await page.goto(str(install_url), wait_until="domcontentloaded", timeout=timeout_ms)
            install_opened = True
        except Exception:  # noqa: BLE001
            install_opened = False
    status = "waiting" if install_url else "completed"
    detail = (
        "Slack app was created through the gateway; OAuth approval is open."
        if install_opened
        else "Slack app was created through the gateway; OAuth approval is ready."
        if install_url
        else "Slack app configuration token was accepted by the gateway."
    )
    result = _dom_step_result(step, status, detail)
    result["endpoint"] = endpoint
    result["http_status"] = http_status
    result["install_url_opened"] = install_opened
    return result


async def _dom_click_optional_text_targets(
    page: Any,
    text_targets: list[Any],
    timeout_ms: int,
) -> bool:
    bounded_timeout = min(timeout_ms, 5000)
    for text in text_targets:
        candidate = str(text).strip()
        if not candidate:
            continue
        try:
            await _first_locator(
                page.get_by_text(re.compile(re.escape(candidate), re.I))
            ).click(timeout=bounded_timeout)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def _dom_wait_after_provider_change(page: Any, timeout_ms: int) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 5000))
    except Exception:  # noqa: BLE001
        pass
    try:
        await page.wait_for_timeout(500)
    except Exception:  # noqa: BLE001
        pass


async def _dom_find_slack_app_config_token(
    page: Any,
    step: dict[str, Any],
    timeout_ms: int,
) -> str | None:
    texts: list[str] = []
    selectors = [
        str(selector)
        for selector in (step.get("token_selectors") or step.get("selectors") or [])
        if str(selector).strip()
    ]
    for selector in selectors:
        locator = page.locator(selector)
        try:
            selected_texts = await locator.all_text_contents(timeout=min(timeout_ms, 5000))
        except TypeError:
            try:
                selected_texts = await locator.all_text_contents()
            except Exception:  # noqa: BLE001
                selected_texts = []
        except Exception:  # noqa: BLE001
            selected_texts = []
        texts.extend(str(text) for text in selected_texts if str(text).strip())
        try:
            values = await locator.evaluate_all(
                "(els) => els.map((el) => el.value || el.textContent || '').filter(Boolean)"
            )
        except Exception:  # noqa: BLE001
            values = []
        if isinstance(values, list):
            texts.extend(str(value) for value in values if str(value).strip())
    try:
        values = await page.evaluate(
            "() => Array.from(document.querySelectorAll('input, textarea, code, pre'))"
            ".map((el) => el.value || el.textContent || '').filter(Boolean)"
        )
    except Exception:  # noqa: BLE001
        values = []
    if isinstance(values, list):
        texts.extend(str(value) for value in values if str(value).strip())
    for text in texts:
        token = _extract_slack_app_config_token(text)
        if token:
            return token
    return None


def _extract_slack_app_config_token(text: str) -> str | None:
    match = _SLACK_APP_CONFIG_TOKEN_RE.search(text)
    return match.group(0) if match else None


def _slack_app_config_token_endpoint(
    gateway_api_base: str | None,
    step: dict[str, Any],
) -> str | None:
    if not gateway_api_base:
        return None
    path = str(
        step.get("gateway_finalize_path")
        or "/platform/onboarding/sources/slack/rehearsal/browser-agent/configuration"
    )
    return urljoin(gateway_api_base.rstrip("/") + "/", path.lstrip("/"))


async def _submit_slack_app_config_token(
    *,
    endpoint: str,
    token: str,
    bearer_token: str | None,
    extra_headers: dict[str, str],
    timeout_s: float,
) -> tuple[dict[str, Any], int | None, str | None]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        **extra_headers,
    }
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            response = await client.post(
                endpoint,
                json={"inputs": {"slack_app_config_token": token}},
                headers=headers,
            )
    except httpx.HTTPError as exc:
        return {}, None, type(exc).__name__
    payload: dict[str, Any] = {}
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    if isinstance(parsed, dict):
        payload = parsed
    if response.status_code >= 400:
        error_name = "gateway_error"
        detail = payload.get("detail")
        if isinstance(detail, dict) and isinstance(detail.get("error"), str):
            error_name = detail["error"]
        elif isinstance(payload.get("error"), str):
            error_name = str(payload["error"])
        return payload, response.status_code, error_name
    return payload, response.status_code, None


def _slack_install_url_from_payload(payload: dict[str, Any]) -> str | None:
    for candidate in (
        payload.get("install_url"),
        (payload.get("auto_connect") or {}).get("install_url")
        if isinstance(payload.get("auto_connect"), dict)
        else None,
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _dom_generate_refs(
    step: dict[str, Any],
    output_dir: Path,
    *,
    source: str,
    generated_secret_values: dict[str, str],
    generated_secret_refs: dict[str, str],
) -> dict[str, Any]:
    refs = [str(ref) for ref in step.get("refs") or [] if str(ref).strip()]
    ref_payloads: list[dict[str, Any]] = []
    for ref in refs:
        field = _generated_secret_field_for_ref(source, ref)
        if field:
            _ensure_generated_secret(
                source=source,
                field=field,
                generated_secret_values=generated_secret_values,
                generated_secret_refs=generated_secret_refs,
            )
        ref_payloads.append(
            {
                "label": ref,
                "field": field,
                **_dom_ref_metadata(ref, field, generated_secret_refs),
                "raw_secret_value_included": False,
            }
        )
    payload = {
        "schema_version": "fyralis.byoc.source.browser_dom_ref_generation.v1",
        "step_id": step.get("id"),
        "refs": ref_payloads,
        "raw_secret_values_included": False,
    }
    path = output_dir / "browser-dom-generated-refs.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return _dom_step_result(step, "completed", f"Generated {len(refs)} customer-cloud ref placeholder(s).")


async def _dom_verify(page: Any, step: dict[str, Any], timeout_ms: int) -> dict[str, Any]:
    for text in step.get("text_targets") or []:
        try:
            await _first_locator(
                page.get_by_text(re.compile(re.escape(str(text)), re.I))
            ).wait_for(timeout=timeout_ms)
            return _dom_step_result(step, "completed", f"Provider page shows {text}.")
        except Exception:  # noqa: BLE001
            pass
    return _dom_step_result(step, "ready", "Verification target was not visible yet.")


async def _fill_first_match(
    page: Any,
    selectors: list[str],
    value: str,
    timeout_ms: int,
) -> bool:
    for selector in selectors:
        try:
            locator = _first_locator(page.locator(str(selector)))
            await locator.fill(value, timeout=timeout_ms)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def _submit_after_provider_change(
    page: Any,
    step: dict[str, Any],
    timeout_ms: int,
) -> bool:
    submit_words = (
        "allow",
        "approve",
        "authorize",
        "confirm",
        "continue",
        "create",
        "enable",
        "install",
        "next",
        "register",
        "save",
        "submit",
        "subscribe",
        "update",
        "verify",
    )
    for text in step.get("text_targets") or []:
        candidate = str(text).strip()
        if not candidate or not any(word in candidate.lower() for word in submit_words):
            continue
        try:
            await _first_locator(
                page.get_by_text(re.compile(re.escape(candidate), re.I))
            ).click(
                timeout=timeout_ms
            )
            return True
        except Exception:  # noqa: BLE001
            continue
    for selector in (
        "button[type=submit]",
        "input[type=submit]",
        "button:has-text('Save')",
        "button:has-text('Create')",
        "button:has-text('Update')",
        "button:has-text('Install')",
        "button:has-text('Authorize')",
    ):
        try:
            await _first_locator(page.locator(selector)).click(timeout=timeout_ms)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _first_locator(locator: Any) -> Any:
    first = getattr(locator, "first")
    return first() if callable(first) else first


async def _wait_for_interactive_admin(step_id: str, reason: str) -> None:
    prompt = textwrap.dedent(
        f"""

        Fyralis browser agent paused at {step_id}.
        {reason}
        Complete the provider step in the opened browser, then press Enter here to continue.
        """
    ).strip()
    await asyncio.to_thread(input, prompt + "\n")


def _dom_step_result(step: dict[str, Any], status: str, detail: str) -> dict[str, Any]:
    return {
        "id": str(step.get("id") or ""),
        "action": str(step.get("action") or ""),
        "status": status,
        "detail": detail,
        "raw_secret_values_included": False,
    }


def _first_existing_artifact(output_dir: Path, filenames: list[Any]) -> Path | None:
    for filename in filenames:
        path = output_dir / _safe_artifact_filename(str(filename))
        if path.is_file():
            return path
    return None


def _secret_ref_name(source: str, label: str) -> str:
    return f"/fyralis/sources/{source.replace('_', '-')}/{_slug(label)}-{secrets.token_hex(4)}"


def _customer_cloud_ref_metadata(source: str, label: str) -> dict[str, Any]:
    secret_name = _secret_ref_name(source, label)
    return {
        "label": label,
        "secret_name_hint": _redacted_ref_hint(secret_name),
        "secret_name_sha256": hashlib.sha256(secret_name.encode("utf-8")).hexdigest(),
        "storage": "customer_cloud_secret_manager",
        "raw_secret_value_included": False,
    }


def _dom_ref_metadata(
    label: str,
    field: str | None,
    generated_secret_refs: dict[str, str],
) -> dict[str, str]:
    if field:
        local_ref = generated_secret_refs[field]
        return {
            "local_ref_hint": _redacted_ref_hint(local_ref),
            "local_ref_sha256": hashlib.sha256(local_ref.encode("utf-8")).hexdigest(),
        }
    local_ref = f"customer-cloud://fyralis/sources/{_slug(label)}/{secrets.token_hex(4)}"
    return {
        "local_ref_hint": _redacted_ref_hint(local_ref),
        "local_ref_sha256": hashlib.sha256(local_ref.encode("utf-8")).hexdigest(),
    }


def _redacted_ref_hint(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    separator = "/" if "/" in text else ":"
    parts = text.rsplit(separator, 1)
    if len(parts) == 1:
        return "[generated]"
    return f"{parts[0]}{separator}[generated]"


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return normalized or "ref"


def _redact_text(value: str) -> str:
    text = str(value).strip()
    patterns = [
        r"(?i)(secret|token|password|api[_ -]?key)\s*[:=]\s*[^\s,;]+",
        r"xox[baprs]-[A-Za-z0-9-]+",
        r"gh[pousr]_[A-Za-z0-9_]+",
        r"sk-[A-Za-z0-9_-]+",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "[redacted]", text)
    return text[:500]


def _generated_provider_setup_artifacts(run_path: Path) -> dict[str, str]:
    output_dir = _provider_setup_artifact_dir(run_path)
    if not output_dir.is_dir():
        return {}
    return {
        path.stem: str(path)
        for path in sorted(output_dir.iterdir())
        if path.is_file() and _is_sanitized_setup_artifact(path)
    }


def _is_sanitized_setup_artifact(path: Path) -> bool:
    lowered = path.name.lower()
    if "storage-state" in lowered or lowered.endswith(".local.json"):
        return False
    return True


def _provider_setup_artifact_dir(run_path: Path) -> Path:
    parent = run_path.parent if run_path.suffix else run_path
    return parent / "browser-agent-provider-setup"


def _safe_artifact_filename(filename: str) -> str:
    name = Path(filename).name
    if name in {"", ".", ".."}:
        return ""
    return name


def _artifact_text(artifact: dict[str, Any]) -> str | None:
    if isinstance(artifact.get("content"), str):
        return str(artifact["content"])
    if "json" in artifact:
        return json.dumps(artifact["json"], indent=2, sort_keys=True) + "\n"
    return None


def _receipt_status(
    run: dict[str, Any],
    results: list[SourceBrowserAgentActionResult],
) -> RunnerStatus:
    if str(run.get("state")) == "connected":
        return "connected"
    if any(item.status == "failed" for item in results):
        return "failed"
    if any(item.status == "blocked" for item in results):
        return "blocked"
    if any(item.status == "waiting" for item in results):
        return "waiting_for_admin"
    return "running"


def _load_run(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    run = payload.get("browser_agent_run", payload)
    if not isinstance(run, dict):
        raise ValueError("browser agent run must be a JSON object")
    if run.get("schema_version") != "fyralis.byoc.source.browser_agent_run.v1":
        raise ValueError("unsupported browser agent run schema")
    return run


def _load_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _is_external_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


__all__ = [
    "SourceBrowserAgentRunnerInputs",
    "SourceBrowserAgentRunnerReceipt",
    "run_source_browser_agent",
]
