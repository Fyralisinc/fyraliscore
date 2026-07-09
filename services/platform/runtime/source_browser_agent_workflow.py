"""Executable source browser-agent workflow contract.

This module turns a per-source browser-agent recipe plus current install state
into a small run-state object. The object is safe to send through hosted control
surfaces because it contains only sanitized metadata, generated-ref labels, and
provider handoff URLs.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from services.platform.runtime.source_browser_agent_recipes import (
    browser_agent_recipe_for_source,
)
from services.platform.runtime.source_browser_agent_setup import (
    build_source_provider_setup_bundle,
    provider_setup_bundle_actions,
)


def source_browser_agent_run_for_payload(
    source: str,
    payload: dict[str, Any],
    *,
    auto_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a browser-agent run from gateway-style source payloads."""
    automation_profile = payload.get("automation_profile") or {}
    status_payload = payload.get("status") or {}
    return build_source_browser_agent_run(
        source=source,
        recipe=payload.get("browser_agent"),
        auto_state=auto_state,
        installed=bool(status_payload.get("installed")),
        observation_count=int(status_payload.get("observation_count") or 0),
        missing_configuration=payload.get("missing_configuration") or [],
        human_steps=automation_profile.get("human_steps") or [],
        automated_actions=automation_profile.get("automated_actions") or [],
        install_url=payload.get("install_url"),
        provider_console_url=payload.get("provider_console_url"),
        oauth_redirect_url=payload.get("oauth_redirect_url"),
        events_request_url=payload.get("events_request_url"),
        finalize_mode=payload.get("finalize_mode"),
        native_connect=payload.get("native_connect"),
        provider_setup_bundle=payload.get("provider_setup_bundle"),
    )


def build_source_browser_agent_run(
    *,
    source: str,
    recipe: dict[str, Any] | None = None,
    auto_state: dict[str, Any] | None = None,
    installed: bool = False,
    observation_count: int = 0,
    missing_configuration: list[str] | tuple[str, ...] | None = None,
    human_steps: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    automated_actions: list[str] | tuple[str, ...] | None = None,
    install_url: str | None = None,
    provider_console_url: str | None = None,
    oauth_redirect_url: str | None = None,
    events_request_url: str | None = None,
    finalize_mode: str | None = None,
    native_connect: dict[str, Any] | None = None,
    provider_setup_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the sanitized contract consumed by a customer-cloud browser agent."""
    normalized_source = _normalize_source(source)
    recipe_payload = recipe or browser_agent_recipe_for_source(normalized_source)
    missing = [str(item) for item in (missing_configuration or []) if str(item)]
    gates = list(human_steps or [])
    actions = [str(item) for item in (automated_actions or []) if str(item)]
    state = _agent_state(
        source=normalized_source,
        auto_state=auto_state,
        installed=installed,
        missing_configuration=missing,
        human_steps=gates,
        install_url=install_url,
        finalize_mode=finalize_mode,
    )
    provider_console = provider_console_url or str(
        recipe_payload.get("provider_console_url") or ""
    )
    handoff_url = _handoff_url(
        install_url=install_url,
        provider_console_url=provider_console,
    )
    setup_bundle = provider_setup_bundle or build_source_provider_setup_bundle(
        source=normalized_source,
        recipe=recipe_payload,
        provider_console_url=provider_console or None,
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
        install_url=install_url,
        native_connect=native_connect,
    )
    action_queue = _action_queue(
        state=state,
        recipe=recipe_payload,
        automated_actions=actions,
        human_steps=gates,
        handoff_url=handoff_url,
        provider_setup_bundle=setup_bundle,
    )
    run = {
        "schema_version": "fyralis.byoc.source.browser_agent_run.v1",
        "source": normalized_source,
        "state": state,
        "launch_mode": "customer_cloud_admin_present_browser",
        "can_start": state not in {"blocked", "connected"},
        "handoff_url": handoff_url,
        "handoff_kind": _handoff_kind(
            install_url=install_url,
            provider_console_url=provider_console,
            handoff_url=handoff_url,
        ),
        "provider_console_url": provider_console or None,
        "oauth_redirect_url": oauth_redirect_url,
        "events_request_url": events_request_url,
        "provider_setup_bundle": setup_bundle,
        "settings_targets": list(recipe_payload.get("settings_targets") or []),
        "agent_collects": list(recipe_payload.get("agent_collects") or []),
        "agent_generates": list(recipe_payload.get("agent_generates") or []),
        "human_gates": _human_gates(recipe_payload, gates, state=state),
        "completion_checks": _completion_checks(
            recipe_payload,
            connected=installed,
            observation_count=observation_count,
        ),
        "action_queue": action_queue,
        "current_action": action_queue[0] if action_queue else None,
        "automated_action_count": sum(
            1 for action in action_queue if action["owner"] == "fyralis_agent"
        ),
        "human_action_count": sum(
            1 for action in action_queue if action["owner"] == "provider_admin"
        ),
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_browser_agent_metadata_only",
    }
    if native_connect:
        run["native_connect"] = dict(native_connect)
        run["action_queue"].insert(
            1 if run["action_queue"] else 0,
            {
                "id": "run_native_preflight",
                "owner": "fyralis_agent",
                "status": "ready",
                "label": "Run native provider preflight in the customer cloud.",
            },
        )
        run["action_queue"].append(
            {
                "id": "run_native_finalize",
                "owner": "fyralis_agent",
                "status": "pending",
                "label": "Finalize the native installation after admin approval.",
            }
        )
        run["current_action"] = run["action_queue"][0] if run["action_queue"] else None
        run["automated_action_count"] = sum(
            1 for action in run["action_queue"] if action["owner"] == "fyralis_agent"
        )
        run["human_action_count"] = sum(
            1 for action in run["action_queue"] if action["owner"] == "provider_admin"
        )
    return run


def _agent_state(
    *,
    source: str,
    auto_state: dict[str, Any] | None,
    installed: bool,
    missing_configuration: list[str],
    human_steps: list[dict[str, Any]],
    install_url: str | None,
    finalize_mode: str | None,
) -> str:
    if auto_state and auto_state.get("state") == "connected":
        return "connected"
    if installed:
        return "connected"
    missing_blocks = _missing_configuration_blocks_agent(source, missing_configuration)
    auto_blocked = bool(auto_state and auto_state.get("state") == "blocked")
    if missing_blocks or (auto_blocked and not _missing_configuration_agent_assisted(source, missing_configuration)):
        return "blocked"
    if auto_state and auto_state.get("state") == "admin_gate":
        return "waiting_for_admin"
    if install_url or finalize_mode == "provider_callback" or human_steps:
        return "waiting_for_admin"
    return "running"


def _missing_configuration_blocks_agent(source: str, missing_configuration: list[str]) -> bool:
    return bool(missing_configuration) and not _missing_configuration_agent_assisted(
        source,
        missing_configuration,
    )


def _missing_configuration_agent_assisted(
    source: str,
    missing_configuration: list[str],
) -> bool:
    normalized_missing = {
        str(name).strip().lower() for name in missing_configuration if str(name).strip()
    }
    return source == "slack" and normalized_missing == {"slack_app_config_token"}


def _action_queue(
    *,
    state: str,
    recipe: dict[str, Any],
    automated_actions: list[str],
    human_steps: list[dict[str, Any]],
    handoff_url: str | None,
    provider_setup_bundle: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if state == "connected":
        return []
    if state == "blocked":
        return [
            {
                "id": "resolve_runtime_configuration",
                "owner": "provider_admin",
                "status": "blocked",
                "label": "Resolve missing customer-cloud configuration.",
            }
        ]

    queue: list[dict[str, Any]] = []
    if handoff_url:
        queue.append(
            {
                "id": "open_provider_settings",
                "owner": "fyralis_agent",
                "status": "ready",
                "label": "Open provider settings in the customer-cloud browser.",
            }
        )
    queue.extend(provider_setup_bundle_actions(provider_setup_bundle))
    queue.extend(
        {
            "id": f"collect_{index}",
            "owner": "fyralis_agent",
            "status": "ready",
            "label": f"Collect {target}.",
        }
        for index, target in enumerate(recipe.get("agent_collects") or [], start=1)
    )
    queue.extend(
        {
            "id": f"generate_{index}",
            "owner": "fyralis_agent",
            "status": "ready",
            "label": f"Generate {target}.",
        }
        for index, target in enumerate(recipe.get("agent_generates") or [], start=1)
    )
    if automated_actions:
        queue.extend(
            {
                "id": f"automate_{index}",
                "owner": "fyralis_agent",
                "status": "ready",
                "label": action,
            }
            for index, action in enumerate(automated_actions, start=1)
        )
    queue.extend(
        {
            "id": str(step.get("id") or f"human_gate_{index}"),
            "owner": "provider_admin",
            "status": "waiting",
            "label": str(step.get("label") or "Approve provider-required step."),
            "reason": str(step.get("reason") or "Provider requires admin action."),
        }
        for index, step in enumerate(human_steps, start=1)
    )
    queue.append(
        {
            "id": "verify_connection_proof",
            "owner": "fyralis_agent",
            "status": "pending",
            "label": "Poll for install and sanitized connection proof.",
        }
    )
    return queue


def _human_gates(
    recipe: dict[str, Any],
    human_steps: list[dict[str, Any]],
    *,
    state: str,
) -> list[dict[str, Any]]:
    if human_steps:
        return [
            {
                "id": str(step.get("id") or f"human_gate_{index}"),
                "label": str(step.get("label") or "Provider admin approval."),
                "reason": str(step.get("reason") or "Provider requires admin action."),
                "status": "waiting" if state == "waiting_for_admin" else "pending",
                "can_agent_complete": bool(step.get("can_agent_complete")),
            }
            for index, step in enumerate(human_steps, start=1)
        ]
    return [
        {
            "id": f"recipe_gate_{index}",
            "label": str(label),
            "reason": "Provider requires an accountable admin-present action.",
            "status": "waiting" if state == "waiting_for_admin" else "pending",
            "can_agent_complete": False,
        }
        for index, label in enumerate(recipe.get("human_gates") or [], start=1)
    ]


def _completion_checks(
    recipe: dict[str, Any],
    *,
    connected: bool,
    observation_count: int,
) -> list[dict[str, Any]]:
    return [
        {
            "name": str(name),
            "status": (
                "passed"
                if connected and (observation_count or "observation" not in str(name))
                else "pending"
            ),
        }
        for name in recipe.get("completion_checks") or []
    ]


def _handoff_url(
    *,
    install_url: str | None,
    provider_console_url: str | None,
) -> str | None:
    if _is_external_url(install_url):
        return install_url
    if _is_external_url(provider_console_url):
        return provider_console_url
    return None


def _handoff_kind(
    *,
    install_url: str | None,
    provider_console_url: str | None,
    handoff_url: str | None,
) -> str:
    if handoff_url and handoff_url == install_url:
        return "provider_install"
    if handoff_url and handoff_url == provider_console_url:
        return "provider_console"
    return "manual_provider_console"


def _is_external_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _normalize_source(source: str) -> str:
    return source.strip().lower().replace("-", "_")


__all__ = [
    "build_source_browser_agent_run",
    "source_browser_agent_run_for_payload",
]
