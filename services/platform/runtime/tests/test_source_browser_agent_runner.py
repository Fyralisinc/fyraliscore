from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from services.platform.runtime.source_browser_agent_runner import (
    SourceBrowserAgentRunnerInputs,
    _autofilled_native_payload_from_template,
    _customer_cloud_ref_metadata,
    _dom_apply_artifact,
    _dom_collect_text,
    _dom_fill_values,
    _dom_generate_refs,
    _dom_set_config_values,
    _generated_provider_setup_artifacts,
    _materialize_native_payload_template,
    run_source_browser_agent,
)
from services.platform.runtime.source_browser_agent_recipes import (
    browser_agent_recipe_for_source,
)
from services.platform.runtime.source_browser_agent_setup import (
    build_source_provider_setup_bundle,
)


class _FakeLocator:
    def __init__(self, page: "_FakePage", *, selector: str | None = None, text: Any = None):
        self.page = page
        self.selector = selector
        self.text = text

    def first(self) -> "_FakeLocator":
        return self

    async def fill(self, value: str, *, timeout: int) -> None:
        if self.selector not in self.page.fillable_selectors:
            raise RuntimeError("not fillable")
        self.page.fills.append((self.selector or "", value))

    async def click(self, *, timeout: int) -> None:
        if self.text is not None:
            if not any(self.text.search(candidate) for candidate in self.page.clickable_texts):
                raise RuntimeError("text not clickable")
            self.page.clicks.append(("text", str(self.text.pattern)))
            return
        if self.selector not in self.page.clickable_selectors:
            raise RuntimeError("selector not clickable")
        self.page.clicks.append(("selector", self.selector or ""))

    async def set_input_files(self, path: str, *, timeout: int) -> None:
        if self.selector not in self.page.file_selectors:
            raise RuntimeError("file input not present")
        self.page.uploads.append((self.selector or "", path))

    async def all_text_contents(self, **kwargs: Any) -> list[str]:
        if self.selector not in self.page.text_contents:
            raise RuntimeError("text target not present")
        return self.page.text_contents[self.selector or ""]


class _FakePage:
    def __init__(self) -> None:
        self.fillable_selectors: set[str] = set()
        self.clickable_selectors: set[str] = set()
        self.clickable_texts: set[str] = set()
        self.file_selectors: set[str] = set()
        self.text_contents: dict[str, list[str]] = {}
        self.fills: list[tuple[str, str]] = []
        self.clicks: list[tuple[str, str]] = []
        self.uploads: list[tuple[str, str]] = []

    def locator(self, selector: str) -> _FakeLocator:
        return _FakeLocator(self, selector=selector)

    def get_by_text(self, text: Any) -> _FakeLocator:
        return _FakeLocator(self, text=text)


@pytest.mark.asyncio
async def test_browser_dom_fill_submits_provider_form() -> None:
    page = _FakePage()
    page.fillable_selectors.add("input[type=url]")
    page.clickable_texts.add("Save URLs")

    result = await _dom_fill_values(
        page,
        {
            "id": "configure_redirect",
            "action": "set_url",
            "selectors": ["input[type=url]"],
            "text_targets": ["Redirect URLs", "OAuth", "Save URLs"],
        },
        ["https://fyralis.example/callback"],
        1000,
    )

    assert result["status"] == "completed"
    assert "submitted the provider form" in result["detail"]
    assert page.fills == [("input[type=url]", "https://fyralis.example/callback")]
    assert page.clicks == [("text", "Save\\ URLs")]


@pytest.mark.asyncio
async def test_browser_dom_apply_artifact_submits_provider_form(tmp_path: Path) -> None:
    output_dir = tmp_path / "provider-setup"
    output_dir.mkdir()
    artifact = output_dir / "manifest.yaml"
    artifact.write_text("display_information:\n  name: Fyralis\n", encoding="utf-8")
    page = _FakePage()
    page.fillable_selectors.add("textarea")
    page.clickable_texts.add("Create")

    result = await _dom_apply_artifact(
        page,
        {
            "id": "create_provider_app",
            "action": "paste_or_upload_manifest",
            "artifacts": ["manifest.yaml"],
            "selectors": ["textarea", "button"],
            "text_targets": ["Create New App", "From an app manifest", "Next", "Create"],
        },
        output_dir,
        1000,
    )

    assert result["status"] == "completed"
    assert "submitted the provider form" in result["detail"]
    assert page.fills == [("textarea", "display_information:\n  name: Fyralis\n")]
    assert page.clicks == [("text", "Create")]


@pytest.mark.asyncio
async def test_browser_dom_collection_persists_only_targeted_non_secret_fields(
    tmp_path: Path,
) -> None:
    page = _FakePage()
    page.text_contents["code"] = [
        "Workspace ID: T12345\nClient secret: should-not-survive\nAPI token: nooo",
        "Admin email: admin@example.test",
    ]

    result = await _dom_collect_text(
        page,
        {
            "id": "collect_non_secret_configuration",
            "action": "collect_text",
            "fields": ["workspace id", "client secret", "admin email"],
            "selectors": ["code"],
        },
        tmp_path,
        1000,
    )
    payload = json.loads(
        (tmp_path / "browser-dom-collections.json").read_text(encoding="utf-8")
    )

    assert result["status"] == "completed"
    assert payload["collection_policy"] == (
        "targeted_non_secret_field_extraction_only"
    )
    assert payload["snippets"] == [
        "workspace id: T12345",
        "admin email: admin@example.test",
    ]
    assert "should-not-survive" not in json.dumps(payload)
    assert "nooo" not in json.dumps(payload)


def test_generated_artifact_index_excludes_browser_storage_state(
    tmp_path: Path,
) -> None:
    run_path = tmp_path / "source-run.json"
    setup_dir = tmp_path / "browser-agent-provider-setup"
    setup_dir.mkdir()
    (setup_dir / "browser-dom-plan.json").write_text("{}\n", encoding="utf-8")
    (setup_dir / "browser-storage-state.local.json").write_text(
        '{"cookies":[{"value":"session-cookie"}]}\n',
        encoding="utf-8",
    )

    artifacts = _generated_provider_setup_artifacts(run_path)

    assert "browser-dom-plan" in artifacts
    assert "browser-storage-state.local" not in artifacts
    assert "session-cookie" not in json.dumps(artifacts)


def test_customer_cloud_ref_metadata_redacts_generated_secret_name() -> None:
    metadata = _customer_cloud_ref_metadata("aws", "external id")

    assert metadata["label"] == "external id"
    assert metadata["secret_name_hint"] == "/fyralis/sources/aws/[generated]"
    assert "secret_name" not in metadata
    assert len(metadata["secret_name_sha256"]) == 64
    assert metadata["raw_secret_value_included"] is False


def test_dom_ref_metadata_redacts_mapped_local_ref(tmp_path: Path) -> None:
    generated_values: dict[str, str] = {}
    generated_refs: dict[str, str] = {}

    result = _dom_generate_refs(
        {"id": "prepare_refs", "refs": ["verify token ref"]},
        tmp_path,
        source="whatsapp",
        generated_secret_values=generated_values,
        generated_secret_refs=generated_refs,
    )
    payload_text = (tmp_path / "browser-dom-generated-refs.json").read_text(
        encoding="utf-8"
    )
    payload = json.loads(payload_text)

    assert result["status"] == "completed"
    assert "local_ref" not in payload["refs"][0]
    assert payload["refs"][0]["local_ref_hint"] == (
        "customer-cloud://fyralis/sources/whatsapp/[generated]"
    )
    assert len(payload["refs"][0]["local_ref_sha256"]) == 64
    assert generated_refs["verify_token"] not in payload_text


def test_figma_oauth_bundle_never_generates_pat_or_webhook_refs() -> None:
    bundle = build_source_provider_setup_bundle(
        source="figma",
        recipe=browser_agent_recipe_for_source("figma"),
        provider_console_url="https://www.figma.com/developers/apps",
        oauth_redirect_url=(
            "https://fyralis.example/integrations/figma/oauth/callback"
        ),
        native_connect={
            "kind": "figma_oauth_file_scoped_connect",
            "start_path": "/integrations/figma/oauth/start",
            "status_path": "/integrations/figma/connect/status",
            "retry_path": "/integrations/figma/connect/retry",
            "disconnect_path": "/integrations/figma/connect",
            "payload_fields": ["file_urls", "return_path"],
        },
    )

    rendered = json.dumps(bundle)
    setup = bundle["artifacts"][0]["json"]
    assert bundle["kind"] == "figma_deployment_oauth_app_setup"
    assert setup["app"]["mode"] == "private"
    assert setup["end_user_connection"]["status_path"] == (
        "/integrations/figma/connect/status"
    )
    assert "api_token" not in rendered
    assert "webhook_secret" not in rendered


@pytest.mark.asyncio
async def test_browser_dom_config_values_generate_secret_in_memory() -> None:
    page = _FakePage()
    page.fillable_selectors.update({"input[type=url]", "input[name=verify_token]"})
    page.clickable_texts.add("Verify and save")
    generated_values: dict[str, str] = {}
    generated_refs: dict[str, str] = {}

    result = await _dom_set_config_values(
        page=page,
        step={
            "id": "configure_meta_webhook",
            "action": "set_config_values",
            "fields": [
                {
                    "name": "callback_url",
                    "value": "https://fyralis.example/integrations/whatsapp/webhook",
                    "selectors": ["input[type=url]"],
                },
                {
                    "name": "verify_token",
                    "generated_secret_field": "verify_token",
                    "selectors": ["input[name=verify_token]"],
                },
            ],
            "text_targets": ["Verify and save"],
        },
        source="whatsapp",
        timeout_ms=1000,
        generated_secret_values=generated_values,
        generated_secret_refs=generated_refs,
    )

    assert result["status"] == "completed"
    assert result["raw_secret_values_included"] is False
    assert "provider configuration field" in result["detail"]
    assert "verify_token" in generated_values
    assert generated_refs["verify_token"] == (
        "customer-cloud://fyralis/sources/whatsapp/verify-token"
    )
    assert page.fills == [
        ("input[type=url]", "https://fyralis.example/integrations/whatsapp/webhook"),
        ("input[name=verify_token]", generated_values["verify_token"]),
    ]
    assert generated_values["verify_token"] not in result["detail"]


def test_native_payload_template_uses_redacted_generated_secret_ref(
    tmp_path: Path,
) -> None:
    run_path = tmp_path / "whatsapp-run.json"
    run_path.write_text(
        json.dumps(
            {
                "schema_version": "fyralis.byoc.source.browser_agent_run.v1",
                "source": "whatsapp",
                "agent_generates": ["verify token ref", "app secret ref"],
                "native_connect": {
                    "kind": "whatsapp_native_connect",
                    "preflight_path": "/integrations/whatsapp/connect/preflight",
                    "finalize_path": "/integrations/whatsapp/connect/finalize",
                    "payload_fields": [
                        "phone_number_id",
                        "verify_token",
                        "app_secret",
                    ],
                },
                "action_queue": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    setup_dir = tmp_path / "browser-agent-provider-setup"
    setup_dir.mkdir()
    (setup_dir / "browser-dom-collections.json").write_text(
        json.dumps(
            {
                "schema_version": "fyralis.byoc.source.browser_dom_collection.v1",
                "fields": ["phone number id"],
                "snippets": ["Phone number ID: 15551234567"],
                "raw_secret_values_included": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    generated_values = {"verify_token": "fyralis-test-verify-token"}
    generated_refs = {
        "verify_token": "customer-cloud://fyralis/sources/whatsapp/verify-token"
    }

    template_path = _materialize_native_payload_template(
        run_path=run_path,
        native_connect={
            "kind": "whatsapp_native_connect",
            "preflight_path": "/integrations/whatsapp/connect/preflight",
            "finalize_path": "/integrations/whatsapp/connect/finalize",
            "payload_fields": [
                "phone_number_id",
                "verify_token",
                "app_secret",
            ],
        },
        generated_secret_values=generated_values,
        generated_secret_refs=generated_refs,
    )
    template_text = template_path.read_text(encoding="utf-8")
    template = json.loads(template_text)

    assert "fyralis-test-verify-token" not in template_text
    assert template["payload"]["verify_token"]["local_ref_hint"] == (
        "customer-cloud://fyralis/sources/whatsapp/[generated]"
    )
    assert len(template["payload"]["verify_token"]["local_ref_sha256"]) == 64
    assert template["payload"]["verify_token"]["raw_secret_value_included"] is False
    assert "local_ref" not in template["payload"]["verify_token"]
    assert template["field_status"]["verify_token"] == (
        "auto_generated_secret_in_browser_session"
    )
    assert template["field_status"]["app_secret"] == "customer_admin_secret_required"
    assert _autofilled_native_payload_from_template(
        template_path,
        generated_secret_values=generated_values,
    ) is None


def test_aws_native_payload_template_defaults_to_assume_role(
    tmp_path: Path,
) -> None:
    run_path = tmp_path / "aws-run.json"
    run_path.write_text(
        json.dumps(
            {
                "schema_version": "fyralis.byoc.source.browser_agent_run.v1",
                "source": "aws",
                "agent_generates": ["external id", "role trust contract"],
                "native_connect": {
                    "kind": "aws_iam_native_connect",
                    "preflight_path": "/integrations/aws/connect/preflight",
                    "finalize_path": "/integrations/aws/connect/finalize",
                    "payload_fields": [
                        "account_id",
                        "region",
                        "credential_kind",
                        "role_arn",
                        "external_id",
                        "backfill_window_days",
                    ],
                },
                "deployment_context": {"aws_region": "us-west-2"},
                "action_queue": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    setup_dir = tmp_path / "browser-agent-provider-setup"
    setup_dir.mkdir()
    (setup_dir / "browser-dom-collections.json").write_text(
        json.dumps(
            {
                "schema_version": "fyralis.byoc.source.browser_dom_collection.v1",
                "fields": ["role arn"],
                "snippets": [
                    "Role ARN: arn:aws:iam::123456789012:role/fyralis-source-readonly",
                ],
                "raw_secret_values_included": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    generated_values = {"external_id": "fyralis-test-external-id"}
    generated_refs = {
        "external_id": "customer-cloud://fyralis/sources/aws/external-id"
    }

    template_path = _materialize_native_payload_template(
        run_path=run_path,
        native_connect={
            "kind": "aws_iam_native_connect",
            "preflight_path": "/integrations/aws/connect/preflight",
            "finalize_path": "/integrations/aws/connect/finalize",
            "payload_fields": [
                "account_id",
                "region",
                "credential_kind",
                "role_arn",
                "external_id",
                "backfill_window_days",
            ],
        },
        generated_secret_values=generated_values,
        generated_secret_refs=generated_refs,
    )
    template_text = template_path.read_text(encoding="utf-8")
    template = json.loads(template_text)

    assert "fyralis-test-external-id" not in template_text
    assert template["payload"]["account_id"] == "123456789012"
    assert template["payload"]["region"] == "us-west-2"
    assert template["payload"]["credential_kind"] == "assume_role"
    assert template["payload"]["role_arn"] == (
        "arn:aws:iam::123456789012:role/fyralis-source-readonly"
    )
    assert template["payload"]["external_id"]["local_ref_hint"] == (
        "customer-cloud://fyralis/sources/aws/[generated]"
    )
    assert template["field_status"]["external_id"] == (
        "auto_generated_secret_in_browser_session"
    )


@pytest.mark.asyncio
async def test_native_execution_without_payload_generates_local_template(
    tmp_path: Path,
) -> None:
    run_path = tmp_path / "slack-run.json"
    run_path.write_text(
        """
{
  "schema_version": "fyralis.byoc.source.browser_agent_run.v1",
  "source": "slack",
  "state": "waiting_for_admin",
  "oauth_redirect_url": "https://fyralis.example/integrations/slack/callback",
  "events_request_url": "https://fyralis.example/webhooks/slack/events",
  "native_connect": {
    "kind": "oauth_callback_native_connect",
    "preflight_path": "/integrations/slack/connect/preflight",
    "finalize_path": "/integrations/slack/connect/finalize",
    "payload_fields": [
      "workspace_id",
      "approved_channel_ids",
      "oauth_redirect_url",
      "events_request_url",
      "installation_id"
    ]
  },
  "action_queue": [
    {
      "id": "run_native_finalize",
      "owner": "fyralis_agent",
      "status": "pending"
    }
  ]
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    setup_dir = tmp_path / "browser-agent-provider-setup"
    setup_dir.mkdir()
    (setup_dir / "browser-dom-collections.json").write_text(
        """
{
  "schema_version": "fyralis.byoc.source.browser_dom_collection.v1",
  "fields": ["workspace id"],
  "snippets": ["Workspace ID: T12345"],
  "raw_secret_values_included": false
}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    receipt = await run_source_browser_agent(
        SourceBrowserAgentRunnerInputs(
            run_path=run_path,
            gateway_api_base="https://gateway.example",
            execute_native=True,
            admin_approved=True,
        )
    )
    payload = receipt.as_json()
    template_path = setup_dir / "native-payload-template.json"
    template = json.loads(template_path.read_text(encoding="utf-8"))

    assert payload["status"] == "blocked"
    assert payload["action_results"][0]["endpoint"] == str(template_path)
    assert "native-payload-template" in payload["generated_artifacts"]
    assert template["raw_secret_values_included"] is False
    assert template["payload"]["workspace_id"] == "T12345"
    assert template["payload"]["oauth_redirect_url"] == (
        "https://fyralis.example/integrations/slack/callback"
    )
    assert template["payload"]["events_request_url"] == (
        "https://fyralis.example/webhooks/slack/events"
    )
    assert template["payload"]["approved_channel_ids"] == []
    assert template["field_status"]["installation_id"] == (
        "customer_admin_value_required"
    )


@pytest.mark.asyncio
async def test_native_execution_uses_complete_autofilled_template(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_path = tmp_path / "drive-run.json"
    run_path.write_text(
        """
{
  "schema_version": "fyralis.byoc.source.browser_agent_run.v1",
  "source": "google_drive",
  "state": "waiting_for_admin",
  "native_connect": {
    "kind": "google_workspace_dwd",
    "preflight_path": "/integrations/google_drive/connect/preflight",
    "finalize_path": "/integrations/google_drive/connect/finalize",
    "preflight_payload_fields": [
      "workspace_domain",
      "admin_email",
      "scope"
    ],
    "payload_fields": [
      "workspace_domain",
      "admin_email",
      "scope",
      "inclusion_spec",
      "include_shared_drives"
    ],
    "scope_aliases": ["drive.readonly"]
  },
  "action_queue": [
    {
      "id": "run_native_preflight",
      "owner": "fyralis_agent",
      "status": "ready"
    }
  ]
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    setup_dir = tmp_path / "browser-agent-provider-setup"
    setup_dir.mkdir()
    (setup_dir / "browser-dom-collections.json").write_text(
        """
{
  "schema_version": "fyralis.byoc.source.browser_dom_collection.v1",
  "fields": ["workspace domain", "admin email"],
  "snippets": [
    "Workspace domain: acme.example",
    "Admin email: admin@acme.example"
  ],
  "raw_secret_values_included": false
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    posted: dict[str, Any] = {}

    class _Response:
        status_code = 200

        def json(self) -> dict[str, Any]:
            return {"ok": True, "message": "Preflight passed."}

    class _Client:
        def __init__(self, *, timeout: float):
            self.timeout = timeout

        async def __aenter__(self) -> "_Client":
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            return False

        async def post(
            self,
            endpoint: str,
            *,
            json: dict[str, Any],
            headers: dict[str, str],
        ) -> _Response:
            posted["endpoint"] = endpoint
            posted["json"] = json
            posted["headers"] = headers
            return _Response()

    monkeypatch.setattr(
        "services.platform.runtime.source_browser_agent_runner.httpx.AsyncClient",
        _Client,
    )

    receipt = await run_source_browser_agent(
        SourceBrowserAgentRunnerInputs(
            run_path=run_path,
            gateway_api_base="https://gateway.example",
            execute_native=True,
        )
    )
    payload = receipt.as_json()
    template = json.loads(
        (setup_dir / "native-payload-template.json").read_text(encoding="utf-8")
    )

    assert payload["status"] == "running"
    assert payload["action_results"][0]["status"] == "completed"
    assert posted["endpoint"] == (
        "https://gateway.example/integrations/google_drive/connect/preflight"
    )
    assert posted["json"] == {
        "workspace_domain": "acme.example",
        "admin_email": "admin@acme.example",
        "scope": "drive.readonly",
    }
    assert template["field_status"] == {
        "admin_email": "auto_from_provider_page_collection",
        "scope": "auto_from_native_contract",
        "workspace_domain": "auto_from_provider_page_collection",
    }
