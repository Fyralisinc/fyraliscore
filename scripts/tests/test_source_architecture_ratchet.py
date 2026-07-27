from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.check_source_architecture_ratchet import (
    RULE_ARBITRARY_INSTALLATION_SELECTION,
    RULE_DUPLICATE_SOURCE_IDS,
    RULE_FABRICATED_LIVE_BINDING,
    RULE_HANDLER_IMPORT_REGISTRATION,
    RULE_LEGACY_PROVIDER_HARNESS,
    RULE_MUTABLE_DISPATCH,
    RULE_PARALLEL_SOURCE_MAP,
    RULE_PROVIDER_TRANSPORT_BYPASS,
    RULE_SHARED_SOURCE_BEHAVIOR_SWITCH,
    RULE_SOURCE_CLIENT_SWITCH,
    RULE_SQL_SOURCE_CHECK,
    SourceArchitectureCheckError,
    apply_baseline,
    iter_synthetic_binding_files,
    iter_production_files,
    load_baseline,
    main,
    scan_repository,
    load_canonical_source_ids,
    load_contract_provider_ids,
    write_baseline,
)


CANONICAL = ("slack", "github", "discord", "gmail", "notion")


def _write(root: Path, relative: str, content: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _scan(root: Path):
    return scan_repository(
        repo_root=root,
        canonical_source_ids=CANONICAL,
    )


def _write_catalog(
    root: Path,
    *,
    provider_ids: tuple[str, ...] = (),
) -> None:
    entries = "\n".join(f'    _source("{source_id}"),' for source_id in CANONICAL)
    providers = "\n".join(
        f'    ProviderDefinition(provider_id="{provider_id}"),'
        for provider_id in provider_ids
    )
    _write(
        root,
        "services/ingest/source_contract/catalog.py",
        (
            f"SOURCE_DEFINITIONS = (\n{entries}\n)\n"
            f"PROVIDER_DEFINITIONS = (\n{providers}\n)\n"
        ),
    )


def test_canonical_ids_are_read_from_source_definition_entries(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path)

    assert load_canonical_source_ids(repo_root=tmp_path) == CANONICAL


def test_provider_ids_are_read_from_provider_definition_entries(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path, provider_ids=("linear", "stripe"))
    catalog = tmp_path / "services/ingest/source_contract/catalog.py"
    catalog.write_text(
        catalog.read_text(encoding="utf-8").replace(
            'ProviderDefinition(provider_id="linear")',
            'ProviderDefinition("linear")',
        ),
        encoding="utf-8",
    )

    assert load_contract_provider_ids(repo_root=tmp_path) == (
        "linear",
        "stripe",
    )


def test_file_discovery_excludes_nonproduction_and_generated_paths(
    tmp_path: Path,
) -> None:
    kept = _write(tmp_path, "services/ingest/live.py", "VALUE = 1\n")
    _write(tmp_path, "services/ingest/tests/test_live.py", "bad = True\n")
    _write(tmp_path, "services/ingest/test_inline.py", "bad = True\n")
    _write(tmp_path, "services/ingest/graphify-out/cache.py", "bad = True\n")
    _write(tmp_path, "services/ingest/synthetic/provider_lab/app.py", "bad = True\n")
    _write(tmp_path, "services/ingest/__pycache__/cached.py", "bad = True\n")
    _write(tmp_path, "docs/example.py", "bad = True\n")

    discovered = tuple(iter_production_files(tmp_path))

    assert discovered == (kept,)


def test_synthetic_binding_discovery_excludes_tests_but_includes_drivers(
    tmp_path: Path,
) -> None:
    kept = _write(
        tmp_path,
        "services/ingest/synthetic/live_generators/slack.py",
        "VALUE = 1\n",
    )
    _write(
        tmp_path,
        "services/ingest/synthetic/tests/test_slack.py",
        "bad = True\n",
    )
    _write(
        tmp_path,
        "services/ingest/synthetic/provider_lab/tests/test_app.py",
        "bad = True\n",
    )

    assert tuple(iter_synthetic_binding_files(tmp_path)) == (kept,)


def test_legacy_provider_harness_paths_are_strict_findings(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/synthetic/mock_servers/slack.py",
        "VALUE = 1\n",
    )
    _write(
        tmp_path,
        "services/ingest/synthetic/spammer/server.py",
        "VALUE = 1\n",
    )
    _write(
        tmp_path,
        "services/ingest/synthetic/validation_runs/run_all_sources.py",
        "VALUE = 1\n",
    )
    _write(
        tmp_path,
        "lib/integrations/endpoints.py",
        ('_SPAMMER_BASE_ENV = "SYNTHETIC_SOURCE_API_BASE"\n' "_SPAMMER_SUBPATH = {}\n"),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_LEGACY_PROVIDER_HARNESS
    ]

    assert {finding.path for finding in findings} == {
        Path("services/ingest/synthetic/mock_servers"),
        Path("services/ingest/synthetic/spammer"),
        Path("services/ingest/synthetic/validation_runs/run_all_sources.py"),
        Path("lib/integrations/endpoints.py"),
    }


def test_provider_lab_is_not_a_legacy_harness(tmp_path: Path) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/synthetic/provider_lab/app.py",
        "VALUE = 1\n",
    )
    _write(
        tmp_path,
        "lib/integrations/endpoints.py",
        'PROVIDER_LAB_URL = "http://127.0.0.1:8787"\n',
    )

    assert not [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_LEGACY_PROVIDER_HARNESS
    ]


def test_fabricated_live_binding_is_a_strict_finding(tmp_path: Path) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/source_contract/legacy_live.py",
        'LIVE_BINDING = "ingest.live.slack.webhook"\n',
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_FABRICATED_LIVE_BINDING
    ]

    assert len(findings) == 1
    assert findings[0].signature == "binding=ingest.live.slack.webhook"


def test_mutable_dispatch_registration_rule_covers_assignment_and_methods(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/wiring.py",
        """
PLANNER_DISPATCH = {"slack": plan_slack}
FETCHER_DISPATCH["github"] = fetch_github
RECONCILER_DISPATCH.update({"discord": reconcile_discord})
""".lstrip(),
    )

    findings = _scan(tmp_path)

    mutable = [
        finding for finding in findings if finding.rule_id == RULE_MUTABLE_DISPATCH
    ]
    assert len(mutable) == 3
    assert {finding.line_number for finding in mutable} == {1, 2, 3}
    assert any("key=github" in finding.signature for finding in mutable)


def test_handler_decorator_is_an_import_side_effect_but_nested_use_is_not(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/ingestion/handlers/example.py",
        """
@register("slack:message")
async def handle(payload, headers):
    return payload

def factory():
    @register("github:webhook")
    async def nested(payload, headers):
        return payload
    return nested
""".lstrip(),
    )

    findings = _scan(tmp_path)

    handler_findings = [
        finding
        for finding in findings
        if finding.rule_id == RULE_HANDLER_IMPORT_REGISTRATION
    ]
    assert len(handler_findings) == 1
    assert handler_findings[0].line_number == 1
    assert "handle" in handler_findings[0].signature


def test_explicit_top_level_handler_registration_call_is_detected(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/ingestion/handlers/example.py",
        """
async def handle(payload, headers):
    return payload

register("slack:message")(handle)
""".lstrip(),
    )

    findings = _scan(tmp_path)

    handler_findings = [
        finding
        for finding in findings
        if finding.rule_id == RULE_HANDLER_IMPORT_REGISTRATION
    ]
    assert len(handler_findings) == 1
    assert handler_findings[0].line_number == 4
    assert handler_findings[0].signature.startswith("call:")


def test_duplicate_source_list_uses_overlap_threshold_and_exempts_catalog(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/legacy.py",
        """
VALID_SOURCES = ("slack", "github", "discord", "gmail")
FINANCE_SOURCES = ("slack", "github")
""".lstrip(),
    )

    findings = _scan(tmp_path)

    duplicate_lists = [
        finding for finding in findings if finding.rule_id == RULE_DUPLICATE_SOURCE_IDS
    ]
    assert len(duplicate_lists) == 1
    assert duplicate_lists[0].path == Path("services/ingest/legacy.py")
    assert "VALID_SOURCES" in duplicate_lists[0].message


@pytest.mark.parametrize(
    "relative_path",
    (
        "scripts/webhook_install.py",
        "services/platform/runtime/source_browser_agent_setup.py",
    ),
)
def test_shared_classifications_cannot_recreate_small_source_lists(
    tmp_path: Path,
    relative_path: str,
) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        relative_path,
        'API_TOKEN_SOURCES = {"slack", "github", "linear", "stripe"}\n',
    )

    duplicate_lists = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_DUPLICATE_SOURCE_IDS
    ]

    assert len(duplicate_lists) == 1
    assert duplicate_lists[0].path == Path(relative_path)
    assert "API_TOKEN_SOURCES" in duplicate_lists[0].signature


def test_parallel_source_map_catches_partial_route_or_policy_registries(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/legacy_routes.py",
        """
SOURCE_CALLBACK_PATHS = {
    "slack": "/slack/callback",
    "github": "/github/callback",
    "discord": "/discord/callback",
    "gmail": "/gmail/callback",
}

def request_payload():
    return {
        "slack": 1,
        "github": 2,
        "discord": 3,
        "gmail": 4,
    }
""".lstrip(),
    )

    findings = _scan(tmp_path)
    maps = [
        finding for finding in findings if finding.rule_id == RULE_PARALLEL_SOURCE_MAP
    ]
    assert len(maps) == 1
    assert "SOURCE_CALLBACK_PATHS" in maps[0].signature


def test_certification_spec_catalog_is_an_intentional_source_map(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/source_certification/catalog.py",
        """
SPECS = {
    "slack": spec_slack,
    "github": spec_github,
    "discord": spec_discord,
    "gmail": spec_gmail,
}
""".lstrip(),
    )

    assert not [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_PARALLEL_SOURCE_MAP
    ]


def test_sql_source_check_rule_ignores_comments_and_other_provider_columns(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "db/migrations/0194_sources.sql",
        """
-- CHECK (source IN ('slack', 'github'))
ALTER TABLE runs
  ADD CONSTRAINT runs_source_check
  CHECK (source IN ('slack', 'github', 'discord'));
CREATE TABLE cloud_jobs (
  provider TEXT CHECK (provider IN ('slack', 'github'))
);
ALTER TABLE registry
  ADD CONSTRAINT registry_source_id_check
  CHECK (source_id IN ('slack', 'github'));
""".lstrip(),
    )

    findings = _scan(tmp_path)

    checks = [
        finding for finding in findings if finding.rule_id == RULE_SQL_SOURCE_CHECK
    ]
    assert len(checks) == 2
    assert {finding.line_number for finding in checks} == {3, 9}
    assert {finding.signature.split(":", 1)[0] for finding in checks} == {
        "constraint=runs_source_check",
        "constraint=registry_source_id_check",
    }


def test_sql_source_checks_before_catalog_cutover_are_superseded(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "db/migrations/0193_contract_driven_source_catalog.sql",
        """
ALTER TABLE runs
  ADD CONSTRAINT runs_source_check
  CHECK (source IN ('slack', 'github', 'discord'));
""".lstrip(),
    )

    assert not [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_SQL_SOURCE_CHECK
    ]


def test_source_client_switch_rule_covers_if_and_match_without_false_positive(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/client_factory.py",
        """
async def build(source):
    if source == "github":
        return await clients.build_github_client()
    if source == "notion":
        return "not a client"
    match source:
        case "slack" | "discord":
            return ProviderClient()

def llm(provider):
    if provider == "openai":
        return OpenAIClient()
""".lstrip(),
    )

    findings = _scan(tmp_path)

    switches = [
        finding for finding in findings if finding.rule_id == RULE_SOURCE_CLIENT_SWITCH
    ]
    assert len(switches) == 2
    assert {finding.line_number for finding in switches} == {2, 7}
    assert all("notion" not in finding.signature for finding in switches)


def test_shared_source_behavior_switch_covers_control_flow_without_client_calls(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/app/webhooks/router.py",
        """
GITHUB_PROVIDER = "github"
PUSH_PROVIDERS = {"slack", "discord"}

async def dispatch(source, provider, normalized_source, rows):
    if provider == GITHUB_PROVIDER:
        return await run_github_hook()
    if source not in PUSH_PROVIDERS:
        return await run_pull_hook()
    match normalized_source:
        case "gmail" | "notion":
            return await run_hydration_hook()
    selected = source == "github"
    filtered = [row for row in rows if row.source == "github"]
    return run_slack() if source != "slack" else run_default()
""".lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_SHARED_SOURCE_BEHAVIOR_SWITCH
    ]

    assert len(findings) == 4
    assert {finding.line_number for finding in findings} == {5, 7, 10, 14}
    assert {
        source_id
        for finding in findings
        for source_id in finding.signature.rsplit("sources=", 1)[1].split(",")
    } == set(CANONICAL)
    assert all("selected =" not in finding.signature for finding in findings)
    assert all("filtered =" not in finding.signature for finding in findings)


def test_shared_behavior_switch_includes_ingress_only_provider_identities(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path, provider_ids=("linear", "stripe"))
    _write(
        tmp_path,
        "services/app/webhooks/router.py",
        """
def dispatch(provider):
    if provider == "linear":
        return run_linear()
    return run_default()
""".lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_SHARED_SOURCE_BEHAVIOR_SWITCH
    ]

    assert len(findings) == 1
    assert findings[0].signature.endswith("sources=linear")


def test_shared_source_behavior_rule_is_scoped_to_shared_orchestration(
    tmp_path: Path,
) -> None:
    provider_owned = _write(
        tmp_path,
        "services/ingest/integrations/github/lifecycle.py",
        """
def dispatch(source):
    if source == "github":
        return handle_lifecycle()
""".lstrip(),
    )
    ordinary_gateway = _write(
        tmp_path,
        "services/app/gateway/reporting.py",
        """
def label(source):
    if source == "github":
        return "GitHub"
""".lstrip(),
    )
    assert provider_owned.is_file()
    assert ordinary_gateway.is_file()

    assert not [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_SHARED_SOURCE_BEHAVIOR_SWITCH
    ]


@pytest.mark.parametrize(
    "relative_path",
    (
        "scripts/manage_dedicated_source_installations.py",
        "scripts/webhook_install.py",
        "services/app/gateway/finance_router.py",
        "services/app/gateway/route_mounts.py",
        "services/app/gateway/byoc_onboarding_router.py",
        "services/app/webhooks/secrets.py",
        "services/app/webhooks/signatures/__init__.py",
        "services/app/webhooks/router.py",
        "services/app/webhooks/tenant_resolver.py",
        "services/ingest/ingestion/reconcilers/__init__.py",
        "services/ingest/integrations/oauth_refresh.py",
        "services/ingest/integrations/router.py",
        "services/ingest/synthetic/validation_runs/composition.py",
        "services/ingest/synthetic/validation_runs/preflight.py",
        "services/platform/runtime/source_browser_agent_recipes.py",
        "services/platform/runtime/source_browser_agent_runner.py",
        "services/platform/runtime/source_browser_agent_workflow.py",
    ),
)
def test_shared_source_behavior_rule_protects_each_shared_router(
    tmp_path: Path,
    relative_path: str,
) -> None:
    _write(
        tmp_path,
        relative_path,
        """
def dispatch(source):
    if source == "github":
        return run_github()
""".lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_SHARED_SOURCE_BEHAVIOR_SWITCH
    ]

    assert len(findings) == 1
    assert findings[0].path == Path(relative_path)


def test_provider_http_call_must_be_a_transport_callback(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/integrations/github/client.py",
        """
import httpx

async def unsafe() -> httpx.Response:
    client = httpx.AsyncClient()
    return await client.get("https://api.github.com/user")

async def safe() -> httpx.Response:
    client = httpx.AsyncClient()
    binding = ProviderRequestBinding()

    async def _once() -> httpx.Response:
        return await client.get("https://api.github.com/user")

    return await binding.execute("users.get", _once)
""".lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_PROVIDER_TRANSPORT_BYPASS
    ]

    assert len(findings) == 1
    assert findings[0].line_number == 5
    assert "unsafe" in findings[0].signature


def test_provider_http_rule_tracks_aliases_injected_clients_and_lambda_calls(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/integrations/slack/oauth.py",
        """
from httpx import AsyncClient as HttpClient

async def unsafe(http: HttpClient) -> None:
    await http.post("https://slack.com/api/oauth.v2.access")

async def safe(http: HttpClient) -> None:
    binding = ProviderRequestBinding()
    await binding.execute(
        "oauth.token.exchange",
        lambda: http.post("https://slack.com/api/oauth.v2.access"),
    )
""".lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_PROVIDER_TRANSPORT_BYPASS
    ]

    assert len(findings) == 1
    assert findings[0].line_number == 4
    assert "http.post" in findings[0].message


def test_provider_callback_cannot_have_an_unmetered_direct_fallback(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/integrations/aws/credentials.py",
        """
async def assume_role(sts, binding: ProviderRequestBinding | None = None):
    async def _once():
        return await sts.assume_role(RoleArn="arn:aws:iam::1:role/test")

    if binding is not None:
        return await binding.execute("sts.assume_role", _once)
    return await _once()
""".lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_PROVIDER_TRANSPORT_BYPASS
    ]

    assert len(findings) == 1
    assert findings[0].line_number == 3
    assert "sts.assume_role" in findings[0].signature


def test_provider_http_rule_follows_a_binding_owned_execute_helper(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/integrations/github/client.py",
        """
import httpx

class GithubClient:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http
        self._binding = ProviderRequestBinding()

    async def _execute(self, operation, call):
        return await self._binding.execute(operation, call)

    async def get_user(self):
        async def _once():
            return await self._http.get("https://api.github.com/user")

        return await self._execute("users.get", _once)
""".lstrip(),
    )

    assert not [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_PROVIDER_TRANSPORT_BYPASS
    ]


def test_provider_http_rule_does_not_mistake_domain_client_for_raw_http(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/ingest/integrations/gmail/client.py",
        """
class GmailClient:
    def __init__(self, http: GoogleHttpClient) -> None:
        self._http = http

    async def list_messages(self) -> dict:
        return await self._http.request(
            "GET",
            "https://gmail.googleapis.com/gmail/v1/users/me/messages",
        )
""".lstrip(),
    )

    assert not [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_PROVIDER_TRANSPORT_BYPASS
    ]


def test_provider_http_rule_exempts_lab_tests_and_transport_implementation(
    tmp_path: Path,
) -> None:
    unsafe = """
import httpx

async def request() -> None:
    await httpx.AsyncClient().get("http://provider.invalid")
""".lstrip()
    _write(
        tmp_path,
        "services/ingest/synthetic/provider_lab/app.py",
        unsafe,
    )
    _write(
        tmp_path,
        "services/ingest/integrations/github/tests/test_client.py",
        unsafe,
    )
    _write(
        tmp_path,
        "services/ingest/integrations/provider_transport.py",
        unsafe,
    )

    assert not [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_PROVIDER_TRANSPORT_BYPASS
    ]


def test_arbitrary_installation_selection_requires_exact_row_or_collection(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "services/app/status.py",
        '''
async def latest(pool, tenant_id):
    return await pool.fetchrow(
        """
        SELECT id FROM figma_installations
         WHERE tenant_id = $1
         ORDER BY created_at DESC
         LIMIT 1
        """,
        tenant_id,
    )

async def exact(pool, tenant_id, installation_id):
    return await pool.fetchrow(
        """
        SELECT id FROM figma_installations
         WHERE tenant_id = $1 AND id = $2
         LIMIT 1
        """,
        tenant_id,
        installation_id,
    )

async def collection(pool, tenant_id):
    return await pool.fetch(
        "SELECT id FROM figma_installations WHERE tenant_id = $1",
        tenant_id,
    )

async def existence(pool):
    return await pool.fetchval(
        "SELECT 1 FROM provider_installations LIMIT 1"
    )
'''.lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_ARBITRARY_INSTALLATION_SELECTION
    ]

    assert len(findings) == 1
    assert findings[0].line_number == 3
    assert "figma_installations" in findings[0].signature


def test_arbitrary_installation_selection_is_enforced_in_synthetic_drivers(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/synthetic/live_generators/figma.py",
        """
async def resolve(pool, tenant_id):
    return await pool.fetchrow(
        "SELECT id FROM figma_installations "
        "WHERE tenant_id = $1 ORDER BY created_at LIMIT 1",
        tenant_id,
    )
""".lstrip(),
    )

    findings = [
        finding
        for finding in _scan(tmp_path)
        if finding.rule_id == RULE_ARBITRARY_INSTALLATION_SELECTION
    ]

    assert len(findings) == 1
    assert findings[0].path == Path(
        "services/ingest/synthetic/live_generators/figma.py"
    )


def test_baseline_allows_only_exact_existing_findings(tmp_path: Path) -> None:
    source = _write(
        tmp_path,
        "services/ingest/wiring.py",
        'FETCHER_DISPATCH["github"] = fetch_github\n',
    )
    initial = _scan(tmp_path)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, initial)

    covered = apply_baseline(initial, load_baseline(baseline_path))
    assert covered.new_findings == ()
    assert len(covered.baselined_findings) == 1

    source.write_text(
        (
            'FETCHER_DISPATCH["github"] = fetch_github\n'
            'FETCHER_DISPATCH["slack"] = fetch_slack\n'
        ),
        encoding="utf-8",
    )
    changed = apply_baseline(_scan(tmp_path), load_baseline(baseline_path))

    assert len(changed.new_findings) == 1
    assert "key=slack" in changed.new_findings[0].signature


def test_resolved_baseline_entries_do_not_fail_ratchet(tmp_path: Path) -> None:
    source = _write(
        tmp_path,
        "services/ingest/wiring.py",
        'FETCHER_DISPATCH["github"] = fetch_github\n',
    )
    initial = _scan(tmp_path)
    source.write_text("FETCHERS = {}\n", encoding="utf-8")

    result = apply_baseline(_scan(tmp_path), [initial[0].baseline_entry])

    assert result.new_findings == ()
    assert result.baselined_findings == ()
    assert result.resolved_entries == (initial[0].baseline_entry,)


def test_cli_baseline_mode_passes_and_strict_mode_fails(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_catalog(tmp_path)
    _write(
        tmp_path,
        "services/ingest/wiring.py",
        'FETCHER_DISPATCH["github"] = fetch_github\n',
    )
    baseline_path = tmp_path / "source-baseline.json"

    assert (
        main(
            [
                "--repo-root",
                str(tmp_path),
                "--baseline",
                str(baseline_path),
                "--write-baseline",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "--repo-root",
                str(tmp_path),
                "--baseline",
                str(baseline_path),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "--repo-root",
                str(tmp_path),
                "--baseline",
                str(baseline_path),
                "--no-baseline",
            ]
        )
        == 1
    )
    assert "strict/no-baseline violations" in capsys.readouterr().err


def test_invalid_baseline_is_a_configuration_error(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "entries": [
                    {
                        "rule_id": "NOT_A_RULE",
                        "path": "services/ingest/x.py",
                        "signature": "bad",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SourceArchitectureCheckError, match="unknown rule_id"):
        load_baseline(baseline)
