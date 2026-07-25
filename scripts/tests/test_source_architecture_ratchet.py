from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.check_source_architecture_ratchet import (
    RULE_DUPLICATE_SOURCE_IDS,
    RULE_HANDLER_IMPORT_REGISTRATION,
    RULE_LEGACY_PROVIDER_HARNESS,
    RULE_MUTABLE_DISPATCH,
    RULE_PARALLEL_SOURCE_MAP,
    RULE_SOURCE_CLIENT_SWITCH,
    RULE_SQL_SOURCE_CHECK,
    SourceArchitectureCheckError,
    apply_baseline,
    iter_production_files,
    load_baseline,
    main,
    scan_repository,
    load_canonical_source_ids,
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


def _write_catalog(root: Path) -> None:
    entries = "\n".join(
        f'    _source("{source_id}"),' for source_id in CANONICAL
    )
    _write(
        root,
        "services/ingest/source_contract/catalog.py",
        f"SOURCE_DEFINITIONS = (\n{entries}\n)\n",
    )


def test_canonical_ids_are_read_from_source_definition_entries(
    tmp_path: Path,
) -> None:
    _write_catalog(tmp_path)

    assert load_canonical_source_ids(repo_root=tmp_path) == CANONICAL


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
        (
            '_SPAMMER_BASE_ENV = "SYNTHETIC_SOURCE_API_BASE"\n'
            '_SPAMMER_SUBPATH = {}\n'
        ),
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
        finding
        for finding in findings
        if finding.rule_id == RULE_PARALLEL_SOURCE_MAP
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
