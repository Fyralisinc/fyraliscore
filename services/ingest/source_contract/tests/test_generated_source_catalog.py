from __future__ import annotations

import ast
import json
from pathlib import Path

from scripts.generate_source_catalog_artifacts import (
    SCHEMA_VERSION,
    TEST_SEED_SCHEMA_VERSION,
    build_source_catalog_artifact,
    build_test_source_catalog_artifact,
    main,
    render_source_catalog_artifact,
    render_test_source_catalog_artifact,
)
from services.ingest.source_contract import (
    CANONICAL_PROVIDER_IDS,
    CANONICAL_SOURCE_IDS,
    SOURCE_DEFINITIONS,
)


REPO_ROOT = Path(__file__).resolve().parents[4]
GENERATED_ARTIFACT = (
    REPO_ROOT / "ui/features/onboarding/data/source-catalog.generated.json"
)
GENERATED_TEST_SEED_ARTIFACT = (
    REPO_ROOT / "lib/shared/testing/source_catalog.generated.json"
)
ONBOARDING_MOCK_DATA = REPO_ROOT / "ui/features/onboarding/data/mock-data.ts"
CATALOG_MODULE = REPO_ROOT / "services/ingest/source_contract/catalog.py"


def _assignment_value(tree: ast.Module, name: str) -> ast.AST:
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == name and node.value is not None:
                return node.value
    raise AssertionError(f"missing annotated assignment for {name}")


def _assert_id_tuple_is_derived_from(
    tree: ast.Module,
    *,
    assignment_name: str,
    definitions_name: str,
) -> None:
    value = _assignment_value(tree, assignment_name)
    assert isinstance(value, ast.Call)
    assert isinstance(value.func, ast.Name)
    assert value.func.id == "tuple"
    assert len(value.args) == 1
    generator = value.args[0]
    assert isinstance(generator, ast.GeneratorExp)
    assert len(generator.generators) == 1
    iterable = generator.generators[0].iter
    assert isinstance(iterable, ast.Name)
    assert iterable.id == definitions_name


def test_checked_in_ui_artifact_exactly_matches_source_contract() -> None:
    expected = render_source_catalog_artifact()

    assert GENERATED_ARTIFACT.read_text(encoding="utf-8") == expected


def test_checked_in_test_seed_artifact_exactly_matches_source_contract() -> None:
    expected = render_test_source_catalog_artifact()
    payload = json.loads(expected)

    assert GENERATED_TEST_SEED_ARTIFACT.read_text(encoding="utf-8") == expected
    assert payload == build_test_source_catalog_artifact()
    assert payload["schemaVersion"] == TEST_SEED_SCHEMA_VERSION
    assert len(payload["rows"]) == 27


def test_canonical_id_tuples_are_derived_from_definition_catalogs() -> None:
    tree = ast.parse(CATALOG_MODULE.read_text(encoding="utf-8"))

    _assert_id_tuple_is_derived_from(
        tree,
        assignment_name="CANONICAL_SOURCE_IDS",
        definitions_name="SOURCE_DEFINITIONS",
    )
    _assert_id_tuple_is_derived_from(
        tree,
        assignment_name="CANONICAL_PROVIDER_IDS",
        definitions_name="PROVIDER_DEFINITIONS",
    )


def test_generated_ui_catalog_has_identity_and_display_parity() -> None:
    payload = build_source_catalog_artifact()
    generated_sources = payload["sources"]
    assert isinstance(generated_sources, list)

    display_order = tuple(
        sorted(SOURCE_DEFINITIONS, key=lambda source: source.display.order)
    )
    assert payload["schemaVersion"] == SCHEMA_VERSION
    assert payload["canonicalSourceIds"] == list(CANONICAL_SOURCE_IDS)
    assert payload["canonicalProviderIds"] == list(CANONICAL_PROVIDER_IDS)
    assert generated_sources == [
        source.as_ui_catalog_entry() for source in display_order
    ]
    assert [source["canonicalId"] for source in generated_sources] == [
        source.source_id for source in display_order
    ]
    assert [source["id"] for source in generated_sources] == [
        source.ui_slug for source in display_order
    ]
    assert len(generated_sources) == 27


def test_generated_artifact_is_valid_json_and_ui_has_no_parallel_source_list() -> None:
    payload = json.loads(GENERATED_ARTIFACT.read_text(encoding="utf-8"))
    ui_module = ONBOARDING_MOCK_DATA.read_text(encoding="utf-8")

    assert payload == build_source_catalog_artifact()
    assert "source-catalog.generated.json" in ui_module
    assert 'source("slack"' not in ui_module
    assert "function source(" not in ui_module


def test_generator_check_mode_rejects_missing_and_stale_artifacts(
    tmp_path: Path,
) -> None:
    output = tmp_path / "source-catalog.generated.json"
    test_seed_output = tmp_path / "source-catalog-test-seed.generated.json"
    arguments = [
        "--output",
        str(output),
        "--test-seed-output",
        str(test_seed_output),
    ]

    assert main([*arguments, "--check"]) == 1
    assert main(arguments) == 0
    assert main([*arguments, "--check"]) == 0
    output.write_text("{}\n", encoding="utf-8")
    assert main([*arguments, "--check"]) == 1
