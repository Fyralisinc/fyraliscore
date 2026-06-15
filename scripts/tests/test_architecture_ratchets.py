from __future__ import annotations

from pathlib import Path

from scripts.check_architecture_ratchets import (
    find_import_linter_allowlist_violations,
    find_raw_model_reeval_insert_violations,
    find_raw_pending_post_commit_action_insert_violations,
    find_raw_think_trigger_insert_violations,
    find_raw_think_obligation_insert_violations,
)


def test_raw_think_trigger_insert_check_flags_production_code(tmp_path: Path) -> None:
    source = tmp_path / "services" / "reasoning"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO think_trigger_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_trigger_insert_violations(repo_root=tmp_path)

    assert [v.path for v in violations] == [Path("services/reasoning/bad.py")]


def test_raw_think_trigger_insert_check_allows_helper_and_tests(tmp_path: Path) -> None:
    helper = tmp_path / "services" / "domain"
    helper.mkdir(parents=True)
    (helper / "triggers.py").write_text(
        'SQL = """\nINSERT INTO think_trigger_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "reasoning" / "think" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_worker.py").write_text(
        'SQL = """\nINSERT INTO think_trigger_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_trigger_insert_violations(repo_root=tmp_path)

    assert violations == []


def test_raw_model_reeval_insert_check_flags_production_code(tmp_path: Path) -> None:
    source = tmp_path / "services" / "reasoning"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_model_reeval_insert_violations(repo_root=tmp_path)

    assert [v.path for v in violations] == [Path("services/reasoning/bad.py")]


def test_raw_model_reeval_insert_check_allows_owners_and_tests(
    tmp_path: Path,
) -> None:
    helper = tmp_path / "services" / "domain"
    helper.mkdir(parents=True)
    (helper / "triggers.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    registry = tmp_path / "lib" / "shared"
    registry.mkdir(parents=True)
    (registry / "edge_registry.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "reasoning" / "think" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_worker.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_model_reeval_insert_violations(repo_root=tmp_path)

    assert violations == []


def test_raw_pending_post_commit_action_insert_check_flags_production_code(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "product"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO pending_post_commit_actions (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_pending_post_commit_action_insert_violations(
        repo_root=tmp_path
    )

    assert [v.path for v in violations] == [Path("services/product/bad.py")]


def test_raw_pending_post_commit_action_insert_check_allows_owner_and_tests(
    tmp_path: Path,
) -> None:
    owner = tmp_path / "services" / "reasoning" / "think"
    owner.mkdir(parents=True)
    (owner / "post_commit.py").write_text(
        'SQL = """\nINSERT INTO pending_post_commit_actions (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "reasoning" / "think" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_post_commit.py").write_text(
        'SQL = """\nINSERT INTO pending_post_commit_actions (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_pending_post_commit_action_insert_violations(
        repo_root=tmp_path
    )

    assert violations == []


def test_raw_think_obligation_insert_check_flags_production_code(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "reasoning"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO think_obligations (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_obligation_insert_violations(repo_root=tmp_path)

    assert [v.path for v in violations] == [Path("services/reasoning/bad.py")]


def test_raw_think_obligation_insert_check_allows_owner_and_tests(
    tmp_path: Path,
) -> None:
    owner = tmp_path / "services" / "domain"
    owner.mkdir(parents=True)
    (owner / "obligations.py").write_text(
        'SQL = """\nINSERT INTO think_obligations (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "domain" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_obligations.py").write_text(
        'SQL = """\nINSERT INTO think_obligations (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_obligation_insert_violations(repo_root=tmp_path)

    assert violations == []


def test_import_linter_allowlist_check_flags_growth(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.importlinter]
root_packages = ["lib", "services"]

[[tool.importlinter.contracts]]
name = "demo contract"
type = "forbidden"
source_modules = ["lib"]
forbidden_modules = ["services"]
ignore_imports = ["a -> b", "c -> d"]
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_import_linter_allowlist_violations(
        repo_root=tmp_path,
        limits={"demo contract": 1},
    )

    assert len(violations) == 1
    assert violations[0].check == "import-linter-allowlist-ratchet"


def test_import_linter_allowlist_check_allows_equal_or_lower_counts(
    tmp_path: Path,
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.importlinter]
root_packages = ["lib", "services"]

[[tool.importlinter.contracts]]
name = "demo contract"
type = "forbidden"
source_modules = ["lib"]
forbidden_modules = ["services"]
ignore_imports = ["a -> b"]
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_import_linter_allowlist_violations(
        repo_root=tmp_path,
        limits={"demo contract": 1},
    )

    assert violations == []
