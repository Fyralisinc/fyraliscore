from __future__ import annotations

from pathlib import Path

from scripts.check_tech_debt_budget import TechDebtBudget, check_budget
from scripts.report_tech_debt_metrics import build_report


def test_tech_debt_budget_allows_counts_at_limit(tmp_path: Path) -> None:
    source = tmp_path / "services"
    source.mkdir()
    (source / "small.py").write_text("def f():\n    return 1\n", encoding="utf-8")

    report = build_report(repo_root=tmp_path)

    assert check_budget(report, TechDebtBudget()) == []


def test_tech_debt_budget_flags_hotspot_growth(tmp_path: Path) -> None:
    source = tmp_path / "services"
    source.mkdir()
    (source / "large.py").write_text(
        "\n".join(["def too_long():", *["    value = 1" for _ in range(5)]]) + "\n",
        encoding="utf-8",
    )
    report = build_report(
        repo_root=tmp_path,
        file_line_threshold=5,
        function_line_threshold=5,
    )

    violations = check_budget(
        report,
        TechDebtBudget(
            files_over_threshold=0,
            functions_over_threshold=0,
        ),
    )

    assert [(violation.metric, violation.actual) for violation in violations] == [
        ("files_over_threshold", 1),
        ("functions_over_threshold", 1),
    ]


def test_tech_debt_budget_flags_file_line_budget_growth(tmp_path: Path) -> None:
    source = tmp_path / "services" / "demo"
    source.mkdir(parents=True)
    path = source / "large.py"
    path.write_text("\n".join(["value = 1" for _ in range(5)]) + "\n", encoding="utf-8")
    report = build_report(repo_root=tmp_path, file_line_threshold=100)

    violations = check_budget(
        report,
        TechDebtBudget(
            file_line_budgets={
                "services/demo/large.py": 4,
            },
        ),
        repo_root=tmp_path,
    )

    assert [
        (violation.metric, violation.actual, violation.limit)
        for violation in violations
    ] == [
        ("file_line_budget:services/demo/large.py", 5, 4),
    ]


def test_tech_debt_budget_flags_function_line_budget_growth(tmp_path: Path) -> None:
    source = tmp_path / "services" / "demo"
    source.mkdir(parents=True)
    path = source / "large.py"
    path.write_text(
        "\n".join(["def too_long():", *["    value = 1" for _ in range(5)]]) + "\n",
        encoding="utf-8",
    )
    report = build_report(repo_root=tmp_path, function_line_threshold=100)

    violations = check_budget(
        report,
        TechDebtBudget(
            function_line_budgets={
                "services/demo/large.py:too_long": 4,
            },
        ),
        repo_root=tmp_path,
    )

    assert [
        (violation.metric, violation.actual, violation.limit)
        for violation in violations
    ] == [
        ("function_line_budget:services/demo/large.py:too_long", 6, 4),
    ]


def test_tech_debt_budget_flags_queue_owner_ratchet_growth(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "demo"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        "\n".join(
            [
                'SQL_ONE = """',
                "INSERT INTO pending_post_commit_actions (id) VALUES ($1)",
                '"""',
                'SQL_TWO = """',
                "INSERT INTO think_obligations (id) VALUES ($1)",
                '"""',
            ]
        ),
        encoding="utf-8",
    )
    report = build_report(repo_root=tmp_path)

    violations = check_budget(report, TechDebtBudget())

    assert [(violation.metric, violation.actual) for violation in violations] == [
        ("raw_pending_post_commit_action_insert_violations", 1),
        ("raw_think_obligation_insert_violations", 1),
    ]
