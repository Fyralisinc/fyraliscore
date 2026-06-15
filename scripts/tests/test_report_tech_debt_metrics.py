from __future__ import annotations

from pathlib import Path

from scripts.report_tech_debt_metrics import build_report, render_markdown


def test_report_counts_files_and_hotspots(tmp_path: Path) -> None:
    source = tmp_path / "services" / "demo"
    source.mkdir(parents=True)
    (source / "large.py").write_text(
        "\n".join(
            [
                "class LargeThing:",
                *[f"    def method_{idx}(self): pass" for idx in range(16)],
                "",
                "def long_function():",
                *["    value = 1" for _ in range(5)],
            ]
        ),
        encoding="utf-8",
    )

    report = build_report(
        repo_root=tmp_path,
        file_line_threshold=5,
        function_line_threshold=5,
        class_line_threshold=100,
        class_method_threshold=15,
    )

    assert report.python_files == 1
    assert report.files_over_threshold[0].path == "services/demo/large.py"
    assert report.functions_over_threshold[0].name == "long_function"
    assert report.classes_over_threshold[0].name == "LargeThing"


def test_report_counts_queue_owner_ratchet_violations(tmp_path: Path) -> None:
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

    assert report.raw_pending_post_commit_action_insert_violations == 1
    assert report.raw_think_obligation_insert_violations == 1


def test_report_markdown_includes_summary(tmp_path: Path) -> None:
    source = tmp_path / "services"
    source.mkdir()
    (source / "small.py").write_text("def f():\n    return 1\n", encoding="utf-8")

    report = build_report(repo_root=tmp_path)
    markdown = render_markdown(report)

    assert "# Technical Debt Metrics" in markdown
    assert "| Python files | 1 |" in markdown
    assert "| Raw pending post-commit action insert violations | 0 |" in markdown
    assert "| Raw Think obligation insert violations | 0 |" in markdown
