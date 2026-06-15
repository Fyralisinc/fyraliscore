import json

from benchmarks.check_baseline_metrics import main


def test_baseline_metrics_check_passes_within_tolerance(tmp_path, capsys) -> None:
    baseline = tmp_path / "baseline.json"
    current = tmp_path / "current.json"
    baseline.write_text(json.dumps({"evidence_recall_at_10": 0.80, "queries": 10}))
    current.write_text(json.dumps({"evidence_recall_at_10": 0.79, "queries": 10}))

    code = main([
        "--baseline",
        str(baseline),
        "--current",
        str(current),
        "--absolute-tolerance",
        "0.02",
    ])
    output = json.loads(capsys.readouterr().out)

    assert code == 0
    assert output["failures"] == []
    assert output["checked"] == ["evidence_recall_at_10"]


def test_baseline_metrics_check_fails_on_quality_regression(tmp_path, capsys) -> None:
    baseline = tmp_path / "baseline.json"
    current = tmp_path / "current.json"
    baseline.write_text(json.dumps({"accuracy": 0.75}))
    current.write_text(json.dumps({"accuracy": 0.70}))

    code = main([
        "--baseline",
        str(baseline),
        "--current",
        str(current),
        "--absolute-tolerance",
        "0.02",
    ])
    output = json.loads(capsys.readouterr().out)

    assert code == 1
    assert "accuracy" in output["failures"][0]
