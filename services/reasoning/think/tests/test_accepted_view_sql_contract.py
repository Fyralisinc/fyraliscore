from pathlib import Path


def test_deterministic_supporter_membership_does_not_read_legacy_status() -> None:
    source = Path("services/reasoning/think/deterministic.py").read_text()
    query = source.split("JOIN accepted_current_models supporter", 1)[1].split(
        '"""', 1
    )[0]
    assert "supporter.status" not in query
