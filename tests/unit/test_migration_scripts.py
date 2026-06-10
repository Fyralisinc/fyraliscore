from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_shell_migration_scripts_do_not_record_failed_migrations() -> None:
    for rel in ("scripts/docker-migrate.sh", "scripts/start.sh"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert "ON_ERROR_STOP=1" in text
        assert "Recording" not in text
        assert "failed; the schema may already include" not in text
