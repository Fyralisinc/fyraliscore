"""Coverage ratchet for public webhook retry/dedup behavior."""
from __future__ import annotations

from pathlib import Path

from services.app.webhooks.router import _PROVIDER_CHANNEL


_ROOT = Path(__file__).resolve().parents[4]

_DEDUP_EVIDENCE: dict[str, Path] = {
    "slack": Path("services/ingest/ingestion/handlers/tests/test_slack.py"),
    "github": Path("services/ingest/ingestion/handlers/tests/test_github.py"),
    "linear": Path("services/ingest/ingestion/handlers/tests/test_linear.py"),
    "stripe": Path("services/ingest/ingestion/handlers/tests/test_stripe.py"),
    "discord": Path("services/ingest/integrations/tests/test_ingest_discord.py"),
    "jira": Path("services/ingest/ingestion/handlers/tests/test_jira.py"),
    "mercury": Path("services/ingest/ingestion/handlers/tests/test_mercury.py"),
    "quickbooks": Path("services/ingest/ingestion/handlers/tests/test_quickbooks.py"),
    "grafana": Path("services/ingest/ingestion/handlers/tests/test_grafana.py"),
    "brex": Path("services/ingest/ingestion/handlers/tests/test_brex.py"),
    "ramp": Path("services/ingest/ingestion/handlers/tests/test_ramp.py"),
    "gusto": Path("services/ingest/ingestion/handlers/tests/test_gusto.py"),
    "deel": Path("services/ingest/ingestion/handlers/tests/test_deel.py"),
    "fireflies": Path("services/ingest/ingestion/handlers/tests/test_fireflies.py"),
    "miro": Path("services/ingest/ingestion/handlers/tests/test_miro.py"),
    "figma": Path("services/ingest/ingestion/handlers/tests/test_figma.py"),
    "hibob": Path("services/ingest/ingestion/handlers/tests/test_hibob.py"),
    "ashby": Path("services/ingest/ingestion/handlers/tests/test_ashby.py"),
}


def test_webhook_providers_have_retry_dedup_test_evidence() -> None:
    assert set(_PROVIDER_CHANNEL) == set(_DEDUP_EVIDENCE)
    for provider, path in _DEDUP_EVIDENCE.items():
        full_path = _ROOT / path
        assert full_path.exists(), f"{provider} missing dedup evidence file: {path}"
        text = full_path.read_text(encoding="utf-8")
        assert "external_id" in text, f"{provider} test file must assert dedup keys"
