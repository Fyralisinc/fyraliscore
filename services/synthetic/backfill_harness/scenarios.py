"""BackfillScenario configuration.

One scenario describes a single tenant's synthetic install + backfill:
which source, what fixture shape to build, what FaultProfile applies
to the per-source client, and the expected observation count (for
assertion validation).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from services.synthetic.fault_profiles import HAPPY_PATH, FaultProfile


# Sources the harness supports. Mirrors the M6 dispatch keys.
_VALID_SOURCES = frozenset(("gmail", "slack", "github", "discord"))


@dataclass(frozen=True)
class BackfillScenario:
    """One tenant's synthetic install + backfill configuration.

    Attributes:
      tenant_slug:
        Human-readable identifier; the harness seeds a real tenant row
        with this slug and resolves the tenant_id from it.
      source:
        One of 'gmail', 'slack', 'github', 'discord'.
      fixture_params:
        Kwargs passed to the source's `make_<source>_*` generator.
        E.g., {"email": "alice@x.com", "messages": 10, "history_events": 0}
        for Gmail, {"team_id": "T1", "channels": 2,
        "messages_per_channel": 50} for Slack.
      fault_profile:
        FaultProfile applied to the mock client serving this tenant.
        Default HAPPY_PATH.
      expected_observation_count:
        Sum of all records the fixture will yield through the M6 chain.
        For Gmail: messages * 1 record/message. For GitHub: repos *
        events_per_repo * 2 (issues + pull_requests). For Slack:
        channels * messages_per_channel. For Discord: channels *
        messages_per_channel (subject to 5% sampling — see Discord
        planner). The harness's assertions read this value.
    """

    tenant_slug: str
    source: str
    fixture_params: dict[str, Any] = field(default_factory=dict)
    fault_profile: FaultProfile = HAPPY_PATH
    expected_observation_count: int = 0

    def __post_init__(self) -> None:
        if self.source not in _VALID_SOURCES:
            raise ValueError(
                f"BackfillScenario.source must be one of {sorted(_VALID_SOURCES)}, "
                f"got {self.source!r}",
            )
        if not self.tenant_slug:
            raise ValueError("BackfillScenario.tenant_slug is required")
