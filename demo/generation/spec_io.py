"""Spec loader shared by lsob-simulation and the demo-snapshot bundler.

A spec YAML (e.g. demo/generation/specs/pelago.yaml) carries:

  - the fields that SimulationConfig knows about (consumed by the simulator)
  - extra fields used only by the demo SQL bundler — `recommendations`,
    `role_mix`, validator-target counts, etc.

`SimulationConfig._Base` has `extra="forbid"`, so we can't just hand the
raw dict to `SimulationConfig.model_validate`. This module splits the
two by name.

The set of simulator-known fields is hardcoded here (rather than imported
from `lsob_contracts`) because the main fyraliscore project does not
depend on the LSOB workspace. If you add a new top-level field to
`SimulationConfig`, add it to `_SIM_FIELDS` here too.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


# Mirrors lsob_contracts.SimulationConfig.model_fields. Adding a new
# top-level SimulationConfig field? Update both places.
_SIM_FIELDS: frozenset[str] = frozenset({
    "company_id",
    "num_actors",
    "actor_personality_distribution",
    "commitment_generation_rate",
    "customer_count",
    "turbulence_events",
    "seed",
    "start_date",
    "duration_months",
    "company_metadata",
    "actor_profiles",
    "customer_profiles",
    "goals",
    "decisions",
    "commitment_seeds",
    "signal_density",
})


def load_spec(path: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the YAML spec and split into (simulator-known dict, demo-bridge extras).

    The simulator-known dict can be passed straight to
    `SimulationConfig.model_validate(...)` from inside the LSOB workspace.
    The extras dict carries `recommendations`, `role_mix`, validator-target
    counts, etc.
    """
    p = Path(path)
    raw = yaml.safe_load(p.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"spec at {p} must be a YAML mapping at the top level")
    sim_payload = {k: v for k, v in raw.items() if k in _SIM_FIELDS}
    extras = {k: v for k, v in raw.items() if k not in _SIM_FIELDS}
    return sim_payload, extras


def sim_fields() -> frozenset[str]:
    return _SIM_FIELDS
