"""Load SimulationConfig from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from lsob_contracts import SimulationConfig


_SIM_FIELDS: set[str] = set(SimulationConfig.model_fields.keys())


def load_config(path: str | Path) -> SimulationConfig:
    """Strict loader: every key in the YAML must be a SimulationConfig field
    (legacy CompanyA/B/C configs). For specs that mix simulator fields with
    demo-bridge extras (e.g. `recommendations`, `role_mix`), use
    `load_spec_filtered` which silently drops the extras."""
    p = Path(path)
    raw = yaml.safe_load(p.read_text())
    return SimulationConfig.model_validate(raw)


def load_spec_filtered(path: str | Path) -> SimulationConfig:
    """Permissive loader: keeps only top-level keys that SimulationConfig
    knows about and drops the rest. Used for richer spec files that also
    carry demo-bridge fields (recommendations, validator counts, etc.)."""
    p = Path(path)
    raw = yaml.safe_load(p.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"spec at {p} must be a YAML mapping at the top level")
    sim_payload = {k: v for k, v in raw.items() if k in _SIM_FIELDS}
    return SimulationConfig.model_validate(sim_payload)


def dump_config_dict(config: SimulationConfig) -> dict[str, Any]:
    return config.model_dump(mode="json")
