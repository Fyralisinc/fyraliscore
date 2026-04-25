"""Load SimulationConfig from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from lsob_contracts import SimulationConfig


def load_config(path: str | Path) -> SimulationConfig:
    p = Path(path)
    raw = yaml.safe_load(p.read_text())
    return SimulationConfig.model_validate(raw)


def dump_config_dict(config: SimulationConfig) -> dict[str, Any]:
    return config.model_dump(mode="json")
