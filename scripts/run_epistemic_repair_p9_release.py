#!/usr/bin/env python3
"""Compatibility command name for the strict reviewer-bound P9 report builder."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_epistemic_repair_p9_release import main


if __name__ == "__main__":
    raise SystemExit(main())
