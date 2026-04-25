"""Tiny sys.path shim so `python simulation/workers/foo.py` works.

Worker scripts import this first — before any `from simulation ...`
imports. The shim only acts when `simulation` is not already on
sys.path (e.g. when the worker is invoked by filename instead of as
`python -m simulation.workers.foo`).
"""
from __future__ import annotations

import pathlib
import sys


_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
