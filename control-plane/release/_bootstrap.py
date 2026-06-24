"""Import bootstrap for the ``release`` package (WS-RELEASE).

The release machinery reuses committed siblings that are *not* installed packages:

* ``control-plane/signing`` — ``sign_bundle`` / ``verify_bundle`` / ``signing_lib``
  are flat modules that ``import signing_lib`` (need the ``signing/`` dir on
  ``sys.path``, the same convention the signing CLIs and the agent use).
* ``control-plane/lib``     — imported as the ``lib`` package (needs the
  control-plane root on ``sys.path``); used for the C4 deployment record and the
  RFC-3339 time helpers when the rollout controller derives fleet health.

Importing this module (``import _bootstrap``) makes both available. It mirrors
``agent/_bootstrap.py`` so every consumer of the signing/lib siblings wires the
path identically. Nothing here touches the network or mutates state beyond
``sys.path``.

It also front-loads the control-plane root and **evicts a foreign top-level
``lib``** (e.g. the host repo's venv ships its own ``lib``) so ``import lib...``
binds to the control-plane shared library — the same guard the console uses.
"""

from __future__ import annotations

import sys
from pathlib import Path

# release/ -> control-plane/
_RELEASE_DIR = Path(__file__).resolve().parent
_CP_ROOT = _RELEASE_DIR.parent
_SIGNING_DIR = _CP_ROOT / "signing"

for _p in (str(_CP_ROOT), str(_SIGNING_DIR)):
    if _p not in sys.path:
        # control-plane root first so ``import lib`` resolves the package, and the
        # signing dir so ``import sign_bundle`` / ``import verify_bundle`` /
        # ``import signing_lib`` resolve.
        sys.path.insert(0, _p)

# If some unrelated top-level ``lib`` is already imported (host repo venv), evict
# it so ``from lib... import`` re-binds to the control-plane shared library.
_root = str(_CP_ROOT)
_existing = sys.modules.get("lib")
if _existing is not None and not (getattr(_existing, "__file__", "") or "").startswith(_root):
    for _name in [n for n in list(sys.modules) if n == "lib" or n.startswith("lib.")]:
        del sys.modules[_name]

RELEASE_DIR = _RELEASE_DIR
CONTROL_PLANE_ROOT = _CP_ROOT
SIGNING_DIR = _SIGNING_DIR

__all__ = ["RELEASE_DIR", "CONTROL_PLANE_ROOT", "SIGNING_DIR"]
