"""Import bootstrap for the agent package.

The agent reuses three committed siblings that are *not* installed packages:

* ``control-plane/lib``     — imported as the ``lib`` package (needs the
  control-plane root on ``sys.path``).
* ``control-plane/signing`` — ``verify_bundle`` / ``signing_lib`` are flat
  modules that ``import signing_lib`` (need the ``signing/`` dir on
  ``sys.path``, same convention the signing CLIs use).

Importing this module (``import _bootstrap``) makes both available. It is the one
place that knows the on-disk layout so the rest of the agent imports cleanly::

    import _bootstrap  # noqa: F401  (side-effect: sys.path)
    from lib import DeploymentRecord
    import verify_bundle

Nothing here touches the network or mutates state beyond ``sys.path``.
"""

from __future__ import annotations

import sys
from pathlib import Path

# agent/ -> control-plane/
_AGENT_DIR = Path(__file__).resolve().parent
_CP_ROOT = _AGENT_DIR.parent
_SIGNING_DIR = _CP_ROOT / "signing"

for _p in (str(_CP_ROOT), str(_SIGNING_DIR)):
    if _p not in sys.path:
        # control-plane root first so ``import lib`` resolves the package, and the
        # signing dir so ``import verify_bundle`` / ``import signing_lib`` resolve.
        sys.path.insert(0, _p)

AGENT_DIR = _AGENT_DIR
CONTROL_PLANE_ROOT = _CP_ROOT
SIGNING_DIR = _SIGNING_DIR

__all__ = ["AGENT_DIR", "CONTROL_PLANE_ROOT", "SIGNING_DIR"]
