"""pytest bootstrap for the console tests.

Ensures the **control-plane root** (the dir holding ``SPRINT_PLAN.md`` / ``lib/``)
and the ``console/`` package dir are at the FRONT of ``sys.path`` *before* any
test module is imported, so ``import lib...`` resolves to the control-plane's
shared library and not some other ``lib`` package that happens to be on the path
(e.g. the fyraliscore repo whose virtualenv runs these tests ships its own
top-level ``lib/``). It also evicts any already-imported ``lib`` so the
control-plane copy wins.
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent          # console/tests
_CONSOLE = _HERE.parent                            # console/
_ROOT = _CONSOLE.parent                            # control-plane/

# Front-load the control-plane root then console/ so our packages win.
for _p in (str(_CONSOLE), str(_ROOT)):
    while _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)
# Order ends up: [console, root, ...]; root must precede any foreign `lib`.
sys.path.remove(str(_ROOT))
sys.path.insert(0, str(_ROOT))

# If a foreign `lib` (or its submodules) was already imported, drop it so the
# next `import lib.deployment` binds to control-plane/lib/.
_lib = sys.modules.get("lib")
if _lib is not None:
    _libfile = getattr(_lib, "__file__", "") or ""
    if not _libfile.startswith(str(_ROOT)):
        for _name in [n for n in sys.modules if n == "lib" or n.startswith("lib.")]:
            del sys.modules[_name]
