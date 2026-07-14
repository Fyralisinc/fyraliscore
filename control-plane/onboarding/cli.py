#!/usr/bin/env python3
"""cli.py — unified ``onboard`` / ``offboard`` dispatcher (container entrypoint).

    python cli.py onboard  --tenant acme --region us-east --plan standard
    python cli.py offboard --tenant acme --deployment acme-use1-7f3a

The two verbs are also runnable directly as ``onboard.py`` / ``offboard.py``; this
dispatcher just gives the Docker image a single entrypoint so the compose service
can be called as ``run --rm onboarding onboard …``.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

_USAGE = (
    "usage: cli.py {onboard|offboard} [options]\n"
    "  onboard  --tenant T --region R --plan P [--console-url URL | --local-ids | --embedded-console]\n"
    "  offboard --tenant T [--deployment D] [--purge-registry] [--purge-bundle]\n"
)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print(_USAGE)
        return 0 if argv else 2

    verb, rest = argv[0], argv[1:]
    if verb == "onboard":
        import onboard
        return onboard.main(rest)
    if verb == "offboard":
        import offboard
        return offboard.main(rest)
    print(f"unknown command {verb!r}\n\n{_USAGE}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
