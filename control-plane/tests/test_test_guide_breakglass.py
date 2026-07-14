"""test_test_guide_breakglass — doc-accuracy regression guard for TEST_GUIDE.md §B.7.

`docs/TEST_GUIDE.md` promises every command in the break-glass walkthrough is real and
runnable, and shows the exact `audit list` / `audit verify` output a reader should expect.
This test drives the SAME `audit/cli.py` the docker `audit` service runs, following the
walkthrough step-for-step, and asserts the corrected claims actually hold:

  * step 1's printed line carries a `bg-<hex>` grant id that step 2 can approve (the guide
    captures it into `$GID` — a hard-coded literal id would 404);
  * the over-broad scope check (step b4) is audited as **breakglass.check_denied**, NOT
    `breakglass.deny` — no `deny` command is ever run in the walkthrough;
  * audit seqs are **0-based** (the (c) expected list starts at `[   0]`);
  * tampering line 1 of the log reports **CHAIN BROKEN at seq 0** (the (d) demo).

If someone reverts the doc to the old (non-runnable / wrong-event / 1-based) text, the doc
and this test disagree; if someone changes the CLI's behaviour, this test catches the drift
the doc would otherwise hide. No Docker needed — it calls the CLI in-process.
"""

from __future__ import annotations

import io
import os
import re
import sys
from contextlib import redirect_stdout, redirect_stderr

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
AUDIT_DIR = os.path.normpath(os.path.join(HERE, "..", "audit"))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (AUDIT_DIR, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import cli as audit_cli  # noqa: E402  (audit/cli.py — the exact CLI the docker service runs)


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
    """Invoke audit/cli.py main() the way the walkthrough invokes the container; capture io."""
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        rc = audit_cli.main(argv)
    return rc, out.getvalue(), err.getvalue()


def _common(tmp_path) -> list[str]:
    # No signing key on this host → chain-only mode (checkpoint-sig=n/a); the hash chain
    # still holds, which is all this walkthrough-fidelity test needs.
    log = str(tmp_path / "audit.log.jsonl")
    store = str(tmp_path / "breakglass_grants.json")
    return ["--log", log, "--store", store]


def test_test_guide_b7_walkthrough_is_runnable_and_faithful(tmp_path):
    base = _common(tmp_path)

    # (b)1 — request: INERT grant; the printed id is bg-<random hex>, captured into a var.
    rc, out, _ = _run_cli(
        base
        + ["breakglass", "request", "--actor", "sre@fyralis",
           "--scope", "tenant:acme/logs:read", "--ttl", "900", "--reason", "inc-4127"]
    )
    assert rc == 0, out
    m = re.search(r"\b(bg-[0-9a-f]+)\b", out)
    assert m, f"request did not print a bg-<hex> grant id to capture: {out!r}"
    gid = m.group(1)
    # The OLD doc hard-coded this exact literal — prove it is NOT a fixed value.
    assert gid != "bg-1a2b3c4d5e6f", "grant id must be random, not the old hard-coded literal"
    assert "AWAITING CUSTOMER APPROVAL" in out

    # (b)2 — customer approves the CAPTURED id (the runnable, copy-paste-safe form).
    rc, out, _ = _run_cli(
        base + ["breakglass", "approve", "--grant-id", gid, "--approved-by", "acme-admin@acme.com"]
    )
    assert rc == 0, out
    assert gid in out and "expires in 900.0s" in out

    # (b)3 — in-scope, in-window access is ALLOWED (exit 0) and audited as a USE.
    rc, out, _ = _run_cli(
        base + ["breakglass", "check", "--actor", "sre@fyralis", "--scope", "tenant:acme/logs:read"]
    )
    assert rc == 0, out
    assert out.startswith("ALLOW:")

    # (b)4 — a different/broader scope is DENIED (exit 1) — this is the event the guide must
    #        label correctly. It is breakglass.check_denied, NOT breakglass.deny.
    rc, _, err = _run_cli(
        base + ["breakglass", "check", "--actor", "sre@fyralis", "--scope", "tenant:bossco/logs:read"]
    )
    assert rc == 1
    assert err.startswith("DENY:")

    # (c) — the audit list the guide prints. Assert the exact action sequence + 0-based seqs.
    rc, out, _ = _run_cli(base + ["audit", "list", "--limit", "10"])
    assert rc == 0, out
    lines = [ln for ln in out.splitlines() if ln.strip()]
    assert len(lines) == 4, f"expected 4 audited events, got {len(lines)}:\n{out}"

    # seqs are 0-based: the first listed entry is [   0], matching the corrected doc.
    seqs = [int(re.match(r"\[\s*(\d+)\]", ln).group(1)) for ln in lines]
    assert seqs == [0, 1, 2, 3], f"seqs must be 0-based and contiguous, got {seqs}"

    # line shape: "[   n] <ts>  <actor>  <ACTION>  -> <target>  {...}" — ACTION is the token
    # immediately before the "->" marker.
    actions = [ln.split()[ln.split().index("->") - 1] for ln in lines]
    assert actions == [
        "breakglass.request",
        "breakglass.approve",
        "breakglass.use",
        "breakglass.check_denied",   # NOT breakglass.deny — the doc's old claim was wrong
    ], f"audit action sequence drifted from the corrected TEST_GUIDE: {actions}"
    assert "breakglass.deny" not in actions, "no deny command is run in the walkthrough"

    # the chain still verifies (guide's (c) audit verify → exit 0).
    rc, out, _ = _run_cli(base + ["audit", "verify"])
    assert rc == 0, out
    assert "CHAIN OK" in out


def test_test_guide_b7d_tamper_reports_seq_0(tmp_path):
    """(d) tamper demo: flipping line 1 of the log reports `CHAIN BROKEN at seq 0`."""
    base = _common(tmp_path)
    log_path = base[1]  # value after "--log"

    rc, out, _ = _run_cli(
        base
        + ["breakglass", "request", "--actor", "sre@fyralis",
           "--scope", "tenant:acme/logs:read", "--ttl", "900"]
    )
    assert rc == 0, out

    # Same edit the guide's sed does: rewrite the actor on the FIRST line (seq 0).
    with open(log_path, "r", encoding="utf-8") as fh:
        lines = fh.readlines()
    lines[0] = lines[0].replace("sre@fyralis", "mallory@evil")
    with open(log_path, "w", encoding="utf-8") as fh:
        fh.writelines(lines)

    rc, _, err = _run_cli(base + ["audit", "verify"])
    assert rc == 1
    assert "CHAIN BROKEN at seq 0" in err, f"tamper demo must point at seq 0, got: {err!r}"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
