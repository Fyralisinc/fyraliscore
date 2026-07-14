"""audit_view — C3 AUDIT VIEWER (read-only operator view over the I5 trail).

The control plane keeps an append-only, **hash-chained** audit log of every
security-relevant operator write (desired-config writes, license suspend/resume,
queued actions, cert events, break-glass, …). This feature renders that trail so
an operator can SEE who did what, when, to which deployment, and — critically —
whether the chain is **intact** (tamper-evident, I5).

Endpoints (both read-only; reads stay open on the operator LAN per roadmap §6,
identical to the rollup landing page — only WRITES require the operator token):

  * ``GET /api/v1/audit`` -> JSON: ``{verify:{ok,reason,count,bad_seq,signature_ok},
    count, events:[{seq, ts, actor, action, target, reason, metadata}]}``
  * ``GET /audit``        -> a self-contained HTML table (timestamp, actor,
    action, target deployment, reason, + a top banner showing chain-verify
    status). Empty trail renders the GENESIS state, never an error.

It reaches the *real* audit engine off ``deps.audit`` (the ConsoleAudit facade
exposes only ``append`` for writes, so we open the same underlying ``AuditLog``
via its lazy ``_ensure()`` accessor to get the rich ``entries()`` /
``verify_chain()`` read API — no second log, no re-implemented crypto). If the
audit engine cannot be reached for any reason, the page degrades gracefully to
"no events yet / GENESIS" rather than taking the console down (the mount loop is
already non-fatal, but this feature is doubly defensive since it is purely a
read).

This module follows the router-plugin contract: a module-level
``register(app, deps)`` mounts the endpoints onto ``app`` using ``deps``; it
never edits ``app.py``.
"""

from __future__ import annotations

import html as _html
import logging
from typing import Any, Dict, List, Optional

from fastapi.responses import HTMLResponse, JSONResponse

_LOG = logging.getLogger("fyralis.console")

# Order newest-first in the rendered views (the chain is verified in stored,
# seq-ascending order regardless of display order).
_DISPLAY_NEWEST_FIRST = True


# --------------------------------------------------------------------------- #
# reading the trail (off the real audit engine, defensively)                  #
# --------------------------------------------------------------------------- #


def _open_audit_log(deps) -> Optional[Any]:
    """Return the underlying ``AuditLog`` (read API) or ``None`` if unavailable.

    ``deps.audit`` is the write-only ``ConsoleAudit`` facade; its ``_ensure()``
    lazily opens — and returns — the shared, hash-chained ``AuditLog`` (the same
    log every operator write appends to). We use that for the rich
    ``entries()`` / ``verify_chain()`` read API. Best-effort: any failure
    (no audit on deps, open error) degrades to ``None`` => empty/GENESIS view.
    """
    audit = getattr(deps, "audit", None)
    if audit is None:
        return None
    ensure = getattr(audit, "_ensure", None)
    if not callable(ensure):
        # Unknown audit facade shape — if it already looks like an AuditLog
        # (has entries()/verify_chain()), use it directly; else give up.
        if hasattr(audit, "entries") and hasattr(audit, "verify_chain"):
            return audit
        return None
    try:
        return ensure()
    except Exception:  # never let a read take the console down
        _LOG.warning("audit viewer: could not open the audit log", exc_info=True)
        return None


def _verification_dict(log: Optional[Any]) -> Dict[str, Any]:
    """Run chain verification and project it to a JSON/HTML-friendly dict.

    Empty/absent log => the GENESIS state (ok, count 0). A present-but-broken
    chain reports ``ok:false`` with the ``bad_seq`` the engine pinpointed.
    """
    if log is None:
        return {
            "ok": True,
            "reason": "no audit log available (GENESIS)",
            "count": 0,
            "bad_seq": None,
            "signature_ok": None,
            "head_hash": None,
        }
    try:
        result = log.verify_chain(check_signature=True)
    except Exception:
        _LOG.warning("audit viewer: verify_chain failed", exc_info=True)
        return {
            "ok": False,
            "reason": "chain verification raised (unable to confirm integrity)",
            "count": 0,
            "bad_seq": None,
            "signature_ok": None,
            "head_hash": None,
        }
    return {
        "ok": bool(result.ok),
        "reason": result.reason,
        "count": int(result.count),
        "bad_seq": result.bad_seq,
        "signature_ok": result.signature_ok,
        "head_hash": result.head_hash,
    }


def _event_dicts(log: Optional[Any]) -> List[Dict[str, Any]]:
    """Project every audit entry to a flat dict for the views.

    ``reason`` and ``target deployment`` are surfaced from the entry's
    ``metadata`` when present (operator writes audited by feature routers put the
    deployment id in ``target`` and a human ``reason`` in ``metadata.reason``),
    falling back to the raw ``target`` / empty string. Returns [] on any error.
    """
    if log is None:
        return []
    events: List[Dict[str, Any]] = []
    try:
        for e in log.entries():
            meta = dict(e.metadata or {})
            # The target deployment: prefer an explicit metadata hint, else the
            # entry's own target field (feature routers set target=deployment_id).
            target_dep = (
                meta.get("deployment_id")
                or meta.get("target_deployment")
                or e.target
                or ""
            )
            reason = meta.get("reason", "") or ""
            events.append(
                {
                    "seq": e.seq,
                    "ts": e.ts,
                    "actor": e.actor,
                    "action": e.action,
                    "target": e.target,
                    "target_deployment": target_dep,
                    "reason": reason,
                    "metadata": meta,
                }
            )
    except Exception:
        _LOG.warning("audit viewer: reading entries failed", exc_info=True)
        return []
    return events


# --------------------------------------------------------------------------- #
# HTML rendering (self-contained, matches the rollup landing page's style)    #
# --------------------------------------------------------------------------- #


def _render_audit_html(verify: Dict[str, Any], events: List[Dict[str, Any]]) -> str:
    if verify["ok"]:
        sig = verify.get("signature_ok")
        if sig is True:
            banner_cls, banner_txt = (
                "v-ok",
                f"CHAIN INTACT — {verify['count']} "
                f"entr{'y' if verify['count'] == 1 else 'ies'} verified, "
                "signed checkpoint valid",
            )
        else:
            banner_cls, banner_txt = (
                "v-ok",
                f"CHAIN INTACT — {verify['count']} "
                f"entr{'y' if verify['count'] == 1 else 'ies'} verified"
                + (" (chain-only, no signed checkpoint)" if verify["count"] else ""),
            )
    else:
        bad = verify.get("bad_seq")
        where = f" at seq {bad}" if bad is not None else ""
        banner_cls, banner_txt = (
            "v-bad",
            f"CHAIN BROKEN{where} — {_html.escape(str(verify['reason']))}",
        )

    display = list(events)
    if _DISPLAY_NEWEST_FIRST:
        display = sorted(display, key=lambda x: x["seq"], reverse=True)

    rows_html: List[str] = []
    for ev in display:
        meta_extra = {
            k: v
            for k, v in ev["metadata"].items()
            if k not in ("reason", "deployment_id", "target_deployment")
        }
        meta_str = ""
        if meta_extra:
            import json as _json

            try:
                meta_str = _json.dumps(meta_extra, sort_keys=True, separators=(",", ":"))
            except Exception:
                meta_str = str(meta_extra)
        rows_html.append(
            "<tr>"
            f"<td class=mono>{ev['seq']}</td>"
            f"<td class=mono title='{_html.escape(str(ev['ts']))}'>"
            f"{_html.escape(str(ev['ts']))}</td>"
            f"<td>{_html.escape(str(ev['actor']))}</td>"
            f"<td class=mono>{_html.escape(str(ev['action']))}</td>"
            f"<td class=mono>{_html.escape(str(ev['target_deployment']))}</td>"
            f"<td>{_html.escape(str(ev['reason']))}</td>"
            f"<td class=mono title='{_html.escape(meta_str)}'>"
            f"{_html.escape(meta_str[:80])}</td>"
            "</tr>"
        )

    if not display:
        body_rows = (
            '<tr><td colspan="7" class="empty">No audit events yet &middot; '
            "GENESIS. Operator writes (config, license, actions) append "
            "hash-chained entries here.</td></tr>"
        )
    else:
        body_rows = "\n".join(rows_html)

    head_short = ""
    if verify.get("head_hash") and verify["head_hash"] != "GENESIS":
        head_short = _html.escape(str(verify["head_hash"])[:16]) + "…"
    elif verify.get("head_hash") == "GENESIS":
        head_short = "GENESIS"

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyralis BYOC — Audit Trail</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
          margin: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 .25rem; }}
  .sub {{ color: #666; font-size: .85rem; margin-bottom: 1rem; }}
  nav.top {{ margin: 0 0 1rem; font-size:.85rem; }}
  nav.top a {{ display:inline-block; margin-right:.9rem; color:#2257a8;
               text-decoration:none; font-weight:600; }}
  nav.top a:hover {{ text-decoration:underline; }}
  .banner {{ display:flex; align-items:center; gap:.6rem; padding:.6rem .9rem;
             border-radius:6px; font-weight:700; font-size:.9rem; margin:0 0 1rem; }}
  .v-ok {{ color:#0a7d28; background:#e6f6ea; border:1px solid #0a7d28; }}
  .v-bad {{ color:#a11; background:#fde6e6; border:1px solid #a11; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
  th, td {{ text-align: left; padding: .5rem .7rem; border-bottom: 1px solid #eee;
            font-size: .88rem; vertical-align: top; }}
  th {{ background: #f3f3f3; font-weight: 600; position: sticky; top: 0; }}
  tr:hover td {{ background: #fbfbfb; }}
  .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .82rem; }}
  .empty {{ text-align:center; color:#777; padding:1.5rem; }}
  footer {{ margin-top: 1rem; color:#999; font-size:.78rem; }}
</style>
</head>
<body>
  <h1>Fyralis BYOC — Audit Trail</h1>
  <nav class="top">
    <a href="/">Fleet</a>
    <a href="/audit">Audit</a>
    <a href="/alerts">Alerts</a>
    <a href="/metering">Metering</a>
  </nav>
  <div class="sub">Append-only, hash-chained operator trail (I5 — tamper-evident).
    Reads are open on the operator LAN; writes that appear here required the
    operator token.</div>
  <div class="banner {banner_cls}">{banner_txt}</div>
  <table>
    <thead>
      <tr>
        <th>#</th><th>Timestamp</th><th>Actor</th><th>Action</th>
        <th>Target deployment</th><th>Reason</th><th>Metadata</th>
      </tr>
    </thead>
    <tbody>
{body_rows}
    </tbody>
  </table>
  <footer>Head: <span class="mono">{head_short or "—"}</span> &middot;
    GET /api/v1/audit for JSON.</footer>
</body>
</html>"""


# --------------------------------------------------------------------------- #
# router registration                                                         #
# --------------------------------------------------------------------------- #


def register(app, deps) -> None:
    """Mount the C3 audit viewer endpoints onto ``app`` using ``deps``."""

    @app.get(
        "/api/v1/audit",
        tags=["audit"],
        summary="C3 audit viewer (JSON) — hash-chained operator trail + verify status.",
    )
    def audit_json() -> JSONResponse:
        log = _open_audit_log(deps)
        verify = _verification_dict(log)
        events = _event_dicts(log)
        return JSONResponse(
            content={
                "verify": verify,
                "count": len(events),
                "events": events,
            }
        )

    @app.get(
        "/audit",
        response_class=HTMLResponse,
        tags=["audit"],
        summary="C3 audit viewer (HTML) — read-only operator view of the I5 trail.",
    )
    def audit_html() -> HTMLResponse:
        log = _open_audit_log(deps)
        verify = _verification_dict(log)
        events = _event_dicts(log)
        return HTMLResponse(content=_render_audit_html(verify, events))
