"""services/ingest/github_intel/enrichment.py — assemble the causal context for a signal.

Produces the `content["intelligence"]` dict that ties causal context to a GitHub
signal: the state transition it caused, the code it touches (blast radius from
code_intel), related entities, and the rule-based (or optional LLM) "why".

Used by both the inline path (read-only proposed transition) and the ordered
worker (authoritative transition from state_store.apply_event).
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from uuid import UUID

from services.ingest.github_intel import fsm
from services.ingest.github_intel.code_client import (
    blast_radius_for, code_rag_for, extract_changed_paths,
)
from services.ingest.github_intel.fsm import GithubEvent

_REF_RE = re.compile(r"(?:closes|fixes|resolves)\s+#(\d+)", re.IGNORECASE)
_HASH_RE = re.compile(r"#(\d+)")


def _related_entities(ev: GithubEvent, content: dict[str, Any]) -> list[dict[str, Any]]:
    rel: list[dict[str, Any]] = []
    if ev.entity_ref:
        rel.append({"kind": ev.entity_kind, "ref": ev.entity_ref})
    text = " ".join(str(content.get(k) or "") for k in ("pr_title", "issue_title", "body"))
    repo = ev.repo or ""
    closes = {m.group(1) for m in _REF_RE.finditer(text)}
    for num in sorted(closes):
        rel.append({"kind": "issue", "ref": f"{repo}#{num}", "relation": "closes"})
    for m in _HASH_RE.finditer(text):
        ref = f"{repo}#{m.group(1)}"
        if m.group(1) not in closes and not any(r["ref"] == ref for r in rel):
            rel.append({"kind": "ref", "ref": ref, "relation": "mentions"})
    return rel


def proposed_transition(ev: GithubEvent, snap: dict[str, Any]) -> dict[str, Any]:
    """Read-only proposed before/after for the inline path."""
    if ev.entity_kind == "pr":
        before = snap.get("lifecycle")
        after = fsm.pr_lifecycle_next(before, ev)
        changed = after != (before or "open")
        return {"before": {"lifecycle": before, "ci_state": snap.get("ci_state")},
                "after": {"lifecycle": after, "ci_state": snap.get("ci_state")},
                "changed": changed, "label": f"{before}->{after}" if changed else "none"}
    if ev.entity_kind == "issue":
        before = snap.get("status")
        after = fsm.issue_status_next(before, ev)
        changed = after != (before or "open")
        return {"before": {"status": before}, "after": {"status": after},
                "changed": changed, "label": f"{before}->{after}" if changed else "none"}
    if ev.entity_kind == "branch":
        before = snap.get("head_sha")
        after = ev.fields.get("after")
        changed = bool(after) and after != before
        return {"before": {"head_sha": before}, "after": {"head_sha": after},
                "changed": changed,
                "label": f"{(before or '')[:8]}->{(after or '')[:8]}" if changed else "none"}
    if ev.entity_kind == "check":
        concl = ev.fields.get("conclusion")
        return {"before": {}, "after": {"check_conclusion": concl},
                "changed": ev.fields.get("status") == "completed",
                "label": f"check:{concl}"}
    return {"before": {}, "after": {}, "changed": False, "label": "none"}


def assemble_intelligence(
    ev: GithubEvent,
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    state_changed: bool,
    state_label: str,
    snapshot_sha: str | None,
    blast_radius: dict[str, Any],
    reasoning: dict[str, Any],
    related: list[dict[str, Any]],
    code_rag: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dep_files = blast_radius.get("dependent_files", []) if blast_radius else []
    dep_syms = blast_radius.get("dependent_symbols", []) if blast_radius else []
    changed_files = blast_radius.get("changed_files", []) if blast_radius else []
    changed_syms = blast_radius.get("changed_symbols", []) if blast_radius else []
    intel: dict[str, Any] = {
        "enriched": True,
        "state_change": state_label,
        "state_changed": state_changed,
        "state_before": before,
        "state_after": after,
        "entity": {"kind": ev.entity_kind, "ref": ev.entity_ref},
        "cause": reasoning.get("cause"),
        "effect": reasoning.get("effect"),
        "explanation": reasoning.get("explanation"),
        "confidence": reasoning.get("confidence"),
        "reasoning_path": reasoning.get("reasoning_path", "rule"),
        "affected": {
            "indexed": bool(blast_radius.get("indexed")) if blast_radius else False,
            "changed_files": changed_files,
            "changed_symbols": [s.get("qualified_name") for s in changed_syms][:25],
            "dependent_files": dep_files[:25],
            "dependent_symbols": [s.get("qualified_name") for s in dep_syms][:25],
            "blast_radius_count": len(dep_files) + len(dep_syms),
        },
        "code_snapshot_sha": snapshot_sha,
        "related_entities": related,
    }
    if code_rag:
        intel["relevant_code"] = code_rag
    return intel


async def build_inline_intelligence(
    ctx: Any,
    *,
    tenant_id: UUID,
    content: dict[str, Any],
    raw_payload: dict[str, Any] | None,
    occurred_at: datetime,
    llm_enabled: bool = False,
    embedder: Any | None = None,
    max_hops: int = 3,
    with_code_rag: bool = False,
) -> dict[str, Any]:
    """Compute the intelligence dict for the INLINE path (read-only state)."""
    ev = fsm.classify(content)
    from services.ingest.github_intel.state_store import read_state_snapshot
    snap = await read_state_snapshot(ctx, ev)
    trans = proposed_transition(ev, snap)

    snapshot_sha, blast = (None, {})
    if ev.repo:
        changed_paths = extract_changed_paths(content, raw_payload)
        snapshot_sha, blast = await blast_radius_for(
            ctx, tenant_id=tenant_id, repo=ev.repo,
            changed_paths=changed_paths, max_hops=max_hops,
        )

    reasoning = fsm.rule_reasoning(
        ev,
        before=_first(trans["before"]),
        after=_first(trans["after"]),
    )
    if llm_enabled and not fsm.is_obvious(ev):
        try:
            from services.ingest.github_intel.reasoner import llm_causal
            llm = await llm_causal(ev, before=trans["before"], after=trans["after"],
                                   blast_radius=blast, content=content)
            if llm:
                reasoning = llm
        except Exception:  # noqa: BLE001 — LLM optional; rule fallback already set
            pass

    code_rag = None
    if with_code_rag and ev.repo:
        code_rag = await code_rag_for(
            ctx, tenant_id=tenant_id, repo=ev.repo,
            query_text=content.get("content_text") or reasoning.get("cause") or "",
            embedder=embedder,
        )

    return assemble_intelligence(
        ev, before=trans["before"], after=trans["after"],
        state_changed=trans["changed"], state_label=trans["label"],
        snapshot_sha=snapshot_sha, blast_radius=blast, reasoning=reasoning,
        related=_related_entities(ev, content), code_rag=code_rag,
    )


def _first(d: dict[str, Any]) -> Any:
    """The primary state value from a before/after dict (lifecycle|status|head_sha)."""
    for k in ("lifecycle", "status", "head_sha"):
        if k in d:
            return d[k]
    return None
