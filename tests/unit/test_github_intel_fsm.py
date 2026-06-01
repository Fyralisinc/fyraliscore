"""Unit: GitHub Intelligence FSM + parsing + blast-radius wiring (no DB).

Pure-logic guarantees the demo's correctness rests on:
  - classify() pulls the right entity identity from handler-shaped content
  - PR lifecycle / issue status / CI rollup transitions
  - rule_reasoning effect strings track the computed `after` state
  - the Python ast indexer extracts symbols/imports/references
"""
from __future__ import annotations

from services.github_intel import fsm
from services.github_intel.enrichment import proposed_transition, _related_entities
from services.code_intel.parsing import PythonAstIndexer


# ---- classify --------------------------------------------------------
def test_classify_issue_close():
    ev = fsm.classify({
        "event_type": "issues", "action": "closed", "issue_number": 12,
        "issue_node_id": "I_12", "repo": "acme/x", "author": "dana",
    })
    assert ev.entity_kind == "issue"
    assert ev.entity_ref == "acme/x#12"
    assert ev.action == "closed"


def test_classify_pr_merge_carries_fields():
    ev = fsm.classify({
        "event_type": "pull_request", "action": "closed", "pr_number": 42,
        "merged": True, "base_ref": "main", "head_sha": "abc", "repo": "acme/x",
    })
    assert ev.entity_kind == "pr"
    assert ev.entity_ref == "acme/x#42"
    assert ev.fields["merged"] is True


# ---- PR lifecycle FSM ------------------------------------------------
def test_pr_lifecycle_open_to_merged():
    merge = fsm.classify({"event_type": "pull_request", "action": "closed",
                          "merged": True, "pr_number": 1, "repo": "r"})
    assert fsm.pr_lifecycle_next("open", merge) == "merged"


def test_pr_lifecycle_close_without_merge():
    close = fsm.classify({"event_type": "pull_request", "action": "closed",
                          "merged": False, "pr_number": 1, "repo": "r"})
    assert fsm.pr_lifecycle_next("open", close) == "closed"


def test_pr_review_approved_then_terminal_sticky():
    approve = fsm.classify({"event_type": "pull_request_review",
                            "review_state": "approved", "pr_number": 1, "repo": "r"})
    assert fsm.pr_lifecycle_next("open", approve) == "approved"
    # terminal states are sticky — a late review never un-merges a PR
    assert fsm.pr_lifecycle_next("merged", approve) == "merged"


def test_pr_changes_requested():
    cr = fsm.classify({"event_type": "pull_request_review",
                       "review_state": "changes_requested", "pr_number": 1, "repo": "r"})
    assert fsm.pr_lifecycle_next("open", cr) == "changes_requested"


# ---- issue status FSM ------------------------------------------------
def test_issue_close_reopen():
    close = fsm.classify({"event_type": "issues", "action": "closed",
                          "issue_number": 1, "repo": "r"})
    reopen = fsm.classify({"event_type": "issues", "action": "reopened",
                           "issue_number": 1, "repo": "r"})
    assert fsm.issue_status_next("open", close) == "closed"
    assert fsm.issue_status_next("closed", reopen) == "open"
    assert fsm.issue_status_next(None, close) == "closed"


# ---- CI rollup -------------------------------------------------------
def test_ci_rollup():
    assert fsm.ci_rollup([]) == "unknown"
    assert fsm.ci_rollup([{"status": "completed", "conclusion": "success"}]) == "passing"
    assert fsm.ci_rollup([
        {"status": "completed", "conclusion": "success"},
        {"status": "completed", "conclusion": "failure"},
    ]) == "failing"
    assert fsm.ci_rollup([{"status": "in_progress", "conclusion": None}]) == "pending"


# ---- rule reasoning tracks `after` -----------------------------------
def test_rule_reasoning_issue_close_says_closed():
    ev = fsm.classify({"event_type": "issues", "action": "closed",
                       "issue_number": 12, "repo": "r"})
    r = fsm.rule_reasoning(ev, before="open", after="closed")
    assert "closed" in r["effect"]
    assert r["state_change"] == "open->closed"
    assert r["reasoning_path"] == "rule"


def test_rule_reasoning_merge_high_confidence():
    ev = fsm.classify({"event_type": "pull_request", "action": "closed",
                       "merged": True, "pr_number": 42, "base_ref": "main", "repo": "r"})
    r = fsm.rule_reasoning(ev, before="open", after="merged")
    assert r["confidence"] == 1.0
    assert "merged" in r["effect"].lower()


# ---- proposed_transition (inline read-only) --------------------------
def test_proposed_transition_issue_close_from_empty():
    ev = fsm.classify({"event_type": "issues", "action": "closed",
                       "issue_number": 12, "repo": "r"})
    trans = proposed_transition(ev, {"status": None})
    assert trans["label"] == "None->closed"
    assert trans["after"]["status"] == "closed"
    assert trans["changed"] is True


def test_related_entities_extracts_closes():
    ev = fsm.classify({"event_type": "pull_request", "action": "opened",
                       "pr_number": 42, "repo": "acme/x"})
    rel = _related_entities(ev, {"pr_title": "Fix it (fixes #12)"})
    refs = {r["ref"]: r.get("relation") for r in rel}
    assert refs.get("acme/x#12") == "closes"


# ---- Python ast indexer ----------------------------------------------
def test_python_indexer_symbols_and_refs():
    src = (
        b"from app.db import query\n\n"
        b"def verify(token):\n"
        b"    return query('x')\n\n"
        b"class Session:\n"
        b"    def refresh(self):\n"
        b"        return verify(self.t)\n"
    )
    parse = PythonAstIndexer().parse(rel_path="app/auth.py", source=src)
    qnames = {s.qualified_name for s in parse.symbols}
    assert "app.auth.verify" in qnames
    assert "app.auth.Session" in qnames
    assert "app.auth.Session.refresh" in qnames
    specs = {imp.module_specifier for imp in parse.imports}
    assert "app.db" in specs
    ref_names = {r.to_name for r in parse.references}
    assert "verify" in ref_names or "query" in ref_names


def test_python_indexer_handles_syntax_error():
    parse = PythonAstIndexer().parse(rel_path="bad.py", source=b"def (:\n")
    assert parse.parse_error is True
    assert parse.module_qname == "bad"
