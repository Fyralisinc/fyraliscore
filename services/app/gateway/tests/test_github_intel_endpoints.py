"""Integration: GitHub Intelligence read API over the real gateway (live DB).

Reuses the gateway test harness (`client` httpx.AsyncClient over the real app +
`valid_session` bearer token from services/app/gateway/tests/conftest.py). It seeds
code + signals through the REAL ingestion path + worker, then drives every
endpoint over HTTP and asserts shapes + the allowlist 404.

Per-run isolation: unique repo derived from the session's tenant (the dev DB's
superuser role bypasses RLS, and observation dedup is global — see the Feature-1
pipeline test for the same rationale).
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from uuid import UUID

import httpx
import pytest

pytestmark = [pytest.mark.integration]

T0 = datetime(2026, 5, 30, 12, 0, 0, tzinfo=timezone.utc)
FIXTURE = {
    "app/db.py": "def query(sql):\n    return []\n",
    "app/auth.py": (
        "from app.db import query\n\n"
        "def verify_token(token):\n"
        "    return query('select * from sessions')\n"
    ),
    "app/api.py": (
        "from app.auth import verify_token\n\n"
        "def handle(req):\n"
        "    return verify_token(req.token)\n"
    ),
}


def _ts(i: int) -> str:
    return (T0 + timedelta(minutes=i)).isoformat()


def _write_fixture() -> str:
    root = tempfile.mkdtemp(prefix="ghintel_api_")
    for rel, body in FIXTURE.items():
        p = os.path.join(root, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            f.write(body)
    return root


async def _seed(pool, tenant_id: UUID, repo: str, uniq: str) -> None:
    """Index code + ingest a PR open→approved→merged sequence + drain the worker."""
    from services.ingest.ingestion.feature_flags.client import TenantFlags
    from services.ingest.github_intel.config import GITHUB_INTEL_ENABLED, CODE_INTEL_ENABLED
    from services.ingest.code_intel.indexer import index_working_copy
    from services.ingest.ingestion.handlers import get_handler
    from services.ingest.ingestion.core import ingest_from_draft
    from services.ingest.github_intel.worker import drain, enqueue_new_github_observations

    flags = TenantFlags(pool)
    await flags.set_bool(tenant_id, CODE_INTEL_ENABLED, True, set_by="test")
    await flags.set_bool(tenant_id, GITHUB_INTEL_ENABLED, True, set_by="test")

    await index_working_copy(
        pool=pool, tenant_id=tenant_id, repo_full_name=repo,
        root_path=_write_fixture(), commit_sha=f"sha-{uniq}", branch="main",
    )

    handler = get_handler("github:webhook")
    events = [
        ("pull_request", {
            "action": "opened", "repository": {"full_name": repo}, "sender": {"login": "alice"},
            "pull_request": {"number": 7, "title": "touch db (fixes #3)", "node_id": f"PRo-{uniq}",
                             "merged": False, "base": {"ref": "main"},
                             "head": {"ref": "f", "sha": "h7"}, "_changed_files": ["app/db.py"],
                             "created_at": _ts(0), "updated_at": _ts(0)}}),
        ("pull_request_review", {
            "action": "submitted", "repository": {"full_name": repo}, "sender": {"login": "bob"},
            "review": {"state": "approved", "node_id": f"RV-{uniq}", "submitted_at": _ts(1)},
            "pull_request": {"number": 7, "node_id": "PR7"}}),
        ("pull_request", {
            "action": "closed", "repository": {"full_name": repo}, "sender": {"login": "alice"},
            "pull_request": {"number": 7, "title": "touch db (fixes #3)", "node_id": f"PRm-{uniq}",
                             "merged": True, "base": {"ref": "main"},
                             "head": {"ref": "f", "sha": "h7"}, "merge_commit_sha": "m7",
                             "_changed_files": ["app/db.py"], "created_at": _ts(0), "updated_at": _ts(2)}}),
    ]
    for event, payload in events:
        draft = await handler(payload, {"X-GitHub-Event": event})
        await ingest_from_draft(channel="github:webhook", draft=draft, pool=pool,
                                tenant_id=tenant_id, enqueue_trigger=False)
    await enqueue_new_github_observations(pool, tenant_id)
    await drain(pool, tenant_id, worker_id="test", llm_enabled=False)


def _hdr(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def test_endpoints_require_auth(client: httpx.AsyncClient):
    resp = await client.get("/github-intel/repos")
    assert resp.status_code == 401


async def test_repos_and_state(client, gateway_pool, valid_session, tenant_id):
    token, _ = valid_session
    repo = f"acme/api-it-{tenant_id.hex[:8]}"
    await _seed(gateway_pool, tenant_id, repo, tenant_id.hex[:8])
    owner, name = repo.split("/")

    # discovery
    resp = await client.get("/github-intel/repos", headers=_hdr(token))
    assert resp.status_code == 200
    body = resp.json()
    assert repo in [r["repo"] for r in body["repos"]]

    # state
    resp = await client.get(f"/github-intel/repos/{owner}/{name}/state", headers=_hdr(token))
    assert resp.status_code == 200, resp.text
    state = resp.json()
    assert state["repo"] == repo
    assert state["code_index"]["indexed"] is True
    pr = [p for p in state["pull_requests"] if p["pr_number"] == 7][0]
    assert pr["lifecycle"] == "merged"


async def test_signals_and_explain(client, gateway_pool, valid_session, tenant_id):
    token, _ = valid_session
    repo = f"acme/api-it-{tenant_id.hex[:8]}"
    await _seed(gateway_pool, tenant_id, repo, tenant_id.hex[:8])
    owner, name = repo.split("/")

    resp = await client.get(f"/github-intel/repos/{owner}/{name}/signals", headers=_hdr(token))
    assert resp.status_code == 200
    signals = resp.json()["signals"]
    assert len(signals) == 3
    # newest-first
    assert signals[0]["enriched_at"] >= signals[-1]["enriched_at"]
    merge = [s for s in signals if s["action"] == "closed"][0]
    assert merge["state_changed"] is True
    assert merge["blast_radius_count"] >= 2  # app/db.py has dependents

    # compound-cursor pagination: walk one row at a time via next_before and
    # assert every signal is seen exactly once (no boundary drop / no dup even
    # when the 3 rows share an enriched_at, which the worker writes in one txn).
    seen: list[str] = []
    cursor = None
    url = f"/github-intel/repos/{owner}/{name}/signals"
    for _ in range(5):  # bounded; 3 rows @ limit=1 -> 3 pages + 1 empty
        params = {"limit": 1}
        if cursor:
            params["before"] = cursor  # httpx URL-encodes the compound cursor
        resp = await client.get(url, params=params, headers=_hdr(token))
        assert resp.status_code == 200, resp.text
        page = resp.json()
        if not page["signals"]:
            break
        seen.append(page["signals"][0]["observation_id"])
        cursor = page["next_before"]
        if cursor is None:
            break
    assert len(seen) == 3
    assert len(set(seen)) == 3  # no duplicates across pages

    # explain the merge signal
    obs_id = merge["observation_id"]
    resp = await client.get(f"/github-intel/signals/{obs_id}/explain", headers=_hdr(token))
    assert resp.status_code == 200, resp.text
    ex = resp.json()
    assert ex["enrichment"]["cause"]
    assert ex["enrichment"]["effect"]
    assert ex["intelligence"] is not None  # inline view present on the row


async def test_prs_list_and_detail(client, gateway_pool, valid_session, tenant_id):
    token, _ = valid_session
    repo = f"acme/api-it-{tenant_id.hex[:8]}"
    await _seed(gateway_pool, tenant_id, repo, tenant_id.hex[:8])
    owner, name = repo.split("/")

    resp = await client.get(f"/github-intel/repos/{owner}/{name}/prs?state=merged", headers=_hdr(token))
    assert resp.status_code == 200
    prs = resp.json()["pull_requests"]
    assert [p["pr_number"] for p in prs] == [7]

    resp = await client.get(f"/github-intel/repos/{owner}/{name}/prs/7", headers=_hdr(token))
    assert resp.status_code == 200, resp.text
    detail = resp.json()
    assert detail["lifecycle"] == "merged"
    assert len(detail["timeline"]) == 3  # opened, approved, merged

    # unknown PR -> 404
    resp = await client.get(f"/github-intel/repos/{owner}/{name}/prs/999", headers=_hdr(token))
    assert resp.status_code == 404


async def test_blast_radius_and_code_search(client, gateway_pool, valid_session, tenant_id):
    token, _ = valid_session
    repo = f"acme/api-it-{tenant_id.hex[:8]}"
    await _seed(gateway_pool, tenant_id, repo, tenant_id.hex[:8])
    owner, name = repo.split("/")

    resp = await client.get(
        f"/github-intel/repos/{owner}/{name}/blast-radius?path=app/db.py", headers=_hdr(token))
    assert resp.status_code == 200, resp.text
    br = resp.json()
    assert br["indexed"] is True
    dep_files = [d["path"] for d in br["dependent_files"]]
    assert "app/auth.py" in dep_files and "app/api.py" in dep_files

    # missing path -> 400
    resp = await client.get(
        f"/github-intel/repos/{owner}/{name}/blast-radius", headers=_hdr(token))
    assert resp.status_code == 400

    # blank/whitespace-only path -> 400 (not a misleading 200)
    resp = await client.get(
        f"/github-intel/repos/{owner}/{name}/blast-radius", params={"path": ""},
        headers=_hdr(token))
    assert resp.status_code == 400

    # code-search: tolerate Ollama being down (results may be empty)
    resp = await client.get(
        f"/github-intel/repos/{owner}/{name}/code-search?q=verify+token", headers=_hdr(token))
    assert resp.status_code == 200
    assert "results" in resp.json()


async def test_allowlist_blocks_unowned_repo(client, valid_session):
    token, _ = valid_session
    # a repo this tenant has no intel for and no provider install -> 404
    resp = await client.get(
        "/github-intel/repos/someone-else/private-repo/state", headers=_hdr(token))
    assert resp.status_code == 404
    assert resp.json()["error"] == "repo_not_found"


async def test_explain_unknown_signal_404(client, valid_session):
    token, _ = valid_session
    resp = await client.get(
        "/github-intel/signals/019e0000-0000-7000-8000-000000000000/explain",
        headers=_hdr(token))
    assert resp.status_code == 404

    # malformed uuid -> 400
    resp = await client.get("/github-intel/signals/not-a-uuid/explain", headers=_hdr(token))
    assert resp.status_code == 400
