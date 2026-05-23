#!/usr/bin/env python3
"""scripts/notion_real_check.py — drive the REAL Notion API through the
actual IN-14 planner → fetcher → handler, with NO Kafka / Postgres / OAuth.

This is the fastest way to verify the Notion integration against a real
workspace. It uses a Notion **internal integration token** (the same bearer
token shape the OAuth callback stores) and exercises exactly the production
code paths:

    plan_shards_notion(ctx)            # real /v1/search enumeration
      → fetch_page_notion(...)         # real db-query / blocks / comments walk
        → handle_notion_object(...)    # the notion:object handler → ObservationDraft

Setup (one-time):
  1. https://www.notion.so/my-integrations → "New integration" (Internal).
     Copy the "Internal Integration Secret" (starts with `ntn_` or `secret_`).
  2. Open a Notion database/page → ••• menu → "Connections" → add your
     integration. Only objects you SHARE are visible to the API.

Run:
    export NOTION_TOKEN="ntn_..."          # the internal integration secret
    .venv/bin/python scripts/notion_real_check.py
    # optional: --max-rounds 200  --workspace-id my-ws

It prints the planned shards, then walks them (bounded by --max-rounds to be
gentle on the ~3 req/s rate limit) and prints each ObservationDraft the
handler would emit. Nothing is written anywhere.
"""
from __future__ import annotations

import argparse
import asyncio
import os
from uuid import uuid4

from services.ingestion.fetchers import notion as nt
from services.ingestion.fetchers.notion import fetch_page_notion
from services.ingestion.handlers.notion import handle_notion_object
from services.ingestion.planners.context import PlannerContext
from services.ingestion.planners.notion import plan_shards_notion
from services.integrations.notion.client import NotionClient
from services.ingestion.normalizer.channel_mapping import resolve_channel


class _Install:
    """Minimal stand-in for a provider_installations row."""

    def __init__(self, workspace_id: str) -> None:
        self._d = {
            "installation_id": workspace_id,
            "tenant_id": uuid4(),
            "id": uuid4(),
            "secret_ref": None,
        }

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-rounds", type=int, default=100,
                    help="cap fetcher invocations (rate-limit friendly)")
    ap.add_argument("--workspace-id", default="real-workspace")
    args = ap.parse_args()

    token = os.environ.get("NOTION_TOKEN")
    if not token:
        print("ERROR: set NOTION_TOKEN to your Notion internal integration secret.")
        return 2

    install = _Install(args.workspace_id)
    client = NotionClient(bot_token=token)

    # Make the fetcher use OUR client (production opens it from the secret
    # store; here we inject the internal-integration token directly).
    async def _opener(_inst):
        async def _noop():
            return None
        return client, _noop
    nt._open_notion_client = _opener  # type: ignore[assignment]

    handler_channel = resolve_channel("notion", "backfill")
    print(f"routing: (notion, backfill) -> {handler_channel}\n")

    try:
        # 1. Plan.
        ctx = PlannerContext(
            tenant_id=install["tenant_id"], install=install,
            conn=None, source_client=client,
        )
        shards = await plan_shards_notion(ctx)
        print(f"PLANNED {len(shards)} shard(s):")
        for s in shards:
            print(f"  - {s.shard_kind} {s.shard_identifier} recency={s.recency_score:.3f}")
        print()

        # 2 + 3. Walk + handle.
        counts = {"page": 0, "block": 0, "comment": 0}
        rounds = 0
        for shard in shards:
            cursor = None
            while rounds < args.max_rounds:
                rounds += 1
                result = await fetch_page_notion(install, shard.shard_identifier, cursor)
                for record in result.records:
                    draft = await handle_notion_object(record, {})
                    ot = draft.content.get("object_type", "?")
                    counts[ot] = counts.get(ot, 0) + 1
                    print(
                        f"[{ot:7}] {draft.external_id:28} kind={draft.kind:13} "
                        f"trust={draft.trust_tier:15} | {draft.content_text[:70]}"
                    )
                cursor = result.next_cursor
                if result.end_of_data:
                    break

        print(f"\nDONE in {rounds} fetch round(s). Observation drafts by type: {counts}")
        if sum(counts.values()) == 0:
            print(
                "\nNOTE: 0 objects. Did you SHARE a database/page with the "
                "integration? (Notion only exposes objects you connect.)"
            )
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
