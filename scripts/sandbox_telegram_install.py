"""scripts/sandbox_telegram_install.py — interactive real-API Telegram install.

Telegram has no OAuth/HTTP install endpoint (unlike the other five sandbox
sources): the durable credential is a persisted Telethon ``StringSession`` minted
by an interactive MTProto login. This is that login, wrapped so it lands a real
``telegram_installations`` row (+ dialogs + onboarding trigger) the way the
production ``finalize_install`` primitive expects.

Run it INSIDE the gateway container so it shares the stack's DATABASE_URL +
MASTER_KEK (the encrypted session must be readable by the in-container workers)
and has Telethon (the image ships the ``[telegram]`` extra):

    docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \
        exec -it gateway python scripts/sandbox_telegram_install.py \
        --account-label my-tg

You will be prompted for: phone number → the login code Telegram sends →
(if set) your 2FA password. Then it lists your dialogs (chats/groups/channels)
so you can choose which to ingest, stores the session(s) encrypted, and writes
the install. After it finishes, restart the live worker so it picks up the new
install:

    docker compose ... restart telegram_gateway_worker

The backfill chain fires automatically from the onboarding trigger this writes.

Credentials (get them once at https://my.telegram.org → API development tools):
  TELEGRAM_API_ID / TELEGRAM_API_HASH  (or pass --api-id / --api-hash)

Topology B (ADR-0003): by default ONE session is reused for both the live and
backfill auth refs — fine for a smoke test. Pass --separate-backfill-session to
do a SECOND login and mint a distinct backfill auth_key (Telegram allows only
one live connection per auth_key, so this avoids reconnect churn when backfill
and live overlap under sustained load).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from uuid import UUID

import asyncpg


def _err(msg: str) -> None:
    print(f"\n[telegram-install] ERROR: {msg}\n", file=sys.stderr)


async def _login(api_id: int, api_hash: str, *, phone: str | None, label: str):
    """Interactive Telethon login → (StringSession string, connected client)."""
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except ImportError:  # pragma: no cover - optional dep
        _err("telethon is not installed. Run inside the gateway container "
             "(the image ships the [telegram] extra), or "
             "`pip install 'fyraliscore[telegram]'` locally.")
        raise SystemExit(2)

    client = TelegramClient(StringSession(), api_id, api_hash)
    print(f"\n[telegram-install] logging in ({label}) — you'll be prompted for "
          "the code Telegram sends you (and your 2FA password if set)…")
    # Telethon's start() drives the interactive prompts (code via input(),
    # password via getpass) inside this coroutine.
    await client.start(phone=phone or (lambda: input("phone (e.g. +14155550100): ")))
    me = await client.get_me()
    who = getattr(me, "username", None) or getattr(me, "first_name", None) or me.id
    print(f"[telegram-install] authorized as {who}")
    return client.session.save(), client


async def _enumerate_dialogs(client, *, limit: int) -> list[dict]:
    from telethon.tl import types as t

    rows: list[dict] = []
    async for d in client.iter_dialogs(limit=limit if limit > 0 else None):
        ent = d.entity
        if isinstance(ent, t.Channel):
            kind, did, ah = "channel", ent.id, ent.access_hash
        elif isinstance(ent, t.User):
            kind, did, ah = "user", ent.id, ent.access_hash
        elif isinstance(ent, t.Chat):
            kind, did, ah = "chat", ent.id, None
        else:
            continue
        rows.append({
            "dialog_id": int(did),
            "dialog_kind": kind,
            "access_hash": int(ah) if ah is not None else None,
            "title": (d.name or str(did))[:120],
        })
    return rows


def _select(dialogs: list[dict], *, dialog_ids: str | None, take_all: bool) -> list[dict]:
    if dialog_ids:
        wanted = {int(x) for x in dialog_ids.replace(" ", "").split(",") if x}
        return [d for d in dialogs if d["dialog_id"] in wanted]
    if take_all:
        return dialogs
    print("\n[telegram-install] dialogs available to ingest:")
    for i, d in enumerate(dialogs):
        print(f"  [{i:>2}] {d['dialog_kind']:<8} {d['dialog_id']:<14} {d['title']}")
    raw = input(
        "\nselect dialogs to ingest — comma-separated numbers, or 'all' "
        "(empty = cancel): ").strip()
    if not raw:
        return []
    if raw.lower() == "all":
        return dialogs
    idx = {int(x) for x in raw.replace(" ", "").split(",") if x.isdigit()}
    return [d for i, d in enumerate(dialogs) if i in idx]


async def _amain() -> int:
    p = argparse.ArgumentParser(description="Interactive real Telegram install for the sandbox.")
    p.add_argument("--account-label", required=True,
                   help="unique label for this account install (e.g. 'my-tg').")
    p.add_argument("--tenant", default=os.environ.get(
        "COMPANY_OS_TENANT_ID", "00000000-0000-0000-0000-000000000001"))
    p.add_argument("--api-id", default=os.environ.get("TELEGRAM_API_ID"))
    p.add_argument("--api-hash", default=os.environ.get("TELEGRAM_API_HASH"))
    p.add_argument("--phone", default=None, help="phone in +E.164 (else prompted).")
    p.add_argument("--max-dialogs", type=int, default=50,
                   help="cap dialogs listed/enumerated (0 = no cap).")
    p.add_argument("--dialog-ids", default=None,
                   help="comma-separated dialog_ids to ingest (skip interactive pick).")
    p.add_argument("--all-dialogs", action="store_true",
                   help="ingest every enumerated dialog (skip interactive pick).")
    p.add_argument("--separate-backfill-session", action="store_true",
                   help="do a SECOND login to mint a distinct backfill auth_key (Topology B).")
    args = p.parse_args()

    if not args.api_id or not args.api_hash:
        _err("api_id/api_hash missing. Set TELEGRAM_API_ID / TELEGRAM_API_HASH "
             "(from https://my.telegram.org) or pass --api-id/--api-hash.")
        return 2
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        _err("DATABASE_URL is not set (run inside the gateway container).")
        return 2
    try:
        tenant_id = UUID(str(args.tenant))
    except ValueError:
        _err(f"--tenant is not a UUID: {args.tenant!r}")
        return 2
    api_id = int(args.api_id)

    # 1) Interactive login(s).
    live_session, client = await _login(
        api_id, args.api_hash, phone=args.phone, label="live")
    try:
        dialogs_all = await _enumerate_dialogs(client, limit=args.max_dialogs)
    finally:
        await client.disconnect()
    if not dialogs_all:
        _err("no dialogs found on this account.")
        return 1
    selected = _select(dialogs_all, dialog_ids=args.dialog_ids, take_all=args.all_dialogs)
    if not selected:
        _err("no dialogs selected — nothing to install.")
        return 1
    print(f"[telegram-install] {len(selected)} dialog(s) selected.")

    backfill_session = live_session
    if args.separate_backfill_session:
        backfill_session, client2 = await _login(
            api_id, args.api_hash, phone=args.phone, label="backfill")
        await client2.disconnect()

    # 2) Store secrets encrypted, then write the install via the production primitive.
    from lib.shared.secrets import build_secret_store
    from services.ingest.integrations.telegram.onboarding import finalize_install

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    try:
        store = build_secret_store(pool)
        lbl = args.account_label
        api_hash_ref = await store.put(
            args.api_hash, label=f"telegram_api_hash:{lbl}", tenant_id=tenant_id)
        live_ref = await store.put(
            live_session, label=f"telegram_session:{lbl}", tenant_id=tenant_id)
        backfill_ref = (
            live_ref if backfill_session == live_session
            else await store.put(
                backfill_session, label=f"telegram_backfill_session:{lbl}",
                tenant_id=tenant_id))

        install_id = await finalize_install(
            pool,
            tenant_id=tenant_id,
            account_label=lbl,
            dialogs=selected,
            api_id=str(api_id),
            api_hash_secret_ref=api_hash_ref,
            session_secret_ref=live_ref,
            backfill_session_secret_ref=backfill_ref,
        )
    finally:
        await pool.close()

    print(
        "\n[telegram-install] ✅ install written\n"
        f"    telegram_installations.id = {install_id}\n"
        f"    tenant                    = {tenant_id}\n"
        f"    account_label             = {lbl}\n"
        f"    dialogs                   = {len(selected)}\n"
        f"    session sharing           = "
        f"{'separate backfill auth_key' if backfill_ref != live_ref else 'single (reused for backfill)'}\n"
        "\nNext:\n"
        "  1) restart the live worker to pick up the new install:\n"
        "       docker compose -f docker-compose.yml -f docker-compose.sandbox.yml "
        "restart telegram_gateway_worker\n"
        "  2) backfill fires automatically from the onboarding trigger.\n"
        "  3) watch it land:  python scripts/sandbox_inspect.py  "
        "(observations source_channel = telegram:message)\n")
    return 0


def main() -> int:
    try:
        return asyncio.run(_amain())
    except KeyboardInterrupt:
        _err("cancelled.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
