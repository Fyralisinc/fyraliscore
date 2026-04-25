"""scripts/capture_scenario_home.py — capture home payload + WS frames.

Invoked once per scenario (acme_tuesday, quiet_week, two_fires) after
`simulation.scenarios.replay` + Think drain. Spins up the Gateway app
in-process (via `services.gateway.main.build_app` + ASGI transport),
forces a fresh `/view/ceo/force-refresh` to fill `view_ceo_cache`
against the live substrate, then captures:

  1. `GET /view/ceo/home` → `tests/integration/captures/<scenario>_home.json`
  2. The WS `/view/ceo/stream` frames produced by the force-refresh
     → `tests/integration/captures/<scenario>_ws_frames.jsonl`
  3. Optionally one `POST /view/ceo/ask` turn for the greeting-tied
     query and saves the response HTML to `<scenario>_ask_turn.html`.

Designed as a command-line tool so:
  source .venv/bin/activate
  export $(cat .env | xargs)
  export COMPANY_OS_ENV=dev
  python scripts/capture_scenario_home.py acme_tuesday

Has no side-effects on the substrate; reads from live `view_ceo_cache`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_TENANT = UUID("00000000-0000-7000-8000-000000000dd1")
CAPTURES_DIR = ROOT / "tests" / "integration" / "captures"


async def _capture(scenario: str, ask_query: str | None) -> int:
    import httpx
    import websockets
    import uvicorn

    os.environ.setdefault("COMPANY_OS_ENV", "dev")
    os.environ.setdefault("DEFAULT_TENANT_ID", str(DEFAULT_TENANT))
    os.environ.setdefault("VIEW_CEO_TOKEN", "ceo-dogfood-token")
    os.environ.setdefault("GRT_RENDERING_BASE_URL", "http://127.0.0.1:8000")
    os.environ.setdefault("QUERY_RENDERING_BASE_URL", "http://127.0.0.1:8000")
    os.environ.setdefault("QUERY_CACHE_BACKEND", "pg")
    os.environ.setdefault("LLM_PROVIDER", "deepseek")
    os.environ.setdefault("LLM_MODEL", "deepseek-chat")

    from services.gateway.main import build_app

    app = build_app()
    # Boot uvicorn on a local port so WS can connect via real ws://. We
    # can't use ASGITransport for WS; we need a running port.
    port = int(os.environ.get("CAPTURE_PORT", "8123"))
    cfg = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="warning",
    )
    server = uvicorn.Server(cfg)
    # Disable uvicorn's signal handlers so we can stop cleanly.
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]

    server_task = asyncio.create_task(server.serve())
    # Wait for server up.
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.1)

    base = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}/view/ceo/stream?token={os.environ['VIEW_CEO_TOKEN']}"
    frames: list[dict[str, Any]] = []

    async def ws_listener() -> None:
        try:
            async with websockets.connect(ws_url, open_timeout=10) as ws:
                # Collect frames for up to ~25s; stop once we've seen
                # all four *_updated frames after the force-refresh.
                end_kinds = {
                    "greeting_updated", "cards_updated",
                    "query_grid_updated", "status_updated",
                }
                seen: set[str] = set()
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        try:
                            parsed = json.loads(msg)
                        except json.JSONDecodeError:
                            parsed = {"type": "raw", "body": msg}
                        frames.append(parsed)
                        t = parsed.get("type")
                        if t in end_kinds:
                            seen.add(t)
                        if seen >= end_kinds:
                            # Let one more frame slip if present.
                            try:
                                msg2 = await asyncio.wait_for(ws.recv(), timeout=2)
                                try:
                                    frames.append(json.loads(msg2))
                                except json.JSONDecodeError:
                                    pass
                            except asyncio.TimeoutError:
                                pass
                            return
                except asyncio.TimeoutError:
                    return
        except Exception as exc:  # noqa: BLE001
            print(f"[capture] ws error: {exc}", file=sys.stderr)

    listener_task = asyncio.create_task(ws_listener())
    # Give WS time to connect before we trigger refresh.
    await asyncio.sleep(0.5)

    async with httpx.AsyncClient(base_url=base, timeout=120) as client:
        # Force a fresh refresh so every render kind is exercised.
        r = await client.post("/view/ceo/force-refresh")
        r.raise_for_status()
        print(f"[capture] force-refresh status={r.status_code}", flush=True)

        # Wait for WS listener (or time out).
        try:
            await asyncio.wait_for(listener_task, timeout=180)
        except asyncio.TimeoutError:
            listener_task.cancel()

        r = await client.get("/view/ceo/home")
        r.raise_for_status()
        home = r.json()

        ask_payload: dict[str, Any] | None = None
        if ask_query:
            r = await client.post(
                "/view/ceo/ask",
                json={"query": ask_query},
                headers={"x-tenant-id": str(DEFAULT_TENANT)},
            )
            r.raise_for_status()
            ask_payload = r.json()

    CAPTURES_DIR.mkdir(parents=True, exist_ok=True)
    home_path = CAPTURES_DIR / f"{scenario}_home.json"
    home_path.write_text(json.dumps(home, indent=2, sort_keys=True))
    ws_path = CAPTURES_DIR / f"{scenario}_ws_frames.jsonl"
    ws_path.write_text("\n".join(json.dumps(f) for f in frames))
    if ask_payload is not None:
        ask_path = CAPTURES_DIR / f"{scenario}_ask_turn.json"
        ask_path.write_text(json.dumps(ask_payload, indent=2, sort_keys=True))

    print(f"[capture] wrote {home_path}")
    print(f"[capture] wrote {ws_path} ({len(frames)} frames)")
    if ask_payload is not None:
        print(f"[capture] wrote {ask_path}")

    server.should_exit = True
    try:
        await asyncio.wait_for(server_task, timeout=10)
    except asyncio.TimeoutError:
        server_task.cancel()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture home payload + WS frames.")
    ap.add_argument("scenario", help="Scenario name (acme_tuesday, quiet_week, two_fires)")
    ap.add_argument(
        "--ask",
        default=None,
        help="Optional query to exercise POST /view/ceo/ask and capture the turn.",
    )
    args = ap.parse_args()
    return asyncio.run(_capture(args.scenario, args.ask))


if __name__ == "__main__":
    sys.exit(main())
