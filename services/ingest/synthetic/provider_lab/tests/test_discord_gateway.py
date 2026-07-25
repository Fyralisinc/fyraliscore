from __future__ import annotations

import asyncio

import pytest
import uvicorn
from fastapi.testclient import TestClient

from services.ingest.synthetic.provider_lab import build_provider_lab_app
from services.ingest.integrations.discord.gateway.client import (
    DiscordGatewayClient,
)


def _configured_client() -> TestClient:
    app = build_provider_lab_app()
    client = TestClient(app)
    response = client.put(
        "/_lab/sources/discord/state",
        json={
            "guilds": {},
            "messages": {},
            "gateway_url": None,
            "gateway_session_id": "session-1",
            "gateway_application_id": "app-1",
            "gateway_heartbeat_interval_ms": 250,
            "gateway_events": [
                {
                    "op": 0,
                    "s": 1,
                    "t": "MESSAGE_CREATE",
                    "d": {"id": "message-1", "content": "first"},
                },
                {
                    "op": 0,
                    "s": 2,
                    "t": "MESSAGE_UPDATE",
                    "d": {"id": "message-1", "content": "edited"},
                },
            ],
        },
    )
    assert response.status_code == 200
    return client


def test_discord_gateway_identify_dispatch_and_heartbeat() -> None:
    with _configured_client() as client:
        gateway = client.get(
            "/discord/api/v10/gateway/bot",
            headers={"Authorization": "Bot lab-discord::guild-1"},
        )
        assert gateway.status_code == 200
        assert gateway.json()["url"].endswith("/discord/gateway")

        with client.websocket_connect("/discord/gateway") as websocket:
            assert websocket.receive_json() == {
                "op": 10,
                "d": {"heartbeat_interval": 250},
            }
            websocket.send_json(
                {
                    "op": 2,
                    "d": {
                        "token": "lab-discord::guild-1",
                        "intents": 33281,
                    },
                }
            )
            ready = websocket.receive_json()
            assert ready["t"] == "READY"
            assert ready["d"]["session_id"] == "session-1"
            assert websocket.receive_json()["s"] == 1
            assert websocket.receive_json()["s"] == 2
            websocket.send_json({"op": 1, "d": 2})
            assert websocket.receive_json() == {"op": 11, "d": None}


def test_discord_gateway_resume_replays_only_newer_sequences() -> None:
    with _configured_client() as client:
        with client.websocket_connect("/discord/gateway") as websocket:
            assert websocket.receive_json()["op"] == 10
            websocket.send_json(
                {
                    "op": 6,
                    "d": {
                        "token": "lab-discord::guild-1",
                        "session_id": "session-1",
                        "seq": 1,
                    },
                }
            )
            resumed = websocket.receive_json()
            assert resumed["t"] == "RESUMED"
            assert resumed["s"] == 1
            replay = websocket.receive_json()
            assert replay["s"] == 2
            assert replay["t"] == "MESSAGE_UPDATE"


def test_discord_gateway_rejects_invalid_session() -> None:
    with _configured_client() as client:
        with client.websocket_connect("/discord/gateway") as websocket:
            assert websocket.receive_json()["op"] == 10
            websocket.send_json(
                {
                    "op": 6,
                    "d": {
                        "token": "lab-discord::guild-1",
                        "session_id": "wrong-session",
                        "seq": 1,
                    },
                }
            )
            assert websocket.receive_json() == {"op": 9, "d": False}


@pytest.mark.timeout(10)
async def test_production_discord_gateway_client_runs_unmodified_against_lab(
    unused_tcp_port: int,
) -> None:
    app = build_provider_lab_app()
    app.state.provider_lab.set_source_state(
        "discord",
        {
            "guilds": {},
            "messages": {},
            "gateway_url": None,
            "gateway_session_id": "production-client-session",
            "gateway_application_id": "production-client-app",
            "gateway_heartbeat_interval_ms": 250,
            "gateway_events": [
                {
                    "op": 0,
                    "s": 1,
                    "t": "MESSAGE_CREATE",
                    "d": {"id": "message-1", "content": "from the lab"},
                }
            ],
        },
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=unused_tcp_port,
            log_level="error",
            lifespan="off",
            ws="websockets-sansio",
        )
    )
    server_task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started

    received: list[dict[str, object]] = []
    client: DiscordGatewayClient

    async def handle(frame: dict[str, object]) -> None:
        received.append(frame)
        client.request_shutdown()

    client = DiscordGatewayClient(
        bot_token="lab-discord::guild-1",
        dispatch_handler=handle,
        gateway_bot_url=(
            f"http://127.0.0.1:{unused_tcp_port}"
            "/discord/api/v10/gateway/bot"
        ),
    )
    try:
        await client.run()
    finally:
        await client.aclose()
        server.should_exit = True
        await server_task

    assert [frame["t"] for frame in received] == ["MESSAGE_CREATE"]
    assert received[0]["d"] == {
        "id": "message-1",
        "content": "from the lab",
    }
