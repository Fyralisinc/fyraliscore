from __future__ import annotations

import base64
import json

import httpx

from services.ingest.synthetic.provider_lab import build_provider_lab_app


def _jwt_for(subject: str) -> str:
    def _segment(value: dict[str, str]) -> str:
        raw = json.dumps(value, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    return f"{_segment({'alg': 'none'})}.{_segment({'sub': subject})}.signature"


async def test_workspace_directory_and_impersonated_drive_are_source_scoped() -> None:
    alice = "alice@acme.example"
    bob = "bob@acme.example"
    fixtures = {
        "gmail": [
            {
                "directory": {
                    "domain": "acme.example",
                    "users": [
                        {"primaryEmail": alice, "orgUnitPath": "/Engineering"},
                        {"primaryEmail": bob, "orgUnitPath": "/Sales"},
                    ],
                    "groups": [{"email": "eng@acme.example"}],
                    "group_members": {
                        "eng@acme.example": [
                            {"type": "USER", "email": alice}
                        ]
                    },
                    "org_units": [{"orgUnitPath": "/Engineering"}],
                },
                "mailboxes": {
                    alice: {
                        "history_id": "101",
                        "messages": [{"id": "mail-a"}],
                    },
                    bob: {
                        "history_id": "202",
                        "messages": [{"id": "mail-b"}],
                    },
                },
            }
        ],
        "google_drive": [
            {
                "drive_my": {
                    alice: {
                        "files": [{"id": "file-a"}],
                        "exports": {"file-a": "alice body"},
                    },
                    bob: {
                        "files": [{"id": "file-b"}],
                        "exports": {"file-b": "bob body"},
                    },
                },
                "shared_drives": [{"id": "shared-1", "name": "Shared"}],
                "drive_shared": {
                    "shared-1": {
                        "files": [{"id": "file-shared"}],
                        "exports": {"file-shared": "shared body"},
                    }
                },
            }
        ],
    }
    app = build_provider_lab_app(fixtures=fixtures)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 43124))

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
    ) as client:
        token_response = await client.post(
            "/gmail/token",
            data={"assertion": _jwt_for(alice)},
        )
        alice_token = token_response.json()["access_token"]
        alice_headers = {"Authorization": f"Bearer {alice_token}"}
        bob_headers = {"Authorization": f"Bearer lab-gmail::{bob}"}

        users = await client.get(
            "/gmail/admin/directory/v1/users",
            params={"query": "orgUnitPath=/Engineering"},
            headers=alice_headers,
        )
        members = await client.get(
            "/gmail/admin/directory/v1/groups/eng@acme.example/members",
            headers=alice_headers,
        )
        alice_files = await client.get(
            "/google_drive/files",
            headers=alice_headers,
        )
        bob_files = await client.get(
            "/google_drive/files",
            headers=bob_headers,
        )
        shared_files = await client.get(
            "/google_drive/files",
            params={"driveId": "shared-1"},
            headers=alice_headers,
        )
        shared_export = await client.get(
            "/google_drive/files/file-shared/export",
            headers=alice_headers,
        )

    assert token_response.status_code == 200
    assert users.json()["users"] == [
        {"primaryEmail": alice, "orgUnitPath": "/Engineering"}
    ]
    assert members.json()["members"] == [{"type": "USER", "email": alice}]
    assert alice_files.json()["files"] == [{"id": "file-a"}]
    assert bob_files.json()["files"] == [{"id": "file-b"}]
    assert shared_files.json()["files"] == [{"id": "file-shared"}]
    assert shared_export.text == "shared body"
    assert app.state.provider_lab.ledger.list(
        source="gmail",
        route_id="gmail.token",
        scope=alice,
    )
