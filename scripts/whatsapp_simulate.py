#!/usr/bin/env python3
"""Local WhatsApp webhook simulator — see live ingestion without Meta/ngrok.

Builds Meta-shaped Cloud API webhook payloads, signs them with your app secret
exactly as Meta would (HMAC-SHA256 → X-Hub-Signature-256), and POSTs them to the
running server (scripts/whatsapp_live_server.py or the full gateway). Pure stdlib.

Examples (server on :8000):
    # 1) register an installation (tenant + creds → routing table)
    python scripts/whatsapp_simulate.py register \\
        --tenant-id 00000000-0000-0000-0000-000000000001 \\
        --phone-number-id 123456789 --app-secret s3cr3t --verify-token vtok

    # 2) send an inbound customer message  → lands as a whatsapp:message observation
    python scripts/whatsapp_simulate.py send \\
        --phone-number-id 123456789 --app-secret s3cr3t \\
        --from 14155550123 --name "Alice Buyer" --text "Do you have this in size M?"

    # 3) send a delivery status for an outbound message → whatsapp:status state_change
    python scripts/whatsapp_simulate.py status \\
        --phone-number-id 123456789 --app-secret s3cr3t \\
        --to 14155550123 --state delivered

    # 4) list recent WhatsApp observations for a tenant
    python scripts/whatsapp_simulate.py recent \\
        --tenant-id 00000000-0000-0000-0000-000000000001
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
import uuid


def _sign(app_secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(app_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _post(url: str, body: bytes, headers: dict[str, str]) -> tuple[int, str]:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")


def _get(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")


def _pretty(label: str, status: int, text: str) -> None:
    print(f"\n{label} → HTTP {status}")
    try:
        print(json.dumps(json.loads(text), indent=2))
    except json.JSONDecodeError:
        print(text)


def _value_envelope(pid: str, display: str, inner: dict) -> dict:
    value = {"messaging_product": "whatsapp",
             "metadata": {"display_phone_number": display, "phone_number_id": pid}}
    value.update(inner)
    return {"object": "whatsapp_business_account",
            "entry": [{"id": "WABA_SIM", "changes": [{"field": "messages", "value": value}]}]}


def cmd_register(a: argparse.Namespace) -> None:
    body = json.dumps({
        "tenant_id": a.tenant_id,
        "phone_number_id": a.phone_number_id,
        "app_secret": a.app_secret,
        "verify_token": a.verify_token,
        "display_phone_number": a.display,
    }).encode("utf-8")
    status, text = _post(f"{a.base_url}/debug/whatsapp/register", body,
                         {"Content-Type": "application/json"})
    _pretty("register", status, text)


def cmd_send(a: argparse.Namespace) -> None:
    wamid = a.wamid or f"wamid.SIM{uuid.uuid4().hex[:18]}"
    msg = {"from": a.sender, "id": wamid, "timestamp": str(int(time.time())),
           "type": "text", "text": {"body": a.text}}
    payload = _value_envelope(a.phone_number_id, a.display, {
        "contacts": [{"profile": {"name": a.name}, "wa_id": a.sender}],
        "messages": [msg],
    })
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if not a.no_sign:
        headers["X-Hub-Signature-256"] = _sign(a.app_secret, body)
    status, text = _post(f"{a.base_url}/integrations/whatsapp/webhook", body, headers)
    _pretty(f"send message (wamid={wamid})", status, text)


def cmd_status(a: argparse.Namespace) -> None:
    wamid = a.wamid or f"wamid.SIM{uuid.uuid4().hex[:18]}"
    payload = _value_envelope(a.phone_number_id, a.display, {
        "statuses": [{"id": wamid, "status": a.state, "timestamp": str(int(time.time())),
                      "recipient_id": a.to}],
    })
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if not a.no_sign:
        headers["X-Hub-Signature-256"] = _sign(a.app_secret, body)
    status, text = _post(f"{a.base_url}/integrations/whatsapp/webhook", body, headers)
    _pretty(f"send status ({a.state}, wamid={wamid})", status, text)


def cmd_recent(a: argparse.Namespace) -> None:
    status, text = _get(f"{a.base_url}/debug/whatsapp/recent?tenant_id={a.tenant_id}")
    _pretty("recent", status, text)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-url", default="http://localhost:8000")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("register", help="upsert a whatsapp_installations row")
    pr.add_argument("--tenant-id", required=True)
    pr.add_argument("--phone-number-id", required=True)
    pr.add_argument("--app-secret", required=True)
    pr.add_argument("--verify-token", default="vtok")
    pr.add_argument("--display", default="15551230000")
    pr.set_defaults(func=cmd_register)

    ps = sub.add_parser("send", help="send an inbound customer text message")
    ps.add_argument("--phone-number-id", required=True)
    ps.add_argument("--app-secret", required=True)
    ps.add_argument("--from", dest="sender", required=True, help="customer wa_id (digits, no +)")
    ps.add_argument("--name", default="Sim Customer")
    ps.add_argument("--text", default="Hello from the WhatsApp simulator")
    ps.add_argument("--wamid", default=None)
    ps.add_argument("--display", default="15551230000")
    ps.add_argument("--no-sign", action="store_true", help="omit the signature header")
    ps.set_defaults(func=cmd_send)

    pst = sub.add_parser("status", help="send a delivery-status callback")
    pst.add_argument("--phone-number-id", required=True)
    pst.add_argument("--app-secret", required=True)
    pst.add_argument("--to", required=True, help="recipient wa_id")
    pst.add_argument("--state", default="delivered",
                     choices=["sent", "delivered", "read", "failed"])
    pst.add_argument("--wamid", default=None)
    pst.add_argument("--display", default="15551230000")
    pst.add_argument("--no-sign", action="store_true")
    pst.set_defaults(func=cmd_status)

    prc = sub.add_parser("recent", help="list recent WhatsApp observations")
    prc.add_argument("--tenant-id", required=True)
    prc.set_defaults(func=cmd_recent)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
