#!/usr/bin/env python
"""scripts/register_extension_client.py — manage extension OAuth2 clients (M1).

Register / rotate / revoke the client_credentials an extension uses to authenticate
to the host (`extension_oauth_clients`, migration 0128). Generated plaintext
secrets are written once to an operator-chosen 0600 file and never printed.

    DATABASE_URL=... python scripts/register_extension_client.py register \
        --extension-id github_intel --env sandbox --created-by ops \
        --callback-url https://ext.example.com/fyralis/webhook \
        --secret-output-file /secure/path/github-intel-client.json
    ... python scripts/register_extension_client.py rotate  --client-id ext_... \
        --secret-output-file /secure/path/github-intel-client-rotated.json
    ... python scripts/register_extension_client.py revoke  --client-id ext_...
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from typing import Any

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.platform.extensions.identity import ExtensionOAuthClientsRepo


class ExtensionClientCliError(ValueError):
    """Operator-facing validation error."""


def _write_secret_file(
    path: pathlib.Path,
    payload: dict[str, Any],
    *,
    overwrite: bool = False,
) -> None:
    flags = os.O_WRONLY | os.O_CREAT
    if not overwrite:
        flags |= os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
            handle.write("\n")
        os.chmod(path, 0o600)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def _secret_output_path(args: argparse.Namespace) -> pathlib.Path:
    raw = getattr(args, "secret_output_file", None)
    if not raw:
        raise ExtensionClientCliError(
            "--secret-output-file is required for register/rotate"
        )
    path = pathlib.Path(raw)
    parent = path.parent
    if parent and not parent.exists():
        raise ExtensionClientCliError(
            f"secret output directory does not exist: {parent}"
        )
    if path.exists() and not getattr(args, "overwrite_secret_file", False):
        raise ExtensionClientCliError(
            f"secret output file already exists: {path}"
        )
    return path


async def _run(args: argparse.Namespace) -> int:
    secret_path: pathlib.Path | None = None
    if args.cmd in {"register", "rotate"}:
        secret_path = _secret_output_path(args)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set")
        return 2
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        repo = ExtensionOAuthClientsRepo(pool)
        if args.cmd == "register":
            client = await repo.register(
                extension_id=args.extension_id, created_by=args.created_by,
                environment=args.env, display_name=args.display_name,
                callback_url=args.callback_url,
            )
            assert secret_path is not None
            _write_secret_file(
                secret_path,
                {
                    "client_id": client.client_id,
                    "client_secret": client.client_secret,
                    "extension_id": client.extension_id,
                    "environment": client.environment,
                    "webhook_secret": client.webhook_secret,
                },
                overwrite=args.overwrite_secret_file,
            )
            print(f"client_id     = {client.client_id}")
            print(f"extension_id  = {client.extension_id}  env={client.environment}")
            print(f"secret_file   = {secret_path}")
        elif args.cmd == "rotate":
            secret = await repo.rotate_secret(args.client_id)
            if secret:
                assert secret_path is not None
                _write_secret_file(
                    secret_path,
                    {
                        "client_id": args.client_id,
                        "client_secret": secret,
                    },
                    overwrite=args.overwrite_secret_file,
                )
                print(f"secret_file   = {secret_path}")
            else:
                print("no such active client")
            return 0 if secret else 1
        elif args.cmd == "revoke":
            ok = await repo.revoke(args.client_id)
            print("revoked" if ok else "no such active client")
            return 0 if ok else 1
    finally:
        await pool.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    reg = sub.add_parser("register")
    reg.add_argument("--extension-id", required=True)
    reg.add_argument("--created-by", required=True)
    reg.add_argument("--env", default="production", choices=["sandbox", "production"])
    reg.add_argument("--display-name", default=None)
    reg.add_argument("--callback-url", default=None)
    _add_secret_output_args(reg)
    rotate = sub.add_parser("rotate")
    rotate.add_argument("--client-id", required=True)
    _add_secret_output_args(rotate)
    revoke = sub.add_parser("revoke")
    revoke.add_argument("--client-id", required=True)
    try:
        return asyncio.run(_run(ap.parse_args()))
    except ExtensionClientCliError as exc:
        print(str(exc), file=sys.stderr)
        return 2


def _add_secret_output_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--secret-output-file",
        help="Path to write generated plaintext secret material as JSON, mode 0600.",
    )
    parser.add_argument(
        "--overwrite-secret-file",
        action="store_true",
        help="Overwrite --secret-output-file if it already exists.",
    )


if __name__ == "__main__":
    raise SystemExit(main())
