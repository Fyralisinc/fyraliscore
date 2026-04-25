"""Email event worker.

Emits a SyntheticSignal for an inbound or outbound email thread.

    python simulation/workers/email_worker.py --direction inbound \\
        --persona tomas --to rachin --subject "Acme renewal update" \\
        --body "Quick note — they want to push to Q3."
"""
from __future__ import annotations

import pathlib as _pl, sys as _sys
_ROOT = _pl.Path(__file__).resolve().parents[2]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

import argparse

from simulation.personas import get_persona
from simulation.workers._common import (
    add_common_args,
    emit_signal,
    parse_occurred_at,
    print_emitted,
    run,
    with_context,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Email event worker")
    parser.add_argument("--persona", required=True, help="Sender persona handle.")
    parser.add_argument(
        "--direction",
        choices=["inbound", "outbound"],
        default="outbound",
        help="Inbound = the persona is external-facing; outbound = internal author.",
    )
    parser.add_argument("--to", required=True, help="Recipient handle or address (comma-separated).")
    parser.add_argument("--subject", required=True)
    parser.add_argument("--body", default="")
    parser.add_argument("--thread-id", default=None, help="Thread id for replies.")
    add_common_args(parser)
    return parser.parse_args()


async def _main(args: argparse.Namespace) -> None:
    persona = get_persona(args.persona)
    subject = args.subject.strip()
    body = args.body.strip()
    content_text = f"From {persona.name} — Subject: {subject}\n\n{body}"
    content = {
        "direction": args.direction,
        "from": persona.email,
        "to": [x.strip() for x in args.to.split(",") if x.strip()],
        "subject": subject,
        "body": body,
        "thread_id": args.thread_id,
    }
    channel = "email:inbound" if args.direction == "inbound" else "email:outbound"
    external_id = args.thread_id or f"email-{persona.slack_handle}-{args.occurred_at}-{subject[:20]}"
    async with with_context(args.tenant_id, args.run_id) as ctx:
        obs_id = await emit_signal(
            ctx,
            source_channel=channel,
            source_actor_ref=persona.email_ref,
            content_text=content_text,
            content=content,
            occurred_at=parse_occurred_at(args.occurred_at),
            external_id=external_id,
            scenario_id=args.scenario_id,
        )
        print_emitted(obs_id, f"email {args.direction}: {subject}")


def main() -> None:
    run(_main(_parse_args()))


if __name__ == "__main__":
    main()
