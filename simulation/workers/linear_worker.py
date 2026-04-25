"""Linear ticket event worker.

Emits a SyntheticSignal for a Linear ticket transition
(status_change, comment, assigned).

    python simulation/workers/linear_worker.py \\
        --persona nora --event status_change \\
        --ticket ENG-412 --title "rate-limiter refactor" \\
        --from-state in_progress --to-state blocked
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


_EVENT_TEMPLATES = {
    "status_change": "{actor} moved {ticket} '{title}' {from_state} → {to_state}",
    "comment": "{actor} commented on {ticket} '{title}'",
    "assigned": "{actor} assigned {ticket} '{title}' to {assignee}",
    "created": "{actor} created {ticket} '{title}'",
    "closed": "{actor} closed {ticket} '{title}'",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Linear event worker")
    parser.add_argument("--persona", required=True)
    parser.add_argument("--event", required=True, choices=sorted(_EVENT_TEMPLATES.keys()))
    parser.add_argument("--ticket", required=True, help="Linear ticket id (ENG-412).")
    parser.add_argument("--title", required=True)
    parser.add_argument("--from-state", default="todo")
    parser.add_argument("--to-state", default="in_progress")
    parser.add_argument("--assignee", default="", help="Persona handle for assignment events.")
    parser.add_argument("--body", default="", help="Comment text.")
    add_common_args(parser)
    return parser.parse_args()


async def _main(args: argparse.Namespace) -> None:
    persona = get_persona(args.persona)
    assignee_name = ""
    if args.assignee:
        try:
            assignee_name = get_persona(args.assignee).name.split()[0]
        except Exception:
            assignee_name = args.assignee
    content_text = _EVENT_TEMPLATES[args.event].format(
        actor=persona.name.split()[0],
        ticket=args.ticket,
        title=args.title,
        from_state=args.from_state,
        to_state=args.to_state,
        assignee=assignee_name or "(nobody)",
    )
    if args.body:
        content_text = f"{content_text}\n\n{args.body}"
    content = {
        "event_kind": f"linear_{args.event}",
        "ticket": args.ticket,
        "title": args.title,
        "from_state": args.from_state if args.event == "status_change" else None,
        "to_state": args.to_state if args.event == "status_change" else None,
        "assignee_handle": args.assignee or None,
        "body": args.body or None,
    }
    external_id = f"linear-{args.ticket}-{args.event}-{args.occurred_at}"
    async with with_context(args.tenant_id, args.run_id) as ctx:
        obs_id = await emit_signal(
            ctx,
            source_channel="linear:webhook",
            source_actor_ref=f"linear:{persona.slack_handle}",
            content_text=content_text,
            content=content,
            occurred_at=parse_occurred_at(args.occurred_at),
            external_id=external_id,
            scenario_id=args.scenario_id,
        )
        print_emitted(obs_id, f"linear {args.event}: {args.ticket}")


def main() -> None:
    run(_main(_parse_args()))


if __name__ == "__main__":
    main()
