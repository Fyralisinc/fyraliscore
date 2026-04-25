"""Calendar event worker.

Emits a SyntheticSignal for a calendar event
(meeting_scheduled, meeting_held, meeting_cancelled).

    python simulation/workers/calendar_worker.py \\
        --persona monica --event meeting_scheduled \\
        --title "Acme renewal sync" --when +2d \\
        --attendees monica,priya,tomas
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
    "meeting_scheduled": "{actor} scheduled '{title}' for {when}",
    "meeting_held": "{title} took place at {when}",
    "meeting_cancelled": "{actor} cancelled '{title}' at {when}",
    "meeting_rescheduled": "{actor} moved '{title}' to {when}",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calendar event worker")
    parser.add_argument("--persona", required=True, help="Organizer persona.")
    parser.add_argument("--event", required=True, choices=sorted(_EVENT_TEMPLATES.keys()))
    parser.add_argument("--title", required=True)
    parser.add_argument("--when", required=True, help="Time of meeting (ISO or relative like +2d).")
    parser.add_argument(
        "--attendees",
        default="",
        help="Comma-separated persona handles of attendees.",
    )
    parser.add_argument("--notes", default="")
    add_common_args(parser)
    return parser.parse_args()


async def _main(args: argparse.Namespace) -> None:
    organizer = get_persona(args.persona)
    when_dt = parse_occurred_at(args.when)
    when_str = when_dt.isoformat() if when_dt else "unknown"
    attendees = [a.strip() for a in args.attendees.split(",") if a.strip()]
    content_text = _EVENT_TEMPLATES[args.event].format(
        actor=organizer.name.split()[0], title=args.title, when=when_str
    )
    if args.notes:
        content_text = f"{content_text}\n\n{args.notes}"
    content = {
        "event_kind": args.event,
        "title": args.title,
        "organizer": organizer.email,
        "attendees": attendees,
        "meeting_time": when_str,
        "notes": args.notes,
    }
    external_id = f"cal-{organizer.slack_handle}-{args.title[:30]}-{when_str}"
    async with with_context(args.tenant_id, args.run_id) as ctx:
        obs_id = await emit_signal(
            ctx,
            source_channel="calendar:sync",
            source_actor_ref=f"calendar:{organizer.email}",
            content_text=content_text,
            content=content,
            # The ingestion timestamp is the *event authoring* time,
            # not the meeting's start time. Both are preserved
            # (meeting_time lives in content; occurred_at is when the
            # calendar system observed the mutation).
            occurred_at=parse_occurred_at(args.occurred_at),
            external_id=external_id,
            scenario_id=args.scenario_id,
        )
        print_emitted(obs_id, f"calendar {args.event}: {args.title}")


def main() -> None:
    run(_main(_parse_args()))


if __name__ == "__main__":
    main()
