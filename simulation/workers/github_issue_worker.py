"""GitHub issue event worker.

Emits a SyntheticSignal for issue events (opened, commented, closed).

    python simulation/workers/github_issue_worker.py \\
        --persona alice --event opened --title "billing-service OOMing" \\
        --repo payments --number 312
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
    "opened": "{actor} opened issue #{number} '{title}' on {repo}",
    "commented": "{actor} commented on issue #{number} '{title}' on {repo}",
    "closed": "{actor} closed issue #{number} '{title}' on {repo}",
    "reopened": "{actor} reopened issue #{number} '{title}' on {repo}",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GitHub issue event worker")
    parser.add_argument("--persona", required=True)
    parser.add_argument("--event", required=True, choices=sorted(_EVENT_TEMPLATES.keys()))
    parser.add_argument("--title", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--number", type=int, default=1)
    parser.add_argument("--body", default="", help="Issue body or comment text.")
    parser.add_argument(
        "--labels",
        default="",
        help="Comma-separated labels ('bug,acme,renewal').",
    )
    add_common_args(parser)
    return parser.parse_args()


async def _main(args: argparse.Namespace) -> None:
    persona = get_persona(args.persona)
    content_text = _EVENT_TEMPLATES[args.event].format(
        actor=persona.name.split()[0],
        number=args.number,
        title=args.title,
        repo=args.repo,
    )
    if args.body:
        content_text = f"{content_text}\n\n{args.body}"
    labels = [x.strip() for x in args.labels.split(",") if x.strip()]
    content = {
        "event_kind": f"issue_{args.event}",
        "repo": args.repo,
        "issue_number": args.number,
        "title": args.title,
        "labels": labels,
    }
    external_id = f"gh-issue-{args.repo}-{args.number}-{args.event}"
    async with with_context(args.tenant_id, args.run_id) as ctx:
        obs_id = await emit_signal(
            ctx,
            source_channel=f"github:issues:{args.repo}",
            source_actor_ref=persona.github_ref,
            content_text=content_text,
            content=content,
            occurred_at=parse_occurred_at(args.occurred_at),
            external_id=external_id,
            scenario_id=args.scenario_id,
        )
        print_emitted(obs_id, content_text)


def main() -> None:
    run(_main(_parse_args()))


if __name__ == "__main__":
    main()
