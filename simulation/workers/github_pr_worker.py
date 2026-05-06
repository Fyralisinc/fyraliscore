"""GitHub PR event worker.

Emits a SyntheticSignal that resembles a GitHub PR webhook event
(opened, commits_pushed, reviewed, merged).

Example:
    python simulation/workers/github_pr_worker.py \\
        --persona alice --event merged \\
        --pr "refactor billing service" --repo payments --number 847

The source_channel is `github:<repo>` so the ingestion bypass
registers one passthrough handler per repo — matches how real
webhooks fan out.
"""
from __future__ import annotations

# Path shim so `python simulation/workers/foo.py` works. No-op when
# invoked as `python -m simulation.workers.foo`.
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
    "opened": "{actor} opened PR #{number} '{pr}' on {repo}",
    "commits_pushed": "{actor} pushed commits to PR #{number} '{pr}' on {repo}",
    "reviewed": "{actor} reviewed PR #{number} '{pr}' on {repo} — {review_state}",
    "merged": "{actor} merged PR #{number} '{pr}' into {repo}@{base_branch}",
    "closed": "{actor} closed PR #{number} '{pr}' on {repo} without merging",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GitHub PR event worker")
    parser.add_argument("--persona", required=True, help="Persona handle or name.")
    parser.add_argument(
        "--event",
        required=True,
        choices=sorted(_EVENT_TEMPLATES.keys()),
        help="PR event kind.",
    )
    parser.add_argument("--pr", required=True, help="PR title.")
    parser.add_argument("--repo", required=True, help="Repo name (no org).")
    parser.add_argument("--number", type=int, default=1, help="PR number.")
    parser.add_argument("--base-branch", default="main", help="Target branch on merge.")
    parser.add_argument(
        "--review-state",
        default="approved",
        choices=["approved", "changes_requested", "commented"],
        help="Review state when --event=reviewed.",
    )
    parser.add_argument(
        "--body",
        default="",
        help="Optional PR body / commit message (appended to content_text).",
    )
    add_common_args(parser)
    return parser.parse_args()


async def _main(args: argparse.Namespace) -> None:
    persona = get_persona(args.persona)
    content_text = _EVENT_TEMPLATES[args.event].format(
        actor=persona.name.split()[0],
        number=args.number,
        pr=args.pr,
        repo=args.repo,
        base_branch=args.base_branch,
        review_state=args.review_state,
    )
    if args.body:
        content_text = f"{content_text}\n\n{args.body}"

    content = {
        "event_kind": f"pr_{args.event}",
        "repo": args.repo,
        "pr_number": args.number,
        "title": args.pr,
        "base_branch": args.base_branch,
        "review_state": args.review_state if args.event == "reviewed" else None,
    }
    external_id = f"gh-pr-{args.repo}-{args.number}-{args.event}"

    async with with_context(args.tenant_id, args.run_id) as ctx:
        obs_id = await emit_signal(
            ctx,
            source_channel=f"github:{args.repo}",
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
