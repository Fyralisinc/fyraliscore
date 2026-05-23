"""simulation/scenarios/replay.py — scenario replayer.

Reads a YAML narrative (simulation/scenarios/<name>.yaml), computes
real timestamps from relative offsets (`-7d 15:30`, `+2h`, ...),
and feeds each event through the synthetic bypass.

CLI:
    python -m simulation.scenarios.replay acme_tuesday
    python -m simulation.scenarios.replay acme_tuesday --speed 60
    python -m simulation.scenarios.replay acme_tuesday --dry-run

Scenario YAML shape:

    name: acme_tuesday
    description: ...
    narrative:
      - t: "-7d 15:30"
        actor: alice
        channel: slack_eng           # or slack:eng, slack:#eng
        content: "text..."

      - t: "-6d 09:15"
        actor: marcus
        kind: github_pr              # or email / calendar / linear / slack
        event: opened
        title: "rate-limiter refactor"
        repo: payments
        number: 847
        body: "...optional..."

Event kinds supported (maps directly to channel workers):
- slack (default when kind omitted, or kind='slack')
- github_pr (PR events)
- github_issue (issue events)
- email (inbound/outbound)
- calendar (meeting_scheduled / _held / _cancelled)
- linear (status_change / comment / assigned)

All events are wall-time scheduled by occurred_at relative to the
scenario's anchor_time (default: now). With --speed >1, wall-clock
wait between events is divided; with --dry-run, nothing is injected.

Think T1 enqueue is left enabled by default so the substrate processes
signals as they land. Pass --skip-t1 to backfill without triggering
Think (useful when you want to prime state then kick Think manually).
"""
from __future__ import annotations

import pathlib as _pl, sys as _sys
_ROOT = _pl.Path(__file__).resolve().parents[2]
if str(_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_ROOT))

import argparse
import asyncio
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

import yaml

# Env guard fires at import.
import services.synthetic  # noqa: F401
from services.synthetic.core import SyntheticSignal, inject

from simulation.personas import get_persona
from simulation.workers._common import with_context


SCENARIOS_DIR = Path(__file__).parent


_REL_RE = re.compile(
    r"^\s*(?P<sign>[+-])?\s*"
    r"(?:(?P<days>\d+)d)?\s*"
    r"(?:(?P<hours>\d{1,2}):(?P<minutes>\d{2}))?\s*$"
)


def parse_scenario_time(
    raw: str, anchor: datetime
) -> datetime:
    """Parse a scenario 't' string.

    Supported forms:
      -7d 15:30       — 7 days before anchor, at 15:30 UTC
      +2d 09:00       — 2 days after anchor, at 09:00 UTC
      -3h             — 3 hours before anchor (relative, no clock)
      +45m            — 45 minutes after anchor
      2026-04-22T09:00:00Z  — absolute ISO-8601
      now             — anchor itself
    """
    if not raw or not isinstance(raw, str):
        raise ValueError(f"scenario t must be a string, got {raw!r}")
    s = raw.strip()
    if s == "now":
        return anchor
    # Absolute ISO
    if "T" in s or s.count("-") >= 2:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    # Relative with unit suffix (h / m / s)
    m = re.match(r"^([+-])(\d+)([smhd])$", s)
    if m:
        sign = 1 if m.group(1) == "+" else -1
        amount = int(m.group(2))
        unit = m.group(3)
        delta_s = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit] * amount
        return anchor + timedelta(seconds=sign * delta_s)
    # Day + clock form: -7d 15:30
    m = _REL_RE.match(s)
    if m:
        sign = 1 if (m.group("sign") == "+" or m.group("sign") is None) else -1
        days = int(m.group("days")) if m.group("days") else 0
        if m.group("hours") is not None:
            h = int(m.group("hours"))
            mi = int(m.group("minutes"))
        else:
            h, mi = None, None
        day_anchor = anchor + timedelta(days=sign * days)
        if h is not None:
            day_anchor = day_anchor.replace(
                hour=h, minute=mi, second=0, microsecond=0
            )
        return day_anchor
    raise ValueError(f"unable to parse scenario time {raw!r}")


@dataclass
class ScenarioEvent:
    t: datetime
    actor: str
    kind: str
    raw: dict[str, Any]


@dataclass
class Scenario:
    name: str
    description: str
    anchor: datetime
    events: list[ScenarioEvent]


def normalize_slack_channel(ch: str) -> str:
    # Accept 'slack_eng', 'slack:eng', '#eng', 'eng' — all produce
    # source_channel 'slack:eng'.
    ch = ch.strip().lstrip("#")
    if ch.startswith("slack:"):
        return ch
    if ch.startswith("slack_"):
        return f"slack:{ch[len('slack_'):]}"
    return f"slack:{ch}"


def load_scenario(path_or_name: str, anchor: Optional[datetime] = None) -> Scenario:
    if anchor is None:
        anchor = datetime.now(timezone.utc).replace(microsecond=0)
    path = Path(path_or_name)
    if not path.exists():
        # Treat as a name inside simulation/scenarios/
        candidate = SCENARIOS_DIR / f"{path_or_name}.yaml"
        if candidate.exists():
            path = candidate
        else:
            raise FileNotFoundError(f"no scenario at {path} or {candidate}")
    raw = yaml.safe_load(path.read_text())
    events: list[ScenarioEvent] = []
    for entry in raw.get("narrative") or []:
        t = parse_scenario_time(entry["t"], anchor)
        actor = entry.get("actor") or entry.get("persona")
        if not actor:
            raise ValueError(f"event missing 'actor': {entry}")
        kind = entry.get("kind", "slack")
        events.append(ScenarioEvent(t=t, actor=actor, kind=kind, raw=entry))
    events.sort(key=lambda e: e.t)
    return Scenario(
        name=raw.get("name") or path.stem,
        description=raw.get("description") or "",
        anchor=anchor,
        events=events,
    )


# ------------------------------------------------------------------
# Per-kind emitters. Each returns a tuple of (source_channel,
# source_actor_ref, content_text, content_dict, external_id).
# ------------------------------------------------------------------


def _emit_slack(entry: dict[str, Any], actor, occurred_at: datetime) -> dict:
    channel = entry.get("channel")
    if not channel:
        raise ValueError(f"slack event missing channel: {entry}")
    source_channel = normalize_slack_channel(channel)
    handle = source_channel.split(":", 1)[1]
    text = entry.get("content") or entry.get("text") or entry.get("body") or ""
    if not text.strip():
        raise ValueError(f"slack event has empty content: {entry}")
    return {
        "source_channel": source_channel,
        "source_actor_ref": actor.slack_ref,
        "content_text": text,
        "content": {
            "event_kind": "message",
            "channel_name": handle,
            "author_slack_handle": actor.slack_handle,
            "ts": occurred_at.isoformat(),
        },
        "external_id": f"sc-slack-{handle}-{actor.slack_handle}-{occurred_at.isoformat()}",
    }


def _emit_github_pr(entry: dict[str, Any], actor, occurred_at: datetime) -> dict:
    repo = entry["repo"]
    number = entry.get("number", 1)
    event = entry.get("event", "opened")
    title = entry.get("title", entry.get("pr", "unnamed PR"))
    text = f"{actor.name.split()[0]} {event} PR #{number} '{title}' on {repo}"
    if entry.get("body"):
        text += "\n\n" + entry["body"]
    return {
        "source_channel": f"github:{repo}",
        "source_actor_ref": actor.github_ref,
        "content_text": text,
        "content": {
            "event_kind": f"pr_{event}",
            "repo": repo,
            "pr_number": number,
            "title": title,
        },
        "external_id": f"sc-gh-pr-{repo}-{number}-{event}",
    }


def _emit_github_issue(entry: dict[str, Any], actor, occurred_at: datetime) -> dict:
    repo = entry["repo"]
    number = entry.get("number", 1)
    event = entry.get("event", "opened")
    title = entry.get("title", "unnamed issue")
    text = f"{actor.name.split()[0]} {event} issue #{number} '{title}' on {repo}"
    if entry.get("body"):
        text += "\n\n" + entry["body"]
    return {
        "source_channel": f"github:issues:{repo}",
        "source_actor_ref": actor.github_ref,
        "content_text": text,
        "content": {
            "event_kind": f"issue_{event}",
            "repo": repo,
            "issue_number": number,
            "title": title,
            "labels": entry.get("labels") or [],
        },
        "external_id": f"sc-gh-issue-{repo}-{number}-{event}",
    }


def _emit_email(entry: dict[str, Any], actor, occurred_at: datetime) -> dict:
    subject = entry.get("subject", "(no subject)")
    body = entry.get("body", "")
    direction = entry.get("direction", "outbound")
    channel = "email:inbound" if direction == "inbound" else "email:outbound"
    text = f"From {actor.name} — Subject: {subject}\n\n{body}"
    return {
        "source_channel": channel,
        "source_actor_ref": actor.email_ref,
        "content_text": text,
        "content": {
            "direction": direction,
            "from": actor.email,
            "to": entry.get("to") or [],
            "subject": subject,
            "body": body,
            "thread_id": entry.get("thread_id"),
        },
        "external_id": f"sc-email-{actor.slack_handle}-{occurred_at.isoformat()}-{subject[:20]}",
    }


def _emit_calendar(entry: dict[str, Any], actor, occurred_at: datetime) -> dict:
    event = entry.get("event", "meeting_scheduled")
    title = entry.get("title", "(untitled)")
    when = entry.get("when") or occurred_at.isoformat()
    text = f"{actor.name.split()[0]} {event.replace('_', ' ')} '{title}' at {when}"
    if entry.get("notes"):
        text += "\n\n" + entry["notes"]
    return {
        "source_channel": "calendar:sync",
        "source_actor_ref": f"calendar:{actor.email}",
        "content_text": text,
        "content": {
            "event_kind": event,
            "title": title,
            "organizer": actor.email,
            "attendees": entry.get("attendees") or [],
            "meeting_time": when,
        },
        "external_id": f"sc-cal-{actor.slack_handle}-{title[:30]}-{event}-{when}",
    }


def _emit_linear(entry: dict[str, Any], actor, occurred_at: datetime) -> dict:
    event = entry.get("event", "status_change")
    ticket = entry["ticket"]
    title = entry.get("title", "(untitled)")
    text = (
        f"{actor.name.split()[0]} {event} {ticket} '{title}'"
        + (f" {entry['from_state']} → {entry['to_state']}" if event == "status_change" else "")
    )
    if entry.get("body"):
        text += "\n\n" + entry["body"]
    return {
        "source_channel": "linear:webhook",
        "source_actor_ref": f"linear:{actor.slack_handle}",
        "content_text": text,
        "content": {
            "event_kind": f"linear_{event}",
            "ticket": ticket,
            "title": title,
            "from_state": entry.get("from_state"),
            "to_state": entry.get("to_state"),
            "assignee_handle": entry.get("assignee"),
        },
        "external_id": f"sc-linear-{ticket}-{event}-{occurred_at.isoformat()}",
    }


_EMITTERS = {
    "slack": _emit_slack,
    "github_pr": _emit_github_pr,
    "github_issue": _emit_github_issue,
    "email": _emit_email,
    "calendar": _emit_calendar,
    "linear": _emit_linear,
}


async def run_scenario(
    scenario: Scenario,
    *,
    speed: float = 0.0,
    dry_run: bool = False,
    skip_t1: bool = False,
    verbose: bool = True,
    tenant_id_arg: Optional[str] = None,
    run_id_arg: Optional[str] = None,
) -> list[UUID]:
    """Execute a loaded scenario.

    speed=0 means "fire everything as fast as possible in ascending
    time order, no real waits". speed>0 scales real time (speed=60
    replays a 24h scenario in 24 minutes). dry_run prints intended
    signals without DB writes.
    """
    obs_ids: list[UUID] = []
    if dry_run:
        # Light-weight dry run: no DB, no actor seeding, no Ollama.
        for ev in scenario.events:
            actor = get_persona(ev.actor)
            emitter = _EMITTERS.get(ev.kind)
            if emitter is None:
                raise ValueError(f"unknown event kind {ev.kind!r}")
            draft = emitter(ev.raw, actor, ev.t)
            if verbose:
                print(
                    f"[dry] {ev.t.isoformat()} {ev.kind} "
                    f"{draft['source_channel']} — {draft['content_text'][:80]}"
                )
        return []

    async with with_context(tenant_id_arg, run_id_arg) as ctx:
        if verbose:
            print(
                f"Replaying scenario {scenario.name!r} — {len(scenario.events)} events, "
                f"tenant={ctx.tenant_id}, run={ctx.run_id}"
            )
        start_real = datetime.now(timezone.utc)
        first_t = scenario.events[0].t if scenario.events else start_real
        for ev in scenario.events:
            if speed > 0:
                # Align real clock to scenario clock at speed factor.
                offset_scenario = (ev.t - first_t).total_seconds()
                target_real_offset = offset_scenario / speed
                now_real_offset = (
                    datetime.now(timezone.utc) - start_real
                ).total_seconds()
                wait = max(0.0, target_real_offset - now_real_offset)
                if wait > 0:
                    await asyncio.sleep(wait)
            actor = get_persona(ev.actor)
            emitter = _EMITTERS.get(ev.kind)
            if emitter is None:
                raise ValueError(f"unknown event kind {ev.kind!r}")
            draft = emitter(ev.raw, actor, ev.t)
            signal = SyntheticSignal(
                source_channel=draft["source_channel"],
                source_actor_ref=draft["source_actor_ref"],
                content_text=draft["content_text"],
                content=draft["content"],
                occurred_at=ev.t,
                external_id=draft["external_id"],
                entities_hint=[],
                scenario_id=scenario.name,
                run_id=ctx.run_id,
            )
            try:
                res = await inject(
                    signal,
                    ctx.tenant_id,
                    pool=ctx.pool,
                    actor_repo=ctx.actor_repo,
                    alias_repo=ctx.alias_repo,
                    embedder=ctx.embedder,
                    skip_t1_enqueue=skip_t1,
                )
                obs_ids.append(res.observation.id)
                if verbose:
                    print(
                        f"[ok]  {ev.t.isoformat()} {ev.kind:13s} "
                        f"{draft['source_channel']:28s} "
                        f"-> obs {str(res.observation.id)[:8]}"
                    )
            except Exception as exc:
                if verbose:
                    print(
                        f"[ERR] {ev.t.isoformat()} {ev.kind} "
                        f"{draft['source_channel']}: {exc}",
                        file=sys.stderr,
                    )
                raise
    return obs_ids


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay a simulation scenario.")
    parser.add_argument("scenario", help="Scenario name or path to YAML.")
    parser.add_argument("--speed", type=float, default=0.0,
                        help="Wall-clock speed multiplier. 0 = no waits.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-t1", action="store_true",
                        help="Skip Think trigger enqueue (backfill mode).")
    parser.add_argument("--anchor", default=None,
                        help="ISO-8601 anchor time (default: now).")
    parser.add_argument("--tenant", dest="tenant_id", default=None)
    parser.add_argument("--run-id", dest="run_id", default=None)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    anchor = None
    if args.anchor:
        anchor = datetime.fromisoformat(args.anchor.replace("Z", "+00:00"))
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)
    scenario = load_scenario(args.scenario, anchor)
    asyncio.run(
        run_scenario(
            scenario,
            speed=args.speed,
            dry_run=args.dry_run,
            skip_t1=args.skip_t1,
            verbose=not args.quiet,
            tenant_id_arg=args.tenant_id,
            run_id_arg=args.run_id,
        )
    )


if __name__ == "__main__":
    main()
