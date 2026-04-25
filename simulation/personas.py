"""simulation/personas.py — persona registry.

Loads simulation/personas.yaml into a typed in-memory registry.

Design:
- No DB access. The persona registry is a YAML-authored manifest; the
  DB mapping (actor_id + actor_identity_mappings rows) is materialised
  separately by `ensure_actors_seeded()` (see simulation/reset.py and
  simulation/workers/_common.py). This split keeps the persona
  metadata trivial to unit-test.
- `voice_hints_for(persona_id)` returns a short reminder string shown
  to Rachin in the Slack UI before he types. It is NEVER fed to an
  LLM — it's a UX aid so the persona's voice is present in his head
  when he composes. The substrate handles what actually gets said.
- `switch_active_persona()` mutates module-level state so CLI workers
  and the FastAPI routes can share "who am I authoring as right now".
  Concurrent callers should pass an explicit persona_id to inject
  functions; the active-persona state is a convenience, not a
  thread-safe channel.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional
from uuid import UUID

import yaml


PERSONAS_YAML = Path(__file__).parent / "personas.yaml"


@dataclass(frozen=True)
class Persona:
    id: UUID
    name: str
    role: str
    title: str
    voice_style_notes: str
    typical_channels: tuple[str, ...]
    slack_handle: Optional[str]
    github_handle: Optional[str]
    email: str

    @property
    def slack_ref(self) -> Optional[str]:
        """source_actor_ref value for Slack — e.g. 'slack:alice'.

        Shaped to match the ingestion handler's expectation
        ('<channel>:<external_ref>') per services/actors/repo.py.
        Returns None if this persona has no Slack presence.
        """
        if not self.slack_handle:
            return None
        return f"slack:{self.slack_handle}"

    @property
    def github_ref(self) -> Optional[str]:
        if not self.github_handle:
            return None
        return f"github:{self.github_handle}"

    @property
    def email_ref(self) -> str:
        return f"email:{self.email}"


_active_lock = threading.Lock()
_active_persona_id: Optional[UUID] = None


class PersonaNotFound(KeyError):
    pass


def load_personas(path: Optional[Path] = None) -> list[Persona]:
    """Load the persona registry from YAML.

    No caching — callers that want stability across a process should
    invoke `load_personas_cached()`. Tests pass an explicit `path`.
    """
    path = path or PERSONAS_YAML
    raw = yaml.safe_load(path.read_text())
    out: list[Persona] = []
    for entry in raw.get("personas", []):
        out.append(
            Persona(
                id=UUID(str(entry["id"])),
                name=entry["name"],
                role=entry["role"],
                title=entry["title"],
                voice_style_notes=entry["voice_style_notes"].strip(),
                typical_channels=tuple(entry.get("typical_channels") or []),
                slack_handle=entry.get("slack_handle") or None,
                github_handle=entry.get("github_handle") or None,
                email=entry["email"],
            )
        )
    if not out:
        raise ValueError(f"no personas loaded from {path}")
    _check_unique_ids(out)
    return out


def _check_unique_ids(personas: list[Persona]) -> None:
    seen: set[UUID] = set()
    for p in personas:
        if p.id in seen:
            raise ValueError(f"duplicate persona id {p.id} ({p.name})")
        seen.add(p.id)


@lru_cache(maxsize=1)
def load_personas_cached() -> tuple[Persona, ...]:
    """Cached convenience wrapper around `load_personas()`.

    Returns an immutable tuple so callers can't mutate the registry.
    Clear with `load_personas_cached.cache_clear()` if the YAML is
    edited during a running process.
    """
    return tuple(load_personas())


def get_persona(persona_id_or_handle: str | UUID) -> Persona:
    """Look up a persona by UUID, name, or slack handle.

    Accepts forgiving inputs so CLI/UI call sites can pass whatever
    the user typed. UUIDs are preferred for programmatic use.
    """
    personas = load_personas_cached()
    if isinstance(persona_id_or_handle, UUID):
        for p in personas:
            if p.id == persona_id_or_handle:
                return p
        raise PersonaNotFound(f"no persona with id {persona_id_or_handle}")

    key = persona_id_or_handle.strip().lower()
    # Try UUID parse
    try:
        uid = UUID(key)
    except ValueError:
        uid = None
    for p in personas:
        if uid is not None and p.id == uid:
            return p
        if p.slack_handle and p.slack_handle.lower() == key:
            return p
        if p.name.lower() == key:
            return p
        if p.name.split()[0].lower() == key:
            return p
    raise PersonaNotFound(
        f"no persona matches {persona_id_or_handle!r} "
        f"(known handles: {[p.slack_handle for p in personas if p.slack_handle]})"
    )


def switch_active_persona(persona_id: str | UUID) -> Persona:
    """Set the module-level active persona. Returns the Persona."""
    global _active_persona_id
    persona = get_persona(persona_id)
    with _active_lock:
        _active_persona_id = persona.id
    return persona


def active_persona() -> Optional[Persona]:
    """Return the currently-active persona, or None."""
    with _active_lock:
        pid = _active_persona_id
    if pid is None:
        return None
    return get_persona(pid)


def voice_hints_for(persona_id: str | UUID) -> str:
    """Return a prompt snippet reminding the author how this persona
    sounds. Used by the Slack UI composer to show the user the voice
    above their textarea; not consumed by any LLM.
    """
    p = get_persona(persona_id)
    channels = ", ".join(p.typical_channels) if p.typical_channels else "(none)"
    return (
        f"You are composing as {p.name} ({p.title}, role: {p.role}).\n"
        f"Voice: {p.voice_style_notes}\n"
        f"Typical channels: {channels}."
    )


__all__ = [
    "Persona",
    "PersonaNotFound",
    "load_personas",
    "load_personas_cached",
    "get_persona",
    "switch_active_persona",
    "active_persona",
    "voice_hints_for",
]
