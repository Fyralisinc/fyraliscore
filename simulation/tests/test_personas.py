"""Unit tests for the persona registry.

Run: `pytest simulation/tests/test_personas.py` — no DB, no Ollama.
"""
from __future__ import annotations

from uuid import UUID

import pytest

from simulation.personas import (
    PERSONAS_YAML,
    Persona,
    PersonaNotFound,
    active_persona,
    get_persona,
    load_personas,
    load_personas_cached,
    switch_active_persona,
    voice_hints_for,
)


def test_yaml_loads_twelve_personas():
    personas = load_personas()
    assert len(personas) == 12
    roles = {p.role for p in personas}
    # Spot check: Acme narrative needs eng + sales + cs + cfo + ceo.
    for needed in (
        "engineer",
        "head_of_sales",
        "customer_success",
        "cfo",
        "ceo",
        "head_of_engineering",
    ):
        assert needed in roles, f"missing role {needed} in personas.yaml"


def test_persona_refs_are_channel_prefixed():
    p = get_persona("alice")
    assert p.slack_ref == "slack:alice"
    assert p.github_ref == "github:alice-dev"
    assert p.email_ref.startswith("email:")


def test_persona_lookup_by_name_and_handle():
    p1 = get_persona("Alice Chen")
    p2 = get_persona("alice")
    p3 = get_persona(p1.id)
    assert p1 == p2 == p3


def test_missing_persona_raises():
    with pytest.raises(PersonaNotFound):
        get_persona("nobody")


def test_switch_active_persona_mutates_module_state():
    switch_active_persona("alice")
    ap = active_persona()
    assert ap is not None and ap.slack_handle == "alice"

    switch_active_persona("monica")
    ap = active_persona()
    assert ap is not None and ap.slack_handle == "monica"


def test_voice_hints_includes_role_and_notes():
    hints = voice_hints_for("alice")
    assert "Alice Chen" in hints
    assert "engineer" in hints
    assert "terse" in hints  # from the voice_style_notes


def test_cached_load_is_tuple_and_same_instance():
    load_personas_cached.cache_clear()
    t1 = load_personas_cached()
    t2 = load_personas_cached()
    assert t1 is t2
    assert isinstance(t1, tuple)
    # All entries are Persona frozen dataclasses.
    assert all(isinstance(p, Persona) for p in t1)


def test_persona_ids_are_uuid7_shape():
    # Not strictly UUID v7 (we hand-authored for readability) but must
    # be valid UUIDs so asyncpg can INSERT them as actors.id.
    for p in load_personas():
        assert isinstance(p.id, UUID)
        # Our authored IDs are all v7-like (version nibble = 7).
        assert str(p.id)[14] == "7"


def test_yaml_file_path_exists():
    assert PERSONAS_YAML.exists()
