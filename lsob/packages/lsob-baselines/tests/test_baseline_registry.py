"""Registry introspection tests."""

from __future__ import annotations

from lsob_baselines import REGISTRY
from lsob_contracts import SUTConfig, SystemUnderTest


EXPECTED_BASELINES = {
    "company-os",
    "vanilla-rag",
    "langchain-memory",
    "llamaindex-kg",
    "memgpt-style",
    "graphrag",
}


def test_registry_lists_all_six_baselines():
    listed = set(REGISTRY.list())
    assert EXPECTED_BASELINES.issubset(listed)
    assert len(listed) >= 6


def test_registry_construct_returns_sut_instance():
    sut = REGISTRY.construct("vanilla-rag", SUTConfig(sut_name="vanilla-rag"))
    assert isinstance(sut, SystemUnderTest)
    assert hasattr(sut, "ingest_signal")
    assert hasattr(sut, "produce_diff_for_trigger")


def test_registry_construct_each_baseline():
    for name in EXPECTED_BASELINES:
        sut = REGISTRY.construct(name, SUTConfig(sut_name=name))
        assert isinstance(sut, SystemUnderTest), f"{name} must satisfy SystemUnderTest"


def test_registry_unknown_name_raises():
    import pytest

    with pytest.raises(KeyError):
        REGISTRY.construct("does-not-exist", SUTConfig(sut_name="x"))
