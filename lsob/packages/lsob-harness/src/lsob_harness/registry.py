"""Dynamic registry shims.

Streams B (evaluators) and C (baselines) may or may not be merged at the time
this harness runs. We therefore *try* to import their concrete registries at
call-time and fall back to the mock versions otherwise. This keeps the harness
fully testable in isolation.
"""

from __future__ import annotations

from typing import Any

from lsob_contracts import SUTConfig

from lsob_harness.mocks import (
    MockEvaluatorRegistry,
    MockSUTRegistry,
)


def _load_baseline_registry() -> Any | None:
    """Return the live BaselineRegistry singleton, or None if not installed.

    Handles two shapes:

    - ``lsob_baselines.registry.REGISTRY`` (a module-level instance — the
      current Stream C layout).
    - ``lsob_baselines.registry.BaselineRegistry`` exposing ``construct`` as a
      classmethod (an earlier design some notes still reference).
    """
    try:
        from lsob_baselines import registry as _reg_mod  # type: ignore
    except Exception:
        return None
    candidate = getattr(_reg_mod, "REGISTRY", None)
    if candidate is not None and hasattr(candidate, "construct"):
        return candidate
    cls = getattr(_reg_mod, "BaselineRegistry", None)
    if cls is not None and hasattr(cls, "construct"):
        return cls
    return None


def construct_sut(name: str, config: SUTConfig) -> Any:
    """Construct a SUT.

    Order of resolution:

    1. ``lsob_baselines.registry`` (via ``REGISTRY`` singleton or
       classmethod-style ``BaselineRegistry``) if importable.
    2. :class:`MockSUTRegistry` fallback (only recognises ``mock``/``mock-sut``).
    """
    # Always honour the in-package mock name so tests stay hermetic even if
    # the real baselines package is also installed.
    if name in MockSUTRegistry.list_names():
        return MockSUTRegistry.construct(name, config)

    registry = _load_baseline_registry()
    if registry is not None:
        try:
            return registry.construct(name, config)
        except KeyError:
            # Fall through — maybe the mock can handle a legacy name.
            pass
    return MockSUTRegistry.construct(name, config)


def baselines_registry_available() -> bool:
    try:
        import lsob_baselines.registry  # type: ignore # noqa: F401

        return True
    except Exception:
        return False


def list_known_suts() -> list[str]:
    names: list[str] = list(MockSUTRegistry.list_names())
    registry = _load_baseline_registry()
    if registry is not None:
        lister = getattr(registry, "list", None) or getattr(registry, "list_names", None)
        if callable(lister):
            try:
                extra = list(lister())
            except Exception:
                extra = []
            for n in extra:
                if n not in names:
                    names.append(n)
    return names


def construct_evaluators(layers: list[int]) -> list[Any]:
    """Construct evaluators for the given layer ids.

    Tries ``lsob_evaluators.registry.EvaluatorRegistry.construct_for_layers``
    first (a convenience aggregator if Stream B provides one) and falls back to
    per-package imports (``lsob_evaluator_l1`` etc.). Anything still missing is
    filled in with :class:`~lsob_harness.mocks.NoopEvaluator`.
    """
    try:
        from lsob_evaluators.registry import EvaluatorRegistry  # type: ignore

        return list(EvaluatorRegistry.construct_for_layers(layers))
    except Exception:
        pass

    collected: list[Any] = []
    for layer in layers:
        ev = _try_layer_package(layer)
        if ev is not None:
            collected.append(ev)
        else:
            collected.extend(MockEvaluatorRegistry.construct_for_layers([layer]))
    return collected


def _try_layer_package(layer: int) -> Any | None:
    mod_name = f"lsob_evaluator_l{layer}"
    try:
        mod = __import__(mod_name, fromlist=["*"])
    except Exception:
        return None
    for attr in ("build_evaluator", "Evaluator", f"L{layer}Evaluator"):
        candidate = getattr(mod, attr, None)
        if candidate is None:
            continue
        try:
            return candidate() if callable(candidate) else candidate
        except Exception:
            continue
    return None
