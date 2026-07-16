"""Shared fail-closed visibility predicate for canonical Model projections."""

from __future__ import annotations


def active_visible_model_predicates(alias: str | None = None) -> tuple[str, str]:
    prefix = f"{alias}." if alias else ""
    return (
        f"{prefix}status = 'active'",
        f"{prefix}visible_to_subjects = TRUE",
    )


def active_visible_model_sql(alias: str | None = None) -> str:
    return " AND ".join(active_visible_model_predicates(alias))


__all__ = ["active_visible_model_predicates", "active_visible_model_sql"]
