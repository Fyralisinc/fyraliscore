"""Structured startup status for gateway readiness and diagnostics."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal


StartupState = Literal["pending", "ok", "disabled", "degraded", "failed"]


@dataclass(slots=True)
class StartupComponent:
    """Status for one startup component."""

    name: str
    status: StartupState
    required: bool
    detail: str | None = None
    error_type: str | None = None
    updated_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "status": self.status,
            "required": self.required,
            "updated_at": self.updated_at,
        }
        if self.detail:
            payload["detail"] = self.detail
        if self.error_type:
            payload["error_type"] = self.error_type
        return payload


@dataclass(slots=True)
class StartupStatus:
    """Mutable startup ledger attached to ``app.state``."""

    ready: bool = False
    failed: bool = False
    phase: str = "created"
    components: dict[str, StartupComponent] = field(default_factory=dict)

    def reset(self) -> None:
        self.ready = False
        self.failed = False
        self.phase = "starting"
        self.components.clear()

    def mark_ready(self) -> None:
        self.ready = True
        self.failed = False
        self.phase = "ready"

    def mark_stopping(self) -> None:
        self.ready = False
        if not self.failed:
            self.phase = "stopping"

    def mark_stopped(self) -> None:
        self.ready = False
        self.phase = "failed" if self.failed else "stopped"

    def ok(
        self,
        name: str,
        *,
        required: bool,
        detail: str | None = None,
    ) -> None:
        self._set(name, "ok", required=required, detail=detail)

    def disabled(
        self,
        name: str,
        *,
        required: bool = False,
        detail: str | None = None,
    ) -> None:
        self._set(name, "disabled", required=required, detail=detail)

    def degraded(
        self,
        name: str,
        *,
        required: bool = False,
        detail: str | None = None,
        exc: BaseException | None = None,
    ) -> None:
        self._set(
            name,
            "degraded",
            required=required,
            detail=detail or (str(exc) if exc else None),
            error_type=type(exc).__name__ if exc else None,
        )

    def failed_component(
        self,
        name: str,
        *,
        required: bool = True,
        detail: str | None = None,
        exc: BaseException | None = None,
    ) -> None:
        self.failed = True
        self.ready = False
        self.phase = "failed"
        self._set(
            name,
            "failed",
            required=required,
            detail=detail or (str(exc) if exc else None),
            error_type=type(exc).__name__ if exc else None,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "failed": self.failed,
            "phase": self.phase,
            "components": {
                name: component.as_dict()
                for name, component in sorted(self.components.items())
            },
        }

    def _set(
        self,
        name: str,
        status: StartupState,
        *,
        required: bool,
        detail: str | None = None,
        error_type: str | None = None,
    ) -> None:
        self.components[name] = StartupComponent(
            name=name,
            status=status,
            required=required,
            detail=detail,
            error_type=error_type,
        )


__all__ = ["StartupComponent", "StartupStatus"]
