"""Explicit execution authority and composition profile for Think runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from lib.shared.errors import InvariantViolation


_EVALUATION_CAPABILITY = object()


@dataclass(frozen=True, slots=True)
class ThinkExecutionPolicy:
    mode: Literal["normal", "validate_only"] = "normal"
    profile: Literal["full", "stage1_company_memory"] = "full"
    authority: str = "production"
    _capability: object | None = None

    def assert_authorized(self) -> None:
        if self.mode == "validate_only" and (
            self.authority != "evaluation_control"
            or self._capability is not _EVALUATION_CAPABILITY
        ):
            raise InvariantViolation(
                "THINK_VALIDATE_ONLY_UNAUTHORIZED",
                "validate-only Think requires an explicit evaluation-control capability",
            )

    @property
    def is_stage1_company_memory(self) -> bool:
        """Whether this run is restricted to the Stage 1 company-memory loop."""

        return self.profile == "stage1_company_memory"


NORMAL_EXECUTION_POLICY = ThinkExecutionPolicy()
STAGE1_COMPANY_MEMORY_POLICY = ThinkExecutionPolicy(
    profile="stage1_company_memory",
    authority="stage1_company_memory",
)


def issue_evaluation_validate_only_policy() -> ThinkExecutionPolicy:
    """Issue the explicit in-process capability; there is intentionally no env hook."""

    return ThinkExecutionPolicy(
        mode="validate_only",
        authority="evaluation_control",
        _capability=_EVALUATION_CAPABILITY,
    )


__all__ = [
    "NORMAL_EXECUTION_POLICY",
    "STAGE1_COMPANY_MEMORY_POLICY",
    "ThinkExecutionPolicy",
    "issue_evaluation_validate_only_policy",
]
