"""Descriptive completion ratios for fixed, heterogeneous proof obligations."""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ChecklistRatio(BaseModel):
    """A descriptive ratio, explicitly carrying no sampling uncertainty claim."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    numerator: int = Field(ge=0)
    denominator: int = Field(gt=0)
    point_estimate: float = Field(ge=0.0, le=1.0)
    method: Literal["descriptive_checklist_ratio"] = (
        "descriptive_checklist_ratio"
    )

    @model_validator(mode="after")
    def exact_ratio(self) -> Self:
        if self.numerator > self.denominator:
            raise ValueError("checklist numerator cannot exceed denominator")
        expected = self.numerator / self.denominator
        if not math.isclose(
            self.point_estimate,
            expected,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError(
                "checklist point estimate must equal numerator / denominator"
            )
        return self

    @classmethod
    def from_flags(cls, flags: Iterable[bool]) -> ChecklistRatio:
        values = tuple(bool(flag) for flag in flags)
        if not values:
            raise ValueError("checklist ratio requires at least one obligation")
        numerator = sum(values)
        denominator = len(values)
        return cls(
            numerator=numerator,
            denominator=denominator,
            point_estimate=numerator / denominator,
        )


__all__ = ["ChecklistRatio"]
