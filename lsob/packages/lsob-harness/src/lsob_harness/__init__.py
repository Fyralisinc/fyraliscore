"""lsob-harness — CLI and run orchestration for the LSOB benchmark."""

from lsob_harness.ablation import (
    REGISTRY as ABLATION_REGISTRY,
    AblationError,
    AblationRegistry,
    AblationValidationError,
    apply_ablation,
)

__version__ = "0.1.0"

__all__ = [
    "ABLATION_REGISTRY",
    "AblationError",
    "AblationRegistry",
    "AblationValidationError",
    "apply_ablation",
]
