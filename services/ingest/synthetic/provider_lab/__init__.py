"""Deterministic, loopback-only fake-provider runtime for integration tests."""
from __future__ import annotations

from .adapters import build_lab_adapter_registry
from .app import build_provider_lab_app
from .calibration import (
    LabCalibration,
    LabCalibrationConfig,
    assess_lab_calibration,
    calibrate_provider_lab,
    require_lab_calibration,
)
from .protocol import (
    AdapterRegistry,
    ProviderAdapter,
    ProviderRequest,
    ProviderResponse,
    ProviderRoute,
)
from .runtime import DEFAULT_CLOCK_START, InjectedDisconnect, LabRuntime
from .server import ProviderLabServer, start_provider_lab


__all__ = [
    "AdapterRegistry",
    "DEFAULT_CLOCK_START",
    "InjectedDisconnect",
    "LabCalibration",
    "LabCalibrationConfig",
    "LabRuntime",
    "ProviderAdapter",
    "ProviderLabServer",
    "ProviderRequest",
    "ProviderResponse",
    "ProviderRoute",
    "assess_lab_calibration",
    "build_lab_adapter_registry",
    "build_provider_lab_app",
    "calibrate_provider_lab",
    "require_lab_calibration",
    "start_provider_lab",
]
