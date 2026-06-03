"""services/product/forecasts — backing store + HTTP surface for the Forecasts page.

Three tabs (Active / Resolved / Accuracy) plus a summary strip and a
risk-exposure timeseries are served from this module. The schema lives
in `db/migrations/0041_predictions.sql`. The router is in `router.py`;
the read-side queries are in `repo.py`; accuracy / calibration bins are
in `accuracy.py`.
"""
from __future__ import annotations

from services.product.forecasts.router import build_router
from services.product.forecasts import repo, accuracy


__all__ = ["build_router", "repo", "accuracy"]
