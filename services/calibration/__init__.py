"""services/calibration/ — class-level calibration anchors.

Track BC. The hit-rate machinery here is read-only and conservative:
when there isn't enough data for a class we return None rather than
fabricate a number. Calibration is a substrate of trust — fake numbers
poison it.

Public surface:
  * `hit_rate_for_class(tenant_id, claim_class, *, db, window_days)`
      Async query against the calibration substrate; returns a
      `{"class", "hit_rate_30d", "n_samples"}` dict or None.
  * `classify_card(card_focus)` — derive a claim class from a card's
      focus dict; returns None for unclassifiable focus.
"""
from services.calibration.hit_rate import (
    classify_card,
    hit_rate_for_class,
    SUPPORTED_CLAIM_CLASSES,
    MIN_SAMPLES_FOR_CALIBRATION,
)


__all__ = [
    "classify_card",
    "hit_rate_for_class",
    "SUPPORTED_CLAIM_CLASSES",
    "MIN_SAMPLES_FOR_CALIBRATION",
]
