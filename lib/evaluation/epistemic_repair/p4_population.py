"""Sealed deterministic population for the P4 online-learning proof."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json


@dataclass(frozen=True, slots=True)
class P4Signal:
    signal_id: str
    batch_id: str
    episode_id: str
    ordinal: int
    semantic_role: str


@dataclass(frozen=True, slots=True)
class P4Batch:
    batch_id: str
    ordinal: int
    required_event: str
    signals: tuple[P4Signal, ...]


@dataclass(frozen=True, slots=True)
class P4Population:
    version: str
    batches: tuple[P4Batch, ...]

    @property
    def digest(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return sha256(payload.encode()).hexdigest()


def build_p4_population() -> P4Population:
    events = (
        "admit_first_model",
        "admit_second_model_and_relation_reusing_first",
        "reuse_models_relation_and_reopen_one_historical_observation",
        "reuse_models_and_relation_without_unnecessary_raw_history",
        "contradict_and_correct_first_model",
        "reuse_corrected_state_and_exclude_stale_state",
    )
    batches = []
    for batch_ordinal, event in enumerate(events, 1):
        batch_id = f"p4-batch-{batch_ordinal}"
        signals = tuple(
            P4Signal(
                signal_id=f"{batch_id}-signal-{signal_ordinal:02d}",
                batch_id=batch_id,
                episode_id=f"{batch_id}-episode-{1 + (signal_ordinal % 2)}",
                ordinal=signal_ordinal,
                semantic_role=(
                    "useful" if signal_ordinal <= 8
                    else "counterevidence" if signal_ordinal <= 10
                    else "distractor"
                ),
            )
            for signal_ordinal in range(1, 21)
        )
        batches.append(P4Batch(batch_id, batch_ordinal, event, signals))
    return P4Population("p4-online-learning-population-v1", tuple(batches))


__all__ = ["P4Batch", "P4Population", "P4Signal", "build_p4_population"]
