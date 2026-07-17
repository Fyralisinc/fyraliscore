"""Sealed, matched worlds for the P7 memory-ablation experiment."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256


P7_POPULATION_VERSION = "epistemic-repair-p7-worlds-v1"
P7_PREREGISTERED_SEEDS = (7103, 7121, 7151, 7177, 7193, 7211, 7243)
P7_INITIAL_WORLD_COUNT = 3
P7_MAX_WORLD_COUNT = 7
P7_BATCH_COUNT = 12
P7_STORYLINES_PER_WORLD = 4


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class P7Thesis(_Frozen):
    thesis_id: str
    facets: tuple[str, ...] = Field(min_length=4)


class P7World(_Frozen):
    world_id: str
    seed: int
    batch_count: int = P7_BATCH_COUNT
    theses: tuple[P7Thesis, ...] = Field(min_length=4, max_length=4)
    corruption_thesis_id: str

    @model_validator(mode="after")
    def corruption_target_exists(self) -> "P7World":
        if self.corruption_thesis_id not in {item.thesis_id for item in self.theses}:
            raise ValueError("corruption target must be one of the sealed theses")
        return self


class P7Population(_Frozen):
    version: str
    initial_world_count: int
    maximum_world_count: int
    seeds: tuple[int, ...]
    worlds: tuple[P7World, ...]
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def digest_is_sealed(self) -> "P7Population":
        if len(self.worlds) != self.maximum_world_count:
            raise ValueError("all optional worlds must be sealed before execution")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"digest"}))
        if expected != self.digest:
            raise ValueError("population digest does not match sealed worlds")
        return self


def build_p7_population() -> P7Population:
    storyline_names = ("release", "customer", "access", "capacity")
    worlds: list[P7World] = []
    for index, seed in enumerate(P7_PREREGISTERED_SEEDS, start=1):
        theses = tuple(
            P7Thesis(
                thesis_id=f"w{index}-{name}",
                facets=(
                    f"{name}:owner",
                    f"{name}:state",
                    f"{name}:dependency",
                    f"{name}:outcome",
                ),
            )
            for name in storyline_names
        )
        worlds.append(
            P7World(
                world_id=f"p7-world-{index:02d}",
                seed=seed,
                theses=theses,
                corruption_thesis_id=theses[0].thesis_id,
            )
        )
    payload = {
        "version": P7_POPULATION_VERSION,
        "initial_world_count": P7_INITIAL_WORLD_COUNT,
        "maximum_world_count": P7_MAX_WORLD_COUNT,
        "seeds": P7_PREREGISTERED_SEEDS,
        "worlds": [item.model_dump(mode="json") for item in worlds],
    }
    return P7Population(**payload, digest=canonical_sha256(payload))
