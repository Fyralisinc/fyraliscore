"""Sealed, matched worlds for the P7 memory-ablation experiment."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import P6Population


P7_POPULATION_VERSION = "epistemic-repair-p7-worlds-v1"
P7_PREREGISTERED_SEEDS = (7103, 7121, 7151, 7177, 7193, 7211, 7243)
P7_INITIAL_WORLD_COUNT = 3
P7_MAX_WORLD_COUNT = 7
P7_BATCH_COUNT = 12
P7_STORYLINES_PER_WORLD = 4
P7_SEMANTIC_ORACLE_VERSION = "epistemic-repair-p7-semantic-oracle-v2"


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


class StructuredClaimOracle(_Frozen):
    storyline_id: str
    subject_terms: tuple[str, ...] = Field(min_length=1)
    cause_facets: tuple[tuple[str, ...], ...] = Field(min_length=1)
    effect_facets: tuple[tuple[str, ...], ...] = Field(min_length=1)
    required_relation: str
    expected_polarity: str = "positive"


class TypedRelationOracle(_Frozen):
    storyline_id: str
    relation_kind: str
    cause_participant_facets: tuple[tuple[str, ...], ...]
    effect_participant_facets: tuple[tuple[str, ...], ...]
    cause_role: str
    effect_role: str


class OutcomeOracle(_Frozen):
    storyline_id: str
    freeze_after_batch: int = 10
    outcome_batch: int = 11
    outcome_label: int = Field(ge=0, le=1)
    outcome_signal_ids: tuple[str, ...] = Field(min_length=1)


class P7SemanticOraclePopulation(_Frozen):
    version: str
    source_population_digest: str
    claims: tuple[StructuredClaimOracle, ...] = Field(min_length=4, max_length=4)
    relations: tuple[TypedRelationOracle, ...] = Field(min_length=4, max_length=4)
    outcomes: tuple[OutcomeOracle, ...] = Field(min_length=4, max_length=4)
    digest: str

    @model_validator(mode="after")
    def validate_semantic_digest(self) -> "P7SemanticOraclePopulation":
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"digest"}))
        if self.digest != expected:
            raise ValueError("P7 semantic oracle digest mismatch")
        return self


_SEMANTIC_STRUCTURE = {
    "atlas": {
        "cause": (("certificate",), ("owner", "ownership"), ("handoff",)),
        "effect": (("slip", "delay"),),
        "relation": "causal_influence", "required": "causes",
        "roles": ("cause", "effect"),
    },
    "beacon": {
        "cause": (("access review", "access"),),
        "effect": (("completion", "complete"), ("deploy", "deployment")),
        "relation": "dependency_constraint", "required": "depends_on",
        "roles": ("dependent", "prerequisite"),
    },
    "cobalt": {
        "cause": (("customer approval", "customer"), ("crm", "optimistic")),
        "effect": (("renewal",), ("risk",)),
        "relation": "predictive_indicator", "required": "predicts",
        "roles": ("indicator", "outcome"),
    },
    "delta": {
        "cause": (("support",), ("handoff",), ("owner", "ownership")),
        "effect": (("incident",),),
        "relation": "causal_influence", "required": "causes",
        "roles": ("cause", "effect"),
    },
}


def build_p7_semantic_oracles(
    population: P6Population,
) -> P7SemanticOraclePopulation:
    """Bind P7-only semantic/outcome gold to one sealed P6 world digest."""

    claims = []
    relations = []
    outcomes = []
    thesis_by_storyline = dict(population.thesis_by_storyline)
    for storyline in ("atlas", "beacon", "cobalt", "delta"):
        spec = _SEMANTIC_STRUCTURE[storyline]
        entity_subjects = tuple(dict.fromkeys(
            item.entity_surface.casefold()
            for item in population.gold
            if item.storyline_id == storyline and item.entity_surface
        ))
        subjects = tuple(dict.fromkeys((
            *entity_subjects,
            *(value.split()[0] for value in entity_subjects),
        )))
        if not subjects:
            subjects = (" ".join(thesis_by_storyline[storyline].split()[:2]).casefold(),)
        claims.append(StructuredClaimOracle(
            storyline_id=storyline, subject_terms=subjects,
            cause_facets=spec["cause"], effect_facets=spec["effect"],
            required_relation=spec["required"],
        ))
        relations.append(TypedRelationOracle(
            storyline_id=storyline, relation_kind=spec["relation"],
            cause_participant_facets=spec["cause"],
            effect_participant_facets=spec["effect"],
            cause_role=spec["roles"][0], effect_role=spec["roles"][1],
        ))
        outcomes.append(OutcomeOracle(
            storyline_id=storyline, outcome_label=1,
            outcome_signal_ids=tuple(
                item.signal_id for item in population.gold
                if item.storyline_id == storyline
                and item.lifecycle_phase == "external_outcome"
            ),
        ))
    payload = {
        "version": P7_SEMANTIC_ORACLE_VERSION,
        "source_population_digest": population.population_digest,
        "claims": claims, "relations": relations, "outcomes": outcomes,
    }
    normalized = {
        key: [item.model_dump(mode="json") for item in value]
        if isinstance(value, list) else value
        for key, value in payload.items()
    }
    return P7SemanticOraclePopulation(
        **payload, digest=canonical_sha256(normalized)
    )


__all__ = [
    "P7_INITIAL_WORLD_COUNT", "P7_MAX_WORLD_COUNT", "P7Population",
    "P7SemanticOraclePopulation", "P7World", "StructuredClaimOracle",
    "TypedRelationOracle", "build_p7_population", "build_p7_semantic_oracles",
]
