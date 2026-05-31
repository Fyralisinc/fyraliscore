"""Claim-role contracts for the Model memory grammar.

The base proposition kind is intentionally small: observation, belief,
prediction, norm. Claim roles carry the structural semantics inside each
stance. This registry gives those roles enough shape to prevent `belief`
from becoming an unbounded junk drawer without bringing back the old
dozen-kind discriminator.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lib.shared.errors import ValidationError
from lib.shared.memory_grammar import (
    AbstractionLevel,
    ClaimRole,
    MemoryGrammar,
    TimeMode,
    derive_memory_grammar,
)


Stance = str
FieldGroup = tuple[str, ...]


@dataclass(frozen=True)
class ClaimRoleContract:
    """A structural contract for one grammar claim_role.

    `required_field_groups` is an OR-of-ANDs:
    at least one group must be fully present. For example, a relation
    requires subject+relation+object, while a fact can be expressed as an
    event, assertion, summary, claim, or assessment depending on stance.
    """

    role: ClaimRole
    allowed_stances: tuple[Stance, ...]
    required_field_groups: tuple[FieldGroup, ...]
    allowed_abstraction_levels: tuple[AbstractionLevel, ...] = ()
    allowed_time_modes: tuple[TimeMode, ...] = ()
    min_member_model_ids: int | None = None
    description: str = ""


CLAIM_ROLE_REGISTRY: dict[ClaimRole, ClaimRoleContract] = {
    "fact": ClaimRoleContract(
        role="fact",
        allowed_stances=("observation", "belief"),
        required_field_groups=(
            ("event",),
            ("assertion",),
            ("summary",),
            ("claim",),
            ("assessment",),
        ),
        allowed_abstraction_levels=("atomic",),
        description="Atomic observed or inferred truth.",
    ),
    "concern": ClaimRoleContract(
        role="concern",
        allowed_stances=("belief",),
        required_field_groups=(
            ("about", "nature"),
            ("assertion",),
            ("summary",),
            ("claim",),
        ),
        allowed_abstraction_levels=("atomic",),
        description="Risk, blocker, warning, or negative pressure.",
    ),
    "hypothesis": ClaimRoleContract(
        role="hypothesis",
        allowed_stances=("belief",),
        required_field_groups=(
            ("hypothesis_text",),
            ("claim",),
            ("summary",),
        ),
        allowed_abstraction_levels=("atomic",),
        description="Unproven explanation or testable interpretation.",
    ),
    "prediction": ClaimRoleContract(
        role="prediction",
        allowed_stances=("prediction",),
        required_field_groups=(("expected", "resolution"),),
        allowed_abstraction_levels=("atomic",),
        allowed_time_modes=("future",),
        description="Falsifiable claim about a future outcome.",
    ),
    "pattern": ClaimRoleContract(
        role="pattern",
        allowed_stances=("belief",),
        required_field_groups=(
            ("signature",),
            ("observed_tendency",),
            ("matched_context",),
            ("direction",),
            ("assessment",),
        ),
        allowed_abstraction_levels=("atomic", "pattern"),
        description="Repeated tendency, trend, or concrete pattern instance.",
    ),
    "situation": ClaimRoleContract(
        role="situation",
        allowed_stances=("belief",),
        required_field_groups=(
            ("situation", "summary", "member_model_ids", "relationship_summary"),
        ),
        allowed_abstraction_levels=("composite",),
        min_member_model_ids=2,
        description="Composite belief over multiple member Models.",
    ),
    "capability": ClaimRoleContract(
        role="capability",
        allowed_stances=("belief",),
        required_field_groups=(
            ("capability_id", "assessment"),
            ("subject", "assessment"),
        ),
        allowed_abstraction_levels=("atomic",),
        description="Evidenced skill, capacity, trust, or capability claim.",
    ),
    "relation": ClaimRoleContract(
        role="relation",
        allowed_stances=("belief",),
        required_field_groups=(("subject", "relation", "object"),),
        allowed_abstraction_levels=("relationship",),
        description="Relationship between two subjects or claims.",
    ),
    "recommendation": ClaimRoleContract(
        role="recommendation",
        allowed_stances=("norm",),
        required_field_groups=(("proposed_change",),),
        allowed_abstraction_levels=("atomic",),
        allowed_time_modes=("future",),
        description="Normative action proposal for a human-approved change.",
    ),
}


def contract_for_claim_role(role: ClaimRole) -> ClaimRoleContract:
    """Return the registered contract for a claim role."""
    return CLAIM_ROLE_REGISTRY[role]


def validate_claim_role_contract(
    proposition: dict[str, Any],
    *,
    natural: str = "",
    scope_entities: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    grammar: MemoryGrammar | None = None,
) -> ClaimRoleContract:
    """Validate a proposition against its derived claim-role contract."""
    if not isinstance(proposition, dict):
        raise ValidationError(
            f"proposition must be a dict; got {type(proposition).__name__}",
            field="proposition",
        )
    grammar = grammar or derive_memory_grammar(
        proposition,
        natural=natural,
        scope_entities=scope_entities,
    )
    contract = CLAIM_ROLE_REGISTRY.get(grammar.claim_role)
    if contract is None:
        raise ValidationError(
            f"unknown claim_role {grammar.claim_role!r}",
            field="proposition.claim_role",
            value=grammar.claim_role,
        )

    stance = proposition.get("kind")
    if stance not in contract.allowed_stances:
        raise ValidationError(
            f"claim_role={grammar.claim_role!r} is not valid for kind={stance!r}",
            field="proposition.claim_role",
            claim_role=grammar.claim_role,
            kind=stance,
            allowed_kinds=list(contract.allowed_stances),
        )

    if (
        contract.allowed_abstraction_levels
        and grammar.abstraction_level not in contract.allowed_abstraction_levels
    ):
        raise ValidationError(
            f"claim_role={grammar.claim_role!r} requires abstraction_level in "
            f"{list(contract.allowed_abstraction_levels)}",
            field="proposition.abstraction_level",
            claim_role=grammar.claim_role,
            abstraction_level=grammar.abstraction_level,
        )

    if (
        contract.allowed_time_modes
        and grammar.time_mode not in contract.allowed_time_modes
    ):
        raise ValidationError(
            f"claim_role={grammar.claim_role!r} requires time_mode in "
            f"{list(contract.allowed_time_modes)}",
            field="proposition.time_mode",
            claim_role=grammar.claim_role,
            time_mode=grammar.time_mode,
        )

    pending_members = bool(
        proposition.get("_pending_members")
        or proposition.get("members_pending")
    )
    if not any(
        _field_group_present(
            proposition,
            group,
            pending_members=pending_members,
        )
        for group in contract.required_field_groups
    ):
        expected = [" + ".join(group) for group in contract.required_field_groups]
        raise ValidationError(
            f"claim_role={grammar.claim_role!r} requires one of: "
            + "; ".join(expected),
            field="proposition",
            claim_role=grammar.claim_role,
            required_field_groups=expected,
        )

    if contract.min_member_model_ids is not None:
        member_ids = proposition.get("member_model_ids")
        if not pending_members and (
            not isinstance(member_ids, list)
            or len(member_ids) < contract.min_member_model_ids
        ):
            raise ValidationError(
                f"claim_role={grammar.claim_role!r} requires at least "
                f"{contract.min_member_model_ids} member_model_ids",
                field="proposition.member_model_ids",
                claim_role=grammar.claim_role,
                min_member_model_ids=contract.min_member_model_ids,
            )

    return contract


def _field_group_present(
    proposition: dict[str, Any],
    group: FieldGroup,
    *,
    pending_members: bool = False,
) -> bool:
    return all(
        (pending_members or _present(proposition.get(field)))
        if field == "member_model_ids"
        else _present(proposition.get(field))
        for field in group
    )


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


__all__ = [
    "CLAIM_ROLE_REGISTRY",
    "ClaimRoleContract",
    "contract_for_claim_role",
    "validate_claim_role_contract",
]
