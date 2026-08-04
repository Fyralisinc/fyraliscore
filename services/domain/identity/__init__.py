"""Versioned organizational identity assertions."""

from .capabilities import (
    SOURCE_IDENTITY_CAPABILITIES,
    SourceIdentityCapability,
    canonical_admission,
    capability_for,
    capability_snapshot,
    reference_kind_for_hint,
)
from .foundation import (
    EntityMentionCreate,
    EntityMentionRow,
    ResolutionRunCreate,
    ResolutionRunRow,
    SourceReferenceCreate,
    SourceReferenceRow,
    mention_key,
    source_reference_key,
)
from .foundation_repo import (
    EntityMentionRepository,
    ResolutionRunRepository,
    SourceReferenceRepository,
)
from .models import IdentityAssertionCreate, IdentityAssertionRow
from .repo import IdentityAssertionRepository
from .resolution import (
    CandidateSeed,
    IdentityConstraintCreate,
    IdentityConstraintValue,
    IdentityResolutionSnapshot,
    IdentitySnapshotItem,
    RankedCandidate,
    ResolutionDecision,
    ResolutionThreshold,
    decide_resolution,
    rank_candidates,
)
from .resolution_repo import IdentityResolutionRepository, PostgresCandidateProvider
from .service import IdentityResolutionService

__all__ = [
    "CandidateSeed",
    "SOURCE_IDENTITY_CAPABILITIES",
    "EntityMentionCreate",
    "EntityMentionRepository",
    "EntityMentionRow",
    "IdentityAssertionCreate",
    "IdentityAssertionRepository",
    "IdentityAssertionRow",
    "IdentityConstraintCreate",
    "IdentityConstraintValue",
    "IdentityResolutionRepository",
    "IdentityResolutionService",
    "IdentityResolutionSnapshot",
    "IdentitySnapshotItem",
    "PostgresCandidateProvider",
    "RankedCandidate",
    "ResolutionDecision",
    "ResolutionRunCreate",
    "ResolutionRunRepository",
    "ResolutionRunRow",
    "ResolutionThreshold",
    "SourceIdentityCapability",
    "SourceReferenceCreate",
    "SourceReferenceRepository",
    "SourceReferenceRow",
    "canonical_admission",
    "capability_for",
    "capability_snapshot",
    "decide_resolution",
    "mention_key",
    "reference_kind_for_hint",
    "rank_candidates",
    "source_reference_key",
]
