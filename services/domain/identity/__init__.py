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
from .resolution_repo import CandidateProvider, IdentityResolutionRepository, PostgresCandidateProvider
from .intake import IdentityIntakeRepository, IdentityOutboxRow
from .lifecycle import IdentityLifecycleService
from .registrar import ObservationMentionRegistrar
from .query import QueryIdentityResolutionService, QueryResolutionResult, extract_query_mentions
from .service import IdentityResolutionService

__all__ = [
    "CandidateSeed",
    "CandidateProvider",
    "SOURCE_IDENTITY_CAPABILITIES",
    "EntityMentionCreate",
    "EntityMentionRepository",
    "EntityMentionRow",
    "IdentityAssertionCreate",
    "IdentityAssertionRepository",
    "IdentityAssertionRow",
    "IdentityIntakeRepository",
    "IdentityLifecycleService",
    "IdentityOutboxRow",
    "IdentityConstraintCreate",
    "IdentityConstraintValue",
    "IdentityResolutionRepository",
    "IdentityResolutionService",
    "IdentityResolutionSnapshot",
    "IdentitySnapshotItem",
    "PostgresCandidateProvider",
    "ObservationMentionRegistrar",
    "QueryIdentityResolutionService",
    "QueryResolutionResult",
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
    "extract_query_mentions",
    "source_reference_key",
]
