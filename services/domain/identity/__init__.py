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

__all__ = [
    "SOURCE_IDENTITY_CAPABILITIES",
    "EntityMentionCreate",
    "EntityMentionRepository",
    "EntityMentionRow",
    "IdentityAssertionCreate",
    "IdentityAssertionRepository",
    "IdentityAssertionRow",
    "ResolutionRunCreate",
    "ResolutionRunRepository",
    "ResolutionRunRow",
    "SourceIdentityCapability",
    "SourceReferenceCreate",
    "SourceReferenceRepository",
    "SourceReferenceRow",
    "canonical_admission",
    "capability_for",
    "capability_snapshot",
    "mention_key",
    "reference_kind_for_hint",
    "source_reference_key",
]
