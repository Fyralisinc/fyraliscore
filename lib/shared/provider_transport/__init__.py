"""Universal outbound-provider transport contract."""

from lib.shared.provider_transport.contracts import (
    NoopQuotaCoordinator,
    ProviderPermanentError,
    ProviderRateLimited,
    ProviderRetryForbiddenError,
    ProviderTimeoutError,
    ProviderTransientError,
    ProviderTransportError,
    QuotaCoordinator,
    QuotaCoordinatorError,
    QuotaDenialReason,
    QuotaDecision,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
    RetrySafety,
)
from lib.shared.provider_transport.progress import (
    FetchProgress,
    ZeroProgressError,
    validate_fetch_progress,
)
from lib.shared.provider_transport.retry_after import (
    parse_retry_after,
    rate_limited_from_headers,
)
from lib.shared.provider_transport.transport import (
    ProviderTransport,
    full_jitter_delay,
)


__all__ = [
    "FetchProgress",
    "NoopQuotaCoordinator",
    "ProviderPermanentError",
    "ProviderRateLimited",
    "ProviderRetryForbiddenError",
    "ProviderTimeoutError",
    "ProviderTransientError",
    "ProviderTransport",
    "ProviderTransportError",
    "QuotaCoordinator",
    "QuotaCoordinatorError",
    "QuotaDenialReason",
    "QuotaDecision",
    "QuotaRequirement",
    "RequestContext",
    "RequestPolicy",
    "RetryLater",
    "RetryReason",
    "RetrySafety",
    "ZeroProgressError",
    "full_jitter_delay",
    "parse_retry_after",
    "rate_limited_from_headers",
    "validate_fetch_progress",
]
