from __future__ import annotations

import pytest

from services.ingest.source_contract.errors import RateLimitedError
from services.ingest.source_contract.host_services import SecretCandidate, SecretValue


def test_error_details_are_immutable() -> None:
    error = RateLimitedError("slow down", details={"retry_after": 10})
    assert error.retryable is True
    assert error.code == "rate_limited"
    with pytest.raises(TypeError):
        error.details["retry_after"] = 20  # type: ignore[index]


def test_secret_value_is_redacted_in_nested_repr() -> None:
    secret = SecretValue.from_text("super-secret")
    candidate = SecretCandidate(slot="api_token", value=secret)
    assert "super-secret" not in repr(secret)
    assert "super-secret" not in repr(candidate)
    assert str(secret) == "<redacted>"
    assert secret.reveal_text() == "super-secret"
