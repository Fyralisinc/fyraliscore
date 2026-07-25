from __future__ import annotations

import inspect

import services.ingest.ingestion.fetchers._clients as clients


def test_source_global_breaker_proxy_cannot_reenter_client_builders() -> None:
    forbidden_symbols = {
        "SourceApiCircuitBreakerProxy",
        "_SOURCE_API_BREAKERS",
        "_SOURCE_API_BREAKERS_LOCK",
        "_record_source_api_breaker_exception",
        "_source_api_breaker",
        "_wrap_source_client",
    }

    assert forbidden_symbols.isdisjoint(vars(clients))

    source = inspect.getsource(clients)
    assert "AsyncCircuitBreaker" not in source
    assert "source_api_" not in source
