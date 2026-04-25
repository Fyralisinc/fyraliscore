"""services/realtime — Wave 4-D Realtime WebSocket service.

Builds a FastAPI sub-router exposing `WS /stream` that the Gateway
mounts on its main app. Dispatcher listens on Postgres NOTIFY channels
(`observations_new`, emitted by Wave 1-A ingestion; `internal:state_change`
rides the same channel with `kind='state_change'`) and fans out to
subscribed clients.

Python, not Rust: BUILD-PLAN top specifies Rust for services/realtime/,
but Wave 4-D is an explicit stack exception — `fastapi.WebSocket` +
asyncpg `LISTEN`. Rationale: (a) no Rust toolchain on the build host,
(b) Wave 5 performance review can port once the Python version is
proven correct. See BUILD-LOG Wave 4-D entry Deviation (a).
"""

from services.realtime.dispatcher import (  # noqa: F401
    Dispatcher,
    EventFrame,
    SubscriptionFilter,
)
from services.realtime.main import (  # noqa: F401
    realtime_router,
    RealtimeDeps,
    configure_realtime,
)

__all__ = [
    "Dispatcher",
    "EventFrame",
    "SubscriptionFilter",
    "realtime_router",
    "RealtimeDeps",
    "configure_realtime",
]
