"""Reusable loopback TCP runner for sandbox and end-to-end Provider Lab use."""
from __future__ import annotations

import socket
import threading
import time
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any, Self

import uvicorn

from .adapters import seed_reference_fixtures
from .app import build_provider_lab_app


class ProviderLabServer:
    """One in-process Provider Lab served by uvicorn on an ephemeral port."""

    def __init__(
        self,
        fixtures: Mapping[str, list[Mapping[str, Any]]] | None = None,
        *,
        host: str = "127.0.0.1",
        startup_timeout_seconds: float = 5.0,
    ) -> None:
        if host != "127.0.0.1":
            raise ValueError("Provider Lab test server must bind to 127.0.0.1")
        self.app = build_provider_lab_app(fixtures=fixtures)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, 0))
        self._socket.listen(2_048)
        self._socket.setblocking(False)
        port = int(self._socket.getsockname()[1])
        self.base_url = f"http://{host}:{port}"
        self._server = uvicorn.Server(
            uvicorn.Config(
                self.app,
                host=host,
                port=port,
                log_level="error",
                lifespan="off",
                ws="websockets-sansio",
                access_log=False,
            )
        )
        self._failure: BaseException | None = None
        self._thread = threading.Thread(
            target=self._serve,
            name="fyralis-provider-lab",
            daemon=True,
        )
        self._thread.start()
        deadline = time.monotonic() + startup_timeout_seconds
        while not self._server.started and self._failure is None:
            if time.monotonic() >= deadline:
                self.shutdown()
                raise TimeoutError("Provider Lab test server did not start")
            time.sleep(0.01)
        if self._failure is not None:
            failure = self._failure
            self.shutdown()
            raise RuntimeError("Provider Lab test server failed to start") from failure

    def _serve(self) -> None:
        try:
            self._server.run(sockets=[self._socket])
        except BaseException as exc:  # pragma: no cover - surfaced by constructor
            self._failure = exc

    def url(self, source: str, path: str = "") -> str:
        """Return an explicit source-scoped Provider Lab URL."""

        normalized_path = f"/{path.lstrip('/')}" if path else ""
        return f"{self.base_url}/{source}{normalized_path}"

    def replace_fixtures(
        self,
        source: str,
        fixtures: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        """Translate canonical fixture input and replace one source's live state."""

        seeded = seed_reference_fixtures({source: list(fixtures)})
        if source not in seeded:
            raise ValueError(f"no Provider Lab fixture seeder for {source!r}")
        return self.app.state.provider_lab.set_source_state(
            source,
            seeded[source],
        )

    def replace_source_state(
        self,
        source: str,
        state: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Use the Provider Lab control-plane state replacement primitive."""

        return self.app.state.provider_lab.set_source_state(source, state)

    def request_count(
        self,
        *,
        source: str,
        route_id: str | None = None,
        scope: str | None = None,
    ) -> int:
        """Count matching provider requests in the canonical request ledger."""

        return len(
            self.app.state.provider_lab.ledger.list(
                source=source,
                route_id=route_id,
                scope=scope,
                limit=1_000,
            )
        )

    def shutdown(self, timeout_seconds: float = 5.0) -> None:
        """Stop uvicorn and release the loopback socket."""

        if not hasattr(self, "_server"):
            return
        self._server.should_exit = True
        if hasattr(self, "_thread") and self._thread.is_alive():
            self._thread.join(timeout_seconds)
        if self._thread.is_alive():
            self._server.force_exit = True
            self._thread.join(1.0)
        try:
            self._socket.close()
        except OSError:
            pass
        if self._thread.is_alive():
            raise RuntimeError("Provider Lab test server did not stop")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.shutdown()


def start_provider_lab(
    fixtures: Mapping[str, list[Mapping[str, Any]]] | None = None,
) -> ProviderLabServer:
    """Start one loopback Provider Lab for sandbox or E2E use."""

    return ProviderLabServer(fixtures)


__all__ = ["ProviderLabServer", "start_provider_lab"]
