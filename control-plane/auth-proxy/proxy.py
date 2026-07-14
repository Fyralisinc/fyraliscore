#!/usr/bin/env python3
"""proxy.py — the Fyralis tenant auth proxy (P2, Invariant I4).

An **mTLS-terminating reverse proxy** that sits in front of central
Mimir/Loki/Grafana. It is the single most security-critical component in the
control plane: it is the *only* place tenant identity is established, and it is
established **server-side from the verified client cert**, never from a header.

Request lifecycle
-----------------
1. **TLS handshake** — the server SSL context requires a client cert
   (``ssl.CERT_REQUIRED``) that chains to the Fyralis CA (``load_verify_locations``
   with ``ca/pki/ca-chain.crt``). A client with no cert, or a cert that does not
   chain to the CA, **fails the handshake** — the request never reaches HTTP.
2. **Peer cert → tenant** — after the handshake we pull the *verified* peer leaf
   in DER form straight off the SSL object and hand it to
   :class:`~tenant_resolver.TenantResolver`, which re-verifies the chain,
   extracts ``tenant_id`` from the SPIFFE SAN, and runs the fail-closed
   revocation check. Any failure ⇒ flat **403** (no 5xx, no detail leak); we
   **never forward an unauthenticated request**.
3. **Header hygiene** — every inbound ``X-Scope-OrgID`` (and any ``x-scope-org*``
   variant) is **stripped**, then a single ``X-Scope-OrgID: <tenant_id>`` derived
   from the cert is **injected**. A client that sends ``X-Scope-OrgID: globex``
   while presenting acme's cert is scoped to **acme**.
4. **Reverse proxy** — the sanitized request is forwarded to the configured
   upstream (default Mimir) and the upstream response is streamed back.

Why a custom asyncio+h11 server (not uvicorn)? A security proxy needs *direct,
unambiguous* access to the verified DER peer cert and *byte-level* control over
which headers cross the trust boundary. asyncio's SSL transport exposes the peer
cert via ``getpeercert(binary_form=True)``; h11 is a vetted sans-IO HTTP/1.1
state machine (no hand-rolled parsing). This keeps the security-critical path
small and auditable.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import h11
import httpx

# Local imports work whether run as ``python proxy.py`` or imported as a module.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import ProxyConfig, load_config  # noqa: E402
from tenant_resolver import (  # noqa: E402
    ResolvedTenant,
    TenantResolutionError,
    TenantResolver,
)

logger = logging.getLogger("auth_proxy")

# Hop-by-hop headers (RFC 7230 §6.1) must not be forwarded by a proxy.
_HOP_BY_HOP = frozenset(
    {
        b"connection",
        b"keep-alive",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"te",
        b"trailers",
        b"transfer-encoding",
        b"upgrade",
    }
)

_MAX_REQUEST_BODY = 64 * 1024 * 1024  # 64 MiB cap so a huge body can't OOM us.


def build_server_ssl_context(config: ProxyConfig) -> ssl.SSLContext:
    """Build the TLS server context that REQUIRES + VERIFIES a client cert.

    * presents the proxy's own server cert/key (``tls_cert_path``/``tls_key_path``);
    * trusts the Fyralis CA chain for *client* verification (``ca_chain_path``);
    * ``verify_mode = CERT_REQUIRED`` ⇒ the handshake fails for a missing or
      untrusted client cert (we never even reach the HTTP layer unauthenticated).
    """
    config.require_files()
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    # Server identity presented to the data-plane agent.
    ctx.load_cert_chain(
        certfile=str(config.tls_cert_path), keyfile=str(config.tls_key_path)
    )
    # Trust anchor used to VERIFY the client (data-plane) cert — this is the C1
    # CA chain. Combined with CERT_REQUIRED, the TLS layer rejects any client
    # cert that does not chain to this CA.
    ctx.load_verify_locations(cafile=str(config.ca_chain_path))
    ctx.verify_mode = ssl.CERT_REQUIRED
    # No need for the client to present a hostname; identity is the SPIFFE SAN.
    ctx.check_hostname = False
    # Modern floor.
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


class _Http403(Exception):
    """Internal signal to emit a flat 403 (carries an audit reason only)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _Http405(Exception):
    """Internal signal to emit a flat 405 (carries an audit reason only)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _safe_upstream_path(target: str) -> str:
    """Reduce a client request target to a forwardable **path?query** only (SSRF guard).

    h11 accepts every RFC 7230 request-target form, including *absolute-form*
    (``GET http://evil-host/path HTTP/1.1``) and *authority-form*
    (``CONNECT evil-host:443``). If the raw target were handed to httpx, an
    absolute-form scheme://authority would OVERRIDE the pinned upstream host and
    let an authenticated tenant re-point the destination — an SSRF.

    This function pins the destination by keeping **only** the path and query:

    * **origin-form** (``/path?query``)  -> returned as-is.
    * **absolute-form** (``scheme://authority/path?query``) -> scheme+authority
      are DISCARDED; only ``path?query`` survives.
    * **authority-form / CONNECT** (``host:port``, no leading ``/``, no scheme)
      -> rejected with 405 (a forward/tunnel proxy verb we never serve).
    * **asterisk-form** (``*``) -> rejected (only valid for server-wide OPTIONS,
      not something we proxy to Mimir).

    The returned value always begins with ``/`` (an empty path becomes ``/``) and
    never carries a scheme or authority, so the caller can safely join it onto the
    configured upstream base URL.
    """
    if not target:
        raise _Http403("empty_request_target")

    # asterisk-form (server-wide OPTIONS) — not proxyable to an upstream path.
    if target == "*":
        raise _Http405("asterisk_form_target")

    # origin-form: the ONLY form we proxy. It must start with '/'. Anything else
    # (absolute-form scheme://authority/..., authority-form host:port, or a bare
    # ``scheme:opaque`` that urlsplit mis-parses) is handled below.
    if target.startswith("/"):
        split = urlsplit(target)
        # Drop any scheme/authority/fragment defensively; keep only path+query.
        return urlunsplit(("", "", split.path or "/", split.query, ""))

    split = urlsplit(target)

    if split.scheme and split.netloc:
        # absolute-form: a real scheme AND authority are present. DISCARD both and
        # keep only path+query. This is the SSRF-defeating step — the attacker's
        # host can never reach httpx; we always dial the configured upstream.
        return urlunsplit(("", "", split.path or "/", split.query, ""))

    # Everything else is authority-form (``CONNECT host:port``), a bare
    # ``scheme:opaque`` with no authority, asterisk-form, or otherwise malformed.
    # We are not a forward/tunnel proxy — reject rather than guess a destination.
    raise _Http405("authority_form_or_non_origin_target")


class AuthProxy:
    """The proxy server: one shared upstream client, per-connection handlers."""

    def __init__(self, config: ProxyConfig) -> None:
        self.config = config
        self.resolver = TenantResolver.from_paths(
            config.ca_chain_path,
            config.tenant_registry_path,
        )
        self._client: Optional[httpx.AsyncClient] = None
        self._server: Optional[asyncio.AbstractServer] = None

    # --- lifecycle ---------------------------------------------------------

    async def start(self) -> asyncio.AbstractServer:
        ssl_ctx = build_server_ssl_context(self.config)
        self._client = httpx.AsyncClient(
            base_url=self.config.upstream_url,
            timeout=self.config.upstream_timeout_s,
        )
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self.config.listen_host,
            port=self.config.listen_port,
            ssl=ssl_ctx,
        )
        sockets = self._server.sockets or []
        addrs = ", ".join(str(s.getsockname()) for s in sockets)
        logger.info(
            "auth-proxy listening (mTLS, client-cert REQUIRED) on %s -> upstream %s",
            addrs,
            self.config.upstream_url,
        )
        return self._server

    async def aclose(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def serve_forever(self) -> None:
        server = await self.start()
        try:
            async with server:
                await server.serve_forever()
        finally:
            await self.aclose()

    # --- per-connection ----------------------------------------------------

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        conn = h11.Connection(h11.SERVER)
        try:
            # The verified peer cert (DER) is established by the TLS handshake;
            # it is identical for every request on this connection.
            peer_der = self._peer_cert_der(writer)
            await self._serve_requests(conn, reader, writer, peer_der)
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except Exception:  # never let a handler bug crash the server loop
            logger.exception("unhandled error in connection handler")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    @staticmethod
    def _peer_cert_der(writer: asyncio.StreamWriter) -> Optional[bytes]:
        """Pull the VERIFIED client leaf cert (DER) off the SSL transport.

        Because the context is ``CERT_REQUIRED`` and trusts only the Fyralis CA,
        any cert present here has already been verified by the TLS stack. We
        re-verify in the resolver anyway (defense in depth).
        """
        ssl_obj = writer.get_extra_info("ssl_object")
        if ssl_obj is None:
            return None
        try:
            return ssl_obj.getpeercert(binary_form=True)
        except Exception:
            return None

    async def _serve_requests(
        self,
        conn: h11.Connection,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_der: Optional[bytes],
    ) -> None:
        while True:
            event = await self._next_event(conn, reader)
            if event is h11.PAUSED or isinstance(event, h11.ConnectionClosed):
                return
            if not isinstance(event, h11.Request):
                # Out of sequence; close the connection.
                return

            request: h11.Request = event
            body = await self._read_body(conn, reader)

            # ---- the security decision -----------------------------------
            try:
                resolved = self._authenticate(peer_der)
            except _Http403 as exc:
                logger.warning("403 reject: reason=%s", exc.reason)
                await self._send_simple(conn, writer, 403, b"Forbidden\n")
                if conn.our_state is h11.MUST_CLOSE or conn.their_state is h11.MUST_CLOSE:
                    return
                self._maybe_recycle(conn)
                continue

            # ---- forward upstream with the injected scope header ----------
            try:
                await self._proxy_upstream(conn, writer, request, body, resolved)
            except _Http403 as exc:
                logger.warning("403 reject: reason=%s", exc.reason)
                await self._send_simple(conn, writer, 403, b"Forbidden\n")
            except _Http405 as exc:
                logger.warning("405 reject: reason=%s", exc.reason)
                await self._send_simple(conn, writer, 405, b"Method Not Allowed\n")
            except httpx.HTTPError as exc:
                logger.warning("upstream error: %s", exc)
                await self._send_simple(conn, writer, 502, b"Bad Gateway\n")

            if conn.our_state is h11.MUST_CLOSE or conn.their_state is h11.MUST_CLOSE:
                return
            self._maybe_recycle(conn)

    def _maybe_recycle(self, conn: h11.Connection) -> None:
        if conn.our_state is h11.DONE and conn.their_state is h11.DONE:
            conn.start_next_cycle()

    # --- security decision -------------------------------------------------

    def _authenticate(self, peer_der: Optional[bytes]) -> ResolvedTenant:
        """Resolve the verified peer cert to an active tenant, or raise 403.

        Every failure mode in :class:`TenantResolver` (no cert, bad chain, bad
        SAN, revoked/unknown fingerprint, SAN↔registry mismatch, unreadable
        registry) collapses to a flat 403 here — fail closed, no detail on wire.
        """
        try:
            return self.resolver.resolve(peer_der)
        except TenantResolutionError as exc:
            raise _Http403(exc.reason) from exc

    # --- upstream proxying -------------------------------------------------

    async def _proxy_upstream(
        self,
        conn: h11.Connection,
        writer: asyncio.StreamWriter,
        request: h11.Request,
        body: bytes,
        resolved: ResolvedTenant,
    ) -> None:
        method = request.method.decode("latin-1")
        # SSRF guard (Invariant I4 / T12): reduce the client target to a path+query
        # ONLY. An absolute-form target (scheme://authority/...) has its scheme and
        # authority discarded; authority-form/CONNECT is rejected. The destination
        # host:port:scheme is ALWAYS the configured upstream — never client input.
        safe_path = _safe_upstream_path(request.target.decode("latin-1"))
        out_headers = self._sanitize_headers(request.headers, resolved.tenant_id)
        # Pin the outgoing Host header to the CONFIGURED upstream authority so no
        # attacker-controlled Host can influence the destination or upstream routing.
        upstream_host = urlsplit(self.config.upstream_url).netloc
        if upstream_host:
            out_headers.append(("host", upstream_host))

        assert self._client is not None
        # Build the upstream URL from the CONFIGURED base + the path-only target.
        # Passing an absolute httpx.URL whose authority equals the client's base_url
        # means there is no field left through which the target could re-point us.
        upstream_base = self.config.upstream_url.rstrip("/")
        upstream_url = upstream_base + safe_path
        upstream = await self._client.request(
            method,
            upstream_url,
            headers=out_headers,
            content=body if body else None,
        )

        # Build the downstream response; strip hop-by-hop + length/encoding
        # headers (httpx already decoded the body; we set our own Content-Length).
        resp_headers = []
        for name, value in upstream.headers.raw:
            lname = name.lower()
            if lname in _HOP_BY_HOP or lname in (b"content-length",):
                continue
            resp_headers.append((name, value))
        content = upstream.content
        resp_headers.append((b"content-length", str(len(content)).encode("latin-1")))

        await self._send(
            conn,
            writer,
            h11.Response(status_code=upstream.status_code, headers=resp_headers),
        )
        await self._send(conn, writer, h11.Data(data=content))
        await self._send(conn, writer, h11.EndOfMessage())

    def _sanitize_headers(self, in_headers, tenant_id: str):
        """Strip client scope headers + hop-by-hop, then inject the real scope.

        The contract scope header and any ``x-scope-org*`` variant are removed
        regardless of casing — a client CANNOT smuggle a tenant scope. The
        single injected value comes ONLY from the cert-derived tenant id (I4).
        """
        scope_lower = self.config.scope_header.lower().encode("latin-1")
        prefixes = tuple(p.encode("latin-1") for p in self.config.strip_header_prefixes)
        out = []
        for name, value in in_headers:
            lname = name.lower()
            if lname == scope_lower:
                continue
            if any(lname.startswith(p) for p in prefixes):
                continue
            if lname in _HOP_BY_HOP:
                continue
            if lname == b"host":
                # Let httpx set the upstream Host from the base_url.
                continue
            if lname == b"content-length":
                # httpx recomputes this from the body we pass.
                continue
            out.append((name.decode("latin-1"), value.decode("latin-1")))
        # Inject the server-derived scope — the sole source of tenant identity.
        out.append((self.config.scope_header, tenant_id))
        return out

    # --- h11 plumbing ------------------------------------------------------

    async def _next_event(self, conn: h11.Connection, reader: asyncio.StreamReader):
        while True:
            event = conn.next_event()
            if event is h11.NEED_DATA:
                data = await reader.read(65536)
                conn.receive_data(data)
                if data == b"":
                    # Peer closed; surface whatever the state machine yields.
                    continue
                continue
            return event

    async def _read_body(
        self, conn: h11.Connection, reader: asyncio.StreamReader
    ) -> bytes:
        chunks = []
        total = 0
        while True:
            event = await self._next_event(conn, reader)
            if isinstance(event, h11.Data):
                chunks.append(bytes(event.data))
                total += len(event.data)
                if total > _MAX_REQUEST_BODY:
                    raise _Http403("request_body_too_large")
            elif isinstance(event, h11.EndOfMessage):
                break
            elif isinstance(event, h11.ConnectionClosed) or event is h11.PAUSED:
                break
            else:
                break
        return b"".join(chunks)

    async def _send(self, conn: h11.Connection, writer: asyncio.StreamWriter, event):
        data = conn.send(event)
        if data is not None:
            writer.write(data)
            await writer.drain()

    async def _send_simple(
        self,
        conn: h11.Connection,
        writer: asyncio.StreamWriter,
        status: int,
        body: bytes,
    ) -> None:
        headers = [
            ("content-type", "text/plain; charset=utf-8"),
            ("content-length", str(len(body))),
        ]
        try:
            await self._send(conn, writer, h11.Response(status_code=status, headers=headers))
            await self._send(conn, writer, h11.Data(data=body))
            await self._send(conn, writer, h11.EndOfMessage())
        except Exception:
            # If h11 is in a state where we cannot send (e.g. mid-stream), just
            # close — we still never forwarded an unauthenticated request.
            pass


async def _amain(config: Optional[ProxyConfig] = None) -> None:
    cfg = config or load_config()
    proxy = AuthProxy(cfg)
    await proxy.serve_forever()


def main(argv: Optional[list] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
