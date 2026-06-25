"""Security headers for gateway-owned static HTML responses."""
from __future__ import annotations

import secrets

from fastapi.responses import HTMLResponse


CSP_NONCE_PLACEHOLDER = "__CSP_NONCE__"


def trusted_static_html_response(
    html: str,
    *,
    status_code: int = 200,
) -> HTMLResponse:
    """Return a static HTML page with nonce-based browser safety headers.

    Gateway HTML surfaces are debug/operator pages, not a template system.
    Dynamic values should be JSON-encoded or assigned via textContent before the
    page reaches this helper.
    """
    nonce = secrets.token_urlsafe(24)
    body = html.replace(CSP_NONCE_PLACEHOLDER, nonce)
    csp = "; ".join(
        (
            "default-src 'none'",
            "base-uri 'none'",
            "object-src 'none'",
            "frame-ancestors 'none'",
            "form-action 'none'",
            "connect-src 'self'",
            f"script-src 'nonce-{nonce}'",
            f"style-src 'nonce-{nonce}'",
            "img-src 'self' data:",
            "font-src 'self'",
        )
    )
    return HTMLResponse(
        body,
        status_code=status_code,
        headers={
            "Cache-Control": "no-store",
            "Content-Security-Policy": csp,
            "Cross-Origin-Opener-Policy": "same-origin",
            "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
        },
    )


__all__ = ["CSP_NONCE_PLACEHOLDER", "trusted_static_html_response"]
