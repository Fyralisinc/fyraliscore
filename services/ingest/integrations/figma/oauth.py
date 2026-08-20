"""Figma deployment OAuth configuration used by BYOC onboarding.

The generic SourceConnector install runtime owns authorization execution.  The
BYOC control plane only needs to report whether the deployment-owned OAuth app
is configured and which callback URL it uses.
"""

from __future__ import annotations

import os

from lib.shared.secrets import load_app_secret_text_from_env


class FigmaOAuthError(RuntimeError):
    """Safe configuration error surfaced by the BYOC handoff."""


def _figma_redirect_uri() -> str:
    return os.environ.get("FIGMA_REDIRECT_URI", "").strip()


def _deployment_oauth_ready() -> bool:
    return bool(
        os.environ.get("FIGMA_CLIENT_ID", "").strip()
        and load_app_secret_text_from_env("FIGMA_CLIENT_SECRET")
        and _figma_redirect_uri()
    )


__all__ = ["FigmaOAuthError"]
