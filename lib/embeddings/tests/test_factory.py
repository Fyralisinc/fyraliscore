"""Tests for lib/embeddings/factory.py — backend resolution from env."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from lib.embeddings.base import EmbedderError
from lib.embeddings.factory import _resolve_backend, make_embedder
from lib.embeddings.ollama import OllamaClient


def test_explicit_backend_wins_over_env():
    with patch.dict(os.environ, {"EMBEDDER_BACKEND": "openai"}, clear=False):
        assert _resolve_backend("ollama") == "ollama"


def test_explicit_unknown_raises():
    with pytest.raises(EmbedderError):
        _resolve_backend("voyage")  # type: ignore[arg-type]


def test_env_backend_used():
    with patch.dict(os.environ, {"EMBEDDER_BACKEND": "openai"}, clear=False):
        assert _resolve_backend(None) == "openai"


def test_env_unknown_raises():
    with patch.dict(os.environ, {"EMBEDDER_BACKEND": "junk"}, clear=False):
        with pytest.raises(EmbedderError):
            _resolve_backend(None)


def test_fallback_prefers_openai_when_only_key_set():
    env = {"OPENAI_API_KEY": "sk-x"}
    with patch.dict(os.environ, env, clear=True):
        assert _resolve_backend(None) == "openai"


def test_fallback_prefers_ollama_when_ollama_url_set():
    env = {"OPENAI_API_KEY": "sk-x", "OLLAMA_URL": "http://localhost:11434"}
    with patch.dict(os.environ, env, clear=True):
        assert _resolve_backend(None) == "ollama"


def test_fallback_default_ollama():
    with patch.dict(os.environ, {}, clear=True):
        assert _resolve_backend(None) == "ollama"


def test_make_embedder_returns_ollama_by_default():
    with patch.dict(os.environ, {}, clear=True):
        e = make_embedder()
    try:
        assert isinstance(e, OllamaClient)
    finally:
        # Don't await close; we never opened a real client. The
        # underlying httpx.AsyncClient is created lazily but the
        # close() coroutine is safe to skip in a sync test.
        pass


def test_make_embedder_openai_requires_key():
    with patch.dict(os.environ, {"EMBEDDER_BACKEND": "openai"}, clear=True):
        with pytest.raises(Exception):  # OpenAIEmbedderError; subclass of EmbedderError
            make_embedder()
