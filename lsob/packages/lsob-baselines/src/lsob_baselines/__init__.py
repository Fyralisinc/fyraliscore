"""lsob-baselines — ``SystemUnderTest`` adapters for the LSOB benchmark.

Importing this package registers all six Phase 1 baselines against
``lsob_baselines.REGISTRY`` (see :mod:`lsob_baselines.registry`).
"""

from __future__ import annotations

# Side-effect registrations.
from . import (  # noqa: F401
    company_os,
    graphrag,
    langchain_memory,
    llamaindex_kg,
    memgpt_style,
    vanilla_rag,
)
from .common import chunk_text, cosine_similarity, extract_entities, hash_embedding
from .company_os import (
    CompanyOSBaseline,
    CompanyOSUnavailableError,
    LocalCompanyOSClient,
    MockCompanyOSClient,
)
from .diff_translator import DiffTranslator, TemplateDiffTranslator
from .graphrag import GraphRAGBaseline
from .langchain_memory import LangChainMemoryBaseline
from .llamaindex_kg import LlamaIndexKGBaseline
from .memgpt_style import MemGPTStyleBaseline
from .registry import REGISTRY, BaselineRegistry
from .vanilla_rag import VanillaRAGBaseline

__version__ = "0.1.0"

__all__ = [
    "BaselineRegistry",
    "CompanyOSBaseline",
    "CompanyOSUnavailableError",
    "DiffTranslator",
    "GraphRAGBaseline",
    "LangChainMemoryBaseline",
    "LlamaIndexKGBaseline",
    "LocalCompanyOSClient",
    "MemGPTStyleBaseline",
    "MockCompanyOSClient",
    "REGISTRY",
    "TemplateDiffTranslator",
    "VanillaRAGBaseline",
    "__version__",
    "chunk_text",
    "cosine_similarity",
    "extract_entities",
    "hash_embedding",
]
