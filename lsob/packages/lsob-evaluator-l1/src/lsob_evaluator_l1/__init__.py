"""lsob-evaluator-l1 — Layer 1 retrieval subcomponent evaluator.

Public surface:
    LayerOneEvaluator                — composite evaluator (Evaluator Protocol)
    SemanticPathwayEvaluator         — recall@k, MRR, nDCG@10
    EntityResolutionEvaluator        — precision, recall, accuracy
    RerankerEvaluator                — nDCG@10, Kendall tau
    RetrievalCapableSUT              — optional SUT surface (Protocol)
    MockRetrievalSUT                 — deterministic in-package mock
"""

from lsob_evaluator_l1.composite import LayerOneEvaluator
from lsob_evaluator_l1.entity_resolution import EntityResolutionEvaluator
from lsob_evaluator_l1.l1_protocol import RetrievalCapableSUT
from lsob_evaluator_l1.mock_sut import MockNonRetrievalSUT, MockRetrievalSUT
from lsob_evaluator_l1.reranker import RerankerEvaluator
from lsob_evaluator_l1.semantic import SemanticPathwayEvaluator

__version__ = "0.1.0"

__all__ = [
    "EntityResolutionEvaluator",
    "LayerOneEvaluator",
    "MockNonRetrievalSUT",
    "MockRetrievalSUT",
    "RerankerEvaluator",
    "RetrievalCapableSUT",
    "SemanticPathwayEvaluator",
    "__version__",
]
