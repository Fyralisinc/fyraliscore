"""lsob-simulation — tick-based deterministic simulator for LSOB corpora."""

from lsob_simulation.config_loader import load_config
from lsob_simulation.ground_truth import GroundTruthRecorder, PatternTruthEntry
from lsob_simulation.io import read_corpus, write_corpus
from lsob_simulation.signal_gen import (
    LLMSignalGenerator,
    SignalGenerator,
    TemplateSignalGenerator,
)
from lsob_simulation.simulator import Simulator
from lsob_simulation.state import ActorState, CommitmentState, CustomerState
from lsob_simulation.validator import (
    ValidationReport,
    validate_corpus,
    validate_corpus_file,
)

__version__ = "0.1.0"

__all__ = [
    "ActorState",
    "CommitmentState",
    "CustomerState",
    "GroundTruthRecorder",
    "LLMSignalGenerator",
    "PatternTruthEntry",
    "SignalGenerator",
    "Simulator",
    "TemplateSignalGenerator",
    "ValidationReport",
    "load_config",
    "read_corpus",
    "validate_corpus",
    "validate_corpus_file",
    "write_corpus",
    "__version__",
]
