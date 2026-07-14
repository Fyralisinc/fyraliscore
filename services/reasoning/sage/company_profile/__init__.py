"""Company-specific SAGE learning profile."""
from services.reasoning.sage.company_profile.types import (
    CompanyLearningProfile,
    LearningPrior,
)


__all__ = [
    "CompanyLearningProfile",
    "LatentPatternProfileInput",
    "LearningPrior",
    "build_company_learning_profile",
    "build_latent_pattern_profile_input",
    "load_company_learning_profile",
]


def __getattr__(name: str):
    if name == "build_company_learning_profile":
        from services.reasoning.sage.company_profile.builder import (
            build_company_learning_profile,
        )

        return build_company_learning_profile
    if name == "load_company_learning_profile":
        from services.reasoning.sage.company_profile.repo import (
            load_company_learning_profile,
        )

        return load_company_learning_profile
    if name in {"LatentPatternProfileInput", "build_latent_pattern_profile_input"}:
        from services.reasoning.sage.company_profile.scout_inputs import (
            LatentPatternProfileInput,
            build_latent_pattern_profile_input,
        )

        return {
            "LatentPatternProfileInput": LatentPatternProfileInput,
            "build_latent_pattern_profile_input": build_latent_pattern_profile_input,
        }[name]
    raise AttributeError(name)
