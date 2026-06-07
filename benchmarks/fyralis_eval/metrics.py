"""Metric helpers for benchmark evaluation."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import Any


def normalize_answer(value: str | None) -> str:
    if value is None:
        return ""
    normalized = value.casefold()
    normalized = re.sub(r"[^a-z0-9\s']", " ", normalized)
    normalized = re.sub(r"\b(a|an|the)\b", " ", normalized)
    return " ".join(normalized.split())


def exact_match(predicted: str | None, gold: str | None) -> float | None:
    if gold is None:
        return None
    return 1.0 if normalize_answer(predicted) == normalize_answer(gold) else 0.0


def token_f1(predicted: str | None, gold: str | None) -> float | None:
    if gold is None:
        return None
    predicted_tokens = normalize_answer(predicted).split()
    gold_tokens = normalize_answer(gold).split()
    if not predicted_tokens and not gold_tokens:
        return 1.0
    if not predicted_tokens or not gold_tokens:
        return 0.0
    common = set(predicted_tokens) & set(gold_tokens)
    if not common:
        return 0.0
    precision = len(common) / len(predicted_tokens)
    recall = len(common) / len(gold_tokens)
    return 2 * precision * recall / (precision + recall)


def recall_at_k(retrieved_ids: list[str], gold_ids: list[str], *, k: int) -> float | None:
    if not gold_ids:
        return None
    retrieved = set(retrieved_ids[:k])
    gold = set(gold_ids)
    return len(retrieved & gold) / len(gold)


def precision_at_k(retrieved_ids: list[str], gold_ids: list[str], *, k: int) -> float | None:
    if not gold_ids:
        return None
    retrieved = retrieved_ids[:k]
    if not retrieved:
        return 0.0
    gold = set(gold_ids)
    return len(set(retrieved) & gold) / len(retrieved)


def mean_metric(values: Iterable[float | None]) -> float | None:
    concrete = [value for value in values if value is not None]
    if not concrete:
        return None
    return sum(concrete) / len(concrete)


def longmemeval_v2_score(
    prediction: str | None,
    answer: str | None,
    eval_function: str | None,
) -> float | None:
    """Score deterministic LongMemEval-V2 evaluator specs.

    LME-V2 also defines LLM-judge evaluators for abstention/gotcha questions.
    Those intentionally return ``None`` here until a judge model is configured.
    """

    if not eval_function:
        return None
    parsed_prediction = extract_boxed_answer(prediction or "")
    name, kwargs = _parse_lme_v2_eval_spec(eval_function)
    if name == "norm_phrase_set_match":
        return float(_norm_phrase_set_match(parsed_prediction, answer, **kwargs))
    if name == "norm_phrase_set_match_ordered":
        return float(_norm_phrase_set_match_ordered(parsed_prediction, answer, **kwargs))
    if name == "mc_choice_match":
        return float(_mc_choice_match(parsed_prediction, answer, **kwargs))
    if name == "mc_choice_set_match":
        return float(_mc_choice_set_match(parsed_prediction, answer, **kwargs))
    return None


def extract_boxed_answer(text: str) -> str:
    marker = "\\boxed{"
    idx = text.rfind(marker)
    if idx == -1:
        return text.strip()
    i = idx + len(marker)
    depth = 1
    out: list[str] = []
    while i < len(text) and depth > 0:
        char = text[i]
        if char == "{":
            depth += 1
            out.append(char)
        elif char == "}":
            depth -= 1
            if depth == 0:
                break
            out.append(char)
        else:
            out.append(char)
        i += 1
    parsed = "".join(out).strip()
    return parsed if parsed else text.strip()


def _normalize_phrase(
    text: str | None,
    *,
    lower: bool = True,
    normalize_hyphen: bool = True,
    strip_punct: bool = True,
) -> str:
    if text is None:
        return ""
    if lower:
        text = text.lower()
    if normalize_hyphen:
        text = text.replace("-", " ").replace("_", " ")
    text = re.sub(r"[,;]", " ", text)
    if strip_punct:
        text = re.sub(r"[^\w\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _split_phrases(
    text: str | None,
    *,
    separators: Iterable[str] = (",", ";"),
    **normalize_kwargs: Any,
) -> list[str]:
    if text is None:
        return []
    separator_list = list(separators)
    if not separator_list:
        normalized = _normalize_phrase(text, **normalize_kwargs)
        return [normalized] if normalized else []
    pattern = "|".join(re.escape(separator) for separator in separator_list)
    return [
        normalized
        for part in re.split(pattern, text)
        if (normalized := _normalize_phrase(part, **normalize_kwargs))
    ]


def _norm_phrase_set_match(
    prediction: str | None,
    answer: str | None,
    *,
    separators: Iterable[str] = (",", ";"),
    require_non_empty: bool = True,
    **normalize_kwargs: Any,
) -> bool:
    normalized_prediction = _normalize_phrase(prediction, **normalize_kwargs)
    answer_phrases = _split_phrases(
        answer,
        separators=separators,
        **normalize_kwargs,
    )
    if require_non_empty and (not normalized_prediction or not answer_phrases):
        return False
    return all(
        re.search(r"\b%s\b" % re.escape(phrase), normalized_prediction) is not None
        for phrase in set(answer_phrases)
    )


def _norm_phrase_set_match_ordered(
    prediction: str | None,
    answer: str | None,
    *,
    separators: Iterable[str] = (",", ";"),
    require_non_empty: bool = True,
    **normalize_kwargs: Any,
) -> bool:
    normalized_prediction = _normalize_phrase(prediction, **normalize_kwargs)
    answer_phrases = _split_phrases(
        answer,
        separators=separators,
        **normalize_kwargs,
    )
    if require_non_empty and (not normalized_prediction or not answer_phrases):
        return False
    start = 0
    for phrase in answer_phrases:
        match = re.search(
            r"\b%s\b" % re.escape(phrase),
            normalized_prediction[start:],
        )
        if match is None:
            return False
        start += match.end()
    return True


def _mc_choice_match(
    prediction: str | None,
    answer: str | None,
    *,
    strip_chars: str = ".",
    require_non_empty: bool = True,
    **_: Any,
) -> bool:
    if prediction is None or answer is None:
        return False
    cleaned = re.sub(r"\b(choice|option)\b", "", prediction, flags=re.IGNORECASE)
    for char in strip_chars:
        cleaned = cleaned.replace(char, "")
    cleaned = cleaned.strip().upper()
    expected = answer.strip().upper()
    if require_non_empty and (not cleaned or not expected):
        return False
    return cleaned == expected


def _mc_choice_set_match(
    prediction: str | None,
    answer: str | None,
    *,
    require_non_empty: bool = True,
    **_: Any,
) -> bool:
    pred_letters = _extract_multi_select_letters(prediction)
    answer_letters = _extract_multi_select_letters(answer)
    if require_non_empty and (not pred_letters or not answer_letters):
        return False
    return set(pred_letters) == set(answer_letters)


def _extract_multi_select_letters(text: str | None) -> list[str]:
    if text is None:
        return []
    fillers = {
        "AND",
        "ANSWER",
        "ANSWERS",
        "CHOICE",
        "CHOICES",
        "FINAL",
        "LETTER",
        "LETTERS",
        "OPTION",
        "OPTIONS",
    }
    letters: list[str] = []
    for chunk in re.findall(r"[A-Z]+", text.upper()):
        if chunk in fillers:
            continue
        letters.extend(chunk)
    return letters


def _parse_lme_v2_eval_spec(spec: str) -> tuple[str, dict[str, Any]]:
    parts = [part.strip() for part in spec.split("|")]
    kwargs: dict[str, Any] = {}
    for part in parts[1:]:
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        kwargs[key.strip()] = _parse_lme_v2_eval_value(key.strip(), value.strip())
    return parts[0], kwargs


def _parse_lme_v2_eval_value(key: str, value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"none", "null"}:
        return None
    if key in {"separators", "separator"}:
        if not value:
            return []
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            return list(json.loads(stripped))
        return [char for char in value if not char.isspace()]
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value
