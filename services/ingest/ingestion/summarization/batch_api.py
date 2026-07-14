"""OpenAI Batch API helpers for document summarization."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Protocol

import orjson

from lib.llm.provider import LLMConfig
from services.ingest.ingestion.summarization.llm import (
    DEFAULT_SUMMARY_MAX_TOKENS,
    DEFAULT_SUMMARIZER_MODEL,
    DocumentSummarySchema,
    SummaryResult,
    batch_input_cap_from_env,
    build_summary_prompt,
    parse_summary_text,
    summary_limit_from_env,
)


log = logging.getLogger(__name__)


BATCH_ENDPOINT = "/v1/responses"


@dataclass(frozen=True)
class BatchSubmitResult:
    provider_batch_id: str
    input_file_id: str | None
    status: str


@dataclass(frozen=True)
class BatchStatus:
    provider_batch_id: str
    status: str
    input_file_id: str | None = None
    output_file_id: str | None = None
    error_file_id: str | None = None
    error_context: dict[str, Any] | None = None


class BatchClient(Protocol):
    async def submit_jsonl(
        self,
        jsonl: str,
        *,
        metadata: dict[str, str],
    ) -> BatchSubmitResult:
        ...

    async def retrieve(self, provider_batch_id: str) -> BatchStatus:
        ...

    async def file_text(self, file_id: str) -> str:
        ...


def _getattr_or_key(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _summary_model() -> str:
    return os.environ.get("INGEST_SUMMARIZER_MODEL", DEFAULT_SUMMARIZER_MODEL)


def _summary_reasoning_effort() -> str:
    return os.environ.get("INGEST_SUMMARIZER_REASONING_EFFORT", "low")


def build_batch_request_line(
    *,
    custom_id: str,
    source_text: str,
    metadata: dict[str, Any],
) -> str:
    max_chars = summary_limit_from_env()
    # The Batch API is one request line per item, so true map-reduce is awkward
    # here (deferred to Phase 2). For now the whole document is sent in a single
    # call; if it exceeds the raised single-call cap we LOG it rather than
    # silently truncating (docs/plans/document-memory-substrate.md §3.2).
    input_cap = batch_input_cap_from_env()
    if len(source_text) > input_cap:
        log.warning(
            "summarization.batch.input_exceeds_cap",
            extra={
                "custom_id": custom_id,
                "source_chars": len(source_text),
                "input_cap_chars": input_cap,
                "source_channel": metadata.get("source_channel"),
            },
        )
    system, user = build_summary_prompt(
        source_text,
        metadata=metadata,
        max_chars=max_chars,
    )
    schema = DocumentSummarySchema.model_json_schema()
    body: dict[str, Any] = {
        "model": _summary_model(),
        "instructions": system,
        "input": user,
        "max_output_tokens": DEFAULT_SUMMARY_MAX_TOKENS,
        "temperature": 0.0,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "fyralis_document_summary",
                "schema": schema,
                "strict": False,
            }
        },
    }
    effort = _summary_reasoning_effort()
    if effort:
        body["reasoning"] = {"effort": effort}
    return orjson.dumps(
        {
            "custom_id": custom_id,
            "method": "POST",
            "url": BATCH_ENDPOINT,
            "body": body,
        }
    ).decode("utf-8")


def _responses_output_text_from_body(body: dict[str, Any]) -> str:
    direct = body.get("output_text")
    if isinstance(direct, str) and direct:
        return direct
    chunks: list[str] = []
    for item in body.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str):
                chunks.append(text)
    return "".join(chunks)


def parse_batch_output_line(
    line: str,
    *,
    max_chars: int | None = None,
) -> tuple[str, SummaryResult | None, str | None]:
    payload = orjson.loads(line)
    custom_id = payload.get("custom_id")
    if not isinstance(custom_id, str) or not custom_id:
        raise ValueError("batch output line missing custom_id")
    error = payload.get("error")
    if error:
        return custom_id, None, json.dumps(error, sort_keys=True)[:1000]
    response = payload.get("response")
    if not isinstance(response, dict):
        return custom_id, None, "batch output line missing response"
    status_code = int(response.get("status_code") or 0)
    body = response.get("body")
    if status_code < 200 or status_code >= 300 or not isinstance(body, dict):
        return custom_id, None, f"batch response status={status_code}"
    text = _responses_output_text_from_body(body)
    if not text:
        return custom_id, None, "batch response body had no output text"
    model = body.get("model")
    result = parse_summary_text(
        text,
        model=model if isinstance(model, str) else _summary_model(),
        max_chars=max_chars or summary_limit_from_env(),
    )
    return custom_id, result, None


class OpenAIBatchClient:
    def __init__(self, *, config: LLMConfig | None = None) -> None:
        self._config = config or LLMConfig.from_env()

    def _client_kwargs(self) -> dict[str, Any]:
        if not self._config.api_key:
            raise RuntimeError("OpenAI Batch API key is unset")
        kwargs: dict[str, Any] = {
            "api_key": self._config.api_key,
            "timeout": self._config.timeout_s,
        }
        return kwargs

    async def submit_jsonl(
        self,
        jsonl: str,
        *,
        metadata: dict[str, str],
    ) -> BatchSubmitResult:
        import openai

        client = openai.AsyncOpenAI(**self._client_kwargs())
        file_obj = await client.files.create(
            file=("summarization_batch.jsonl", jsonl.encode("utf-8")),
            purpose="batch",
        )
        input_file_id = _getattr_or_key(file_obj, "id")
        batch = await client.batches.create(
            input_file_id=input_file_id,
            endpoint=BATCH_ENDPOINT,
            completion_window="24h",
            metadata=metadata,
        )
        return BatchSubmitResult(
            provider_batch_id=str(_getattr_or_key(batch, "id")),
            input_file_id=str(input_file_id) if input_file_id else None,
            status=str(_getattr_or_key(batch, "status") or "submitted"),
        )

    async def retrieve(self, provider_batch_id: str) -> BatchStatus:
        import openai

        client = openai.AsyncOpenAI(**self._client_kwargs())
        batch = await client.batches.retrieve(provider_batch_id)
        errors = _getattr_or_key(batch, "errors")
        return BatchStatus(
            provider_batch_id=str(_getattr_or_key(batch, "id") or provider_batch_id),
            status=str(_getattr_or_key(batch, "status") or "submitted"),
            input_file_id=_getattr_or_key(batch, "input_file_id"),
            output_file_id=_getattr_or_key(batch, "output_file_id"),
            error_file_id=_getattr_or_key(batch, "error_file_id"),
            error_context=errors if isinstance(errors, dict) else None,
        )

    async def file_text(self, file_id: str) -> str:
        import openai

        client = openai.AsyncOpenAI(**self._client_kwargs())
        response = await client.files.content(file_id)
        raw = await response.aread()
        return raw.decode("utf-8")


__all__ = [
    "BATCH_ENDPOINT",
    "BatchClient",
    "BatchStatus",
    "BatchSubmitResult",
    "OpenAIBatchClient",
    "build_batch_request_line",
    "parse_batch_output_line",
]
