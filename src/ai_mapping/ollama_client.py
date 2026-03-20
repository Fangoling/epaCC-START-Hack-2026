"""
Ollama client for the AI Mapping Agent.
Uses instructor + openai-compatible API for validated structured output.

All failures raise LLMUnavailableError — there are no silent fallbacks.
Connection failures use exponential backoff before raising.
"""

from __future__ import annotations

import os
import time
from typing import TypeVar, Type

from pydantic import BaseModel

try:
    import instructor as _instructor
    from openai import OpenAI as _OpenAI, APIConnectionError, APIStatusError
    _LLM_AVAILABLE = True
except ImportError:
    _instructor = None  # type: ignore[assignment]
    _OpenAI = None  # type: ignore[assignment]
    APIConnectionError = Exception  # type: ignore[assignment,misc]
    APIStatusError = Exception  # type: ignore[assignment,misc]
    _LLM_AVAILABLE = False

OLLAMA_ENABLED = os.getenv("OLLAMA_ENABLED", "true").lower() == "true"
OLLAMA_URL = os.getenv("OLLAMA_URL", "https://ollama.com")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:20b")
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))

_MAX_RETRIES = 3
_BACKOFF_RETRIES = 5
_BACKOFF_BASE = 2.0

T = TypeVar("T", bound=BaseModel)


class LLMUnavailableError(RuntimeError):
    """Raised when the LLM cannot be reached or is not configured."""


def _get_client():
    base = _OpenAI(
        base_url=f"{OLLAMA_URL.rstrip('/')}/v1",
        api_key=OLLAMA_API_KEY or "ollama",
        timeout=OLLAMA_TIMEOUT,
    )
    return _instructor.from_openai(base, mode=_instructor.Mode.JSON)


def _guard() -> None:
    """Raise LLMUnavailableError if LLM is not configured."""
    if not _LLM_AVAILABLE:
        raise LLMUnavailableError("instructor/openai not installed")
    if not OLLAMA_ENABLED:
        raise LLMUnavailableError("OLLAMA_ENABLED=false")
    if not OLLAMA_API_KEY:
        raise LLMUnavailableError("OLLAMA_API_KEY not set")


def call_structured(
    prompt: str,
    response_model: Type[T],
    model: str | None = None,
    system: str | None = None,
    run=None,  # PipelineRun | None — avoid circular import with TYPE_CHECKING
    stage: str = "schema_discovery",
) -> T:
    """
    Send a prompt and return a validated Pydantic model instance.

    Raises LLMUnavailableError if:
      - LLM is not configured
      - All retries are exhausted (connection errors)
      - A non-retriable API error occurs
    """
    _guard()

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    target_model = model or OLLAMA_MODEL
    last_exc: Exception | None = None

    if run:
        try:
            from src.observability.models import EventType
            run.log(
                EventType.LLM_CALL_STARTED,
                stage=stage,
                data={
                    "model": target_model,
                    "response_model": response_model.__name__,
                    "prompt_chars": len(prompt),
                    "prompt_full": prompt,
                    "prompt_preview": prompt[:500],
                    "prompt_tail": prompt[-300:] if len(prompt) > 500 else "",
                },
            )
        except Exception:
            pass

    t0 = time.monotonic()
    for attempt in range(_BACKOFF_RETRIES):
        try:
            client = _get_client()
            result = client.chat.completions.create(
                model=target_model,
                response_model=response_model,
                messages=messages,
                max_retries=_MAX_RETRIES,
            )
            elapsed_ms = (time.monotonic() - t0) * 1000
            if run:
                try:
                    from src.observability.models import EventType
                    result_str = str(result)
                    run.log(
                        EventType.LLM_CALL_COMPLETED,
                        stage=stage,
                        duration_ms=elapsed_ms,
                        data={
                            "model": target_model,
                            "attempts": attempt + 1,
                            "response_model": response_model.__name__,
                            "result_full": result_str,
                            "result_preview": result_str[:400],
                        },
                    )
                except Exception:
                    pass
            return result
        except APIConnectionError as exc:
            last_exc = exc
            wait = _BACKOFF_BASE ** attempt
            print(f"[ollama] connection error (attempt {attempt + 1}/{_BACKOFF_RETRIES}): {exc} — retrying in {wait:.0f}s")
            if run:
                try:
                    from src.observability.models import EventType
                    run.log(
                        EventType.LLM_CALL_RETRY,
                        stage=stage,
                        data={
                            "attempt": attempt + 1,
                            "max_attempts": _BACKOFF_RETRIES,
                            "wait_s": wait,
                            "error": str(exc),
                        },
                    )
                except Exception:
                    pass
            if attempt < _BACKOFF_RETRIES - 1:
                time.sleep(wait)
        except APIStatusError as exc:
            raise LLMUnavailableError(f"LLM API error {exc.status_code}: {exc.message}") from exc
        except Exception as exc:
            raise LLMUnavailableError(f"Unexpected LLM error ({type(exc).__name__}): {exc}") from exc

    raise LLMUnavailableError(f"LLM failed after {_BACKOFF_RETRIES} retries") from last_exc


def call_ollama(prompt: str, model: str | None = None) -> str:
    """
    Send a prompt and return the raw text response.
    Raises LLMUnavailableError on all failures.
    """
    _guard()

    target_model = model or OLLAMA_MODEL
    last_exc: Exception | None = None

    for attempt in range(_BACKOFF_RETRIES):
        try:
            base = _OpenAI(
                base_url=f"{OLLAMA_URL.rstrip('/')}/v1",
                api_key=OLLAMA_API_KEY or "ollama",
                timeout=OLLAMA_TIMEOUT,
            )
            response = base.chat.completions.create(
                model=target_model,
                messages=[{"role": "user", "content": prompt}],
            )
            content = response.choices[0].message.content
            return content or ""
        except APIConnectionError as exc:
            last_exc = exc
            wait = _BACKOFF_BASE ** attempt
            print(f"[ollama] connection error (attempt {attempt + 1}/{_BACKOFF_RETRIES}): {exc} — retrying in {wait:.0f}s")
            if attempt < _BACKOFF_RETRIES - 1:
                time.sleep(wait)
        except Exception as exc:
            raise LLMUnavailableError(f"Unexpected LLM error ({type(exc).__name__}): {exc}") from exc

    raise LLMUnavailableError(f"LLM failed after {_BACKOFF_RETRIES} retries") from last_exc
