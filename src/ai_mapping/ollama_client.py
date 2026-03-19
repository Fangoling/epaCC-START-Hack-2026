"""
Ollama client for the AI Mapping Agent.
Uses instructor + openai-compatible API for validated structured output.
Adds exponential backoff retry for connection failures.
"""

from __future__ import annotations

import os
import time
from typing import TypeVar, Type

from pydantic import BaseModel

# instructor and openai are optional at import time — loaded lazily in _get_client()
# so that the rest of the pipeline can be imported and tested without these packages.
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

_MAX_RETRIES = 3          # instructor retries on validation failure
_BACKOFF_RETRIES = 3      # retries on connection error
_BACKOFF_BASE = 2.0       # seconds — doubles each attempt

T = TypeVar("T", bound=BaseModel)


def _get_client():
    """Return an instructor-patched OpenAI client pointing at Ollama Cloud."""
    base = _OpenAI(
        base_url=f"{OLLAMA_URL.rstrip('/')}/v1",
        api_key=OLLAMA_API_KEY or "ollama",   # key required by openai SDK even if unused
        timeout=OLLAMA_TIMEOUT,
    )
    # JSON mode: works with any model; tool-call mode requires function-calling support
    return _instructor.from_openai(base, mode=_instructor.Mode.JSON)


def _guard() -> bool:
    """Return False (with a log message) if Ollama is disabled, key missing, or libs absent."""
    print(f"[ollama] _guard: LLM_AVAILABLE={_LLM_AVAILABLE} ENABLED={OLLAMA_ENABLED} KEY_SET={bool(OLLAMA_API_KEY)}")
    if not _LLM_AVAILABLE:
        print("[ollama] _guard: instructor/openai not installed — falling back.")
        return False
    if not OLLAMA_ENABLED:
        print("[ollama] _guard: OLLAMA_ENABLED=false — falling back.")
        return False
    if not OLLAMA_API_KEY:
        print("[ollama] _guard: OLLAMA_API_KEY not set — falling back.")
        return False
    print(f"[ollama] _guard: OK — url={OLLAMA_URL} model={OLLAMA_MODEL} timeout={OLLAMA_TIMEOUT}s")
    return True


# ---------------------------------------------------------------------------
# Structured output (primary interface)
# ---------------------------------------------------------------------------

def call_structured(
    prompt: str,
    response_model: Type[T],
    model: str | None = None,
    system: str | None = None,
) -> T | None:
    """
    Send a prompt and return a validated Pydantic model instance.
    instructor handles JSON extraction + validation + retry on parse failure.
    Exponential backoff handles connection errors.

    Args:
        prompt:         User prompt text.
        response_model: Pydantic model class to validate against.
        model:          Override the default Ollama model.
        system:         Optional system message.

    Returns:
        Validated Pydantic instance, or None if unavailable/failed.
    """
    print(f"[ollama] call_structured: model={model or OLLAMA_MODEL} response_model={response_model.__name__}")
    if not _guard():
        return None

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    target_model = model or OLLAMA_MODEL
    print(f"[ollama] call_structured: sending request to {OLLAMA_URL}/v1 prompt_len={len(prompt)}")

    for attempt in range(_BACKOFF_RETRIES):
        try:
            print(f"[ollama] call_structured: attempt {attempt + 1}/{_BACKOFF_RETRIES}")
            client = _get_client()
            result = client.chat.completions.create(
                model=target_model,
                response_model=response_model,
                messages=messages,
                max_retries=_MAX_RETRIES,
            )
            print(f"[ollama] call_structured: success — got {response_model.__name__}")
            return result
        except APIConnectionError as exc:
            wait = _BACKOFF_BASE ** attempt
            print(f"[ollama] call_structured: connection error (attempt {attempt + 1}/{_BACKOFF_RETRIES}): {exc} — retrying in {wait:.0f}s")
            if attempt < _BACKOFF_RETRIES - 1:
                time.sleep(wait)
        except APIStatusError as exc:
            print(f"[ollama] call_structured: API error {exc.status_code}: {exc.message} — falling back.")
            return None
        except Exception as exc:
            print(f"[ollama] call_structured: unexpected error ({type(exc).__name__}): {exc} — falling back.")
            return None

    print("[ollama] call_structured: all retries exhausted — falling back.")
    return None


# ---------------------------------------------------------------------------
# Raw text output (for transformation script generation)
# ---------------------------------------------------------------------------

def call_ollama(prompt: str, model: str | None = None) -> str | None:
    """
    Send a prompt and return the raw text response (no structured parsing).
    Used for transformation script generation where output is Python code.
    Includes exponential backoff.
    """
    print(f"[ollama] call_ollama: model={model or OLLAMA_MODEL}")
    if not _guard():
        return None

    target_model = model or OLLAMA_MODEL
    print(f"[ollama] call_ollama: sending raw request to {OLLAMA_URL}/v1 prompt_len={len(prompt)}")

    for attempt in range(_BACKOFF_RETRIES):
        try:
            print(f"[ollama] call_ollama: attempt {attempt + 1}/{_BACKOFF_RETRIES}")
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
            print(f"[ollama] call_ollama: success — response_len={len(content or '')}")
            return content
        except APIConnectionError as exc:
            wait = _BACKOFF_BASE ** attempt
            print(f"[ollama] call_ollama: connection error (attempt {attempt + 1}/{_BACKOFF_RETRIES}): {exc} — retrying in {wait:.0f}s")
            if attempt < _BACKOFF_RETRIES - 1:
                time.sleep(wait)
        except Exception as exc:
            print(f"[ollama] call_ollama: unexpected error ({type(exc).__name__}): {exc} — falling back.")
            return None

    print("[ollama] call_ollama: all retries exhausted — falling back.")
    return None
