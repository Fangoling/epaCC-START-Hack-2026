"""
Ollama client for the AI Mapping Agent.
Uses the ollama Python SDK to call Ollama Cloud (https://ollama.com).
Falls back gracefully when Ollama is unavailable or API key is missing.
"""

import json
import os
import re

from ollama import Client

OLLAMA_ENABLED = os.getenv("OLLAMA_ENABLED", "true").lower() == "true"
OLLAMA_URL = os.getenv("OLLAMA_URL", "https://ollama.com")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:20b")
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "")


def _get_client() -> Client:
    return Client(
        host=OLLAMA_URL,
        headers={"Authorization": f"Bearer {OLLAMA_API_KEY}"},
    )


def call_ollama(prompt: str, model: str | None = None) -> str | None:
    """
    Send a prompt to Ollama Cloud and return the raw text response.
    Returns None if Ollama is disabled, API key is missing, or the call fails.
    """
    if not OLLAMA_ENABLED:
        return None
    if not OLLAMA_API_KEY:
        print("[ai_mapping] OLLAMA_API_KEY not set — falling back.")
        return None

    target_model = model or OLLAMA_MODEL
    try:
        client = _get_client()
        response = client.chat(
            model=target_model,
            messages=[{"role": "user", "content": prompt}],
        )
        return response["message"]["content"]
    except Exception as exc:
        print(f"[ai_mapping] Ollama error: {exc} — falling back.")
        return None


def call_ollama_json(prompt: str, model: str | None = None) -> dict | list | None:
    """
    Send a prompt expecting a JSON response.
    Strips markdown fences and extracts the first JSON object or array.
    Returns None if the call fails or no valid JSON is found.
    """
    raw = call_ollama(prompt, model=model)
    if raw is None:
        return None

    # Strip markdown code fences if present
    raw = re.sub(r"```(?:json)?\s*", "", raw).strip()

    # Try full response first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Extract first JSON object or array
    match = re.search(r"(\{.*\}|\[.*\])", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    print(f"[ai_mapping] Could not parse JSON from Ollama response:\n{raw[:500]}")
    return None
