#!/usr/bin/env python3
"""
Quick smoke test for the Ollama Cloud connection.
Run: OLLAMA_API_KEY=your_key python test_ollama.py
"""

import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))

from src.ai_mapping.ollama_client import call_ollama, call_ollama_json

# --- Test 1: plain text response ---
print("=== Test 1: plain text ===")
result = call_ollama("Say hello in one sentence.")
if result:
    print("Response:", result)
else:
    print("FAILED — check OLLAMA_API_KEY and model availability")

# --- Test 2: JSON response ---
print("\n=== Test 2: JSON output ===")
result = call_ollama_json(
    'Return a JSON object with keys "status" and "message". '
    'Set status to "ok" and message to "connection successful".'
)
if result:
    print("Parsed JSON:", result)
else:
    print("FAILED — could not get or parse JSON response")
