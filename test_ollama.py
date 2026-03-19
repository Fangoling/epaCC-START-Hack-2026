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
from src.ai_mapping.agent import MappingAgent

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

# --- Test 3: Full agent — column mapping ---
print("\n=== Test 3: MappingAgent column mapping ===")
SOURCE = "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_labs.csv"
agent = MappingAgent(source_path=SOURCE, target_table="tbImportLabsData")
print("Context:", agent.describe_context())
column_map = agent.get_column_mapping()
if column_map:
    print("Column map:", column_map)
else:
    print("No column map returned (Ollama unavailable or failed)")

# --- Test 4: Quality issues ---
print("\n=== Test 4: MappingAgent quality issues ===")
issues = agent.get_quality_issues()
if issues:
    for issue in issues:
        print(f"  [{issue.get('severity')}] row {issue.get('row_index')} "
              f"col={issue.get('column')} val={issue.get('original_value')!r} "
              f"→ {issue.get('issue_type')}")
else:
    print("No issues returned")
