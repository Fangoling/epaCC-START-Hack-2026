#!/usr/bin/env python3
"""
Smoke tests for the AI Mapping Agent.
Run: OLLAMA_API_KEY=your_key python test_ollama.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from src.ai_mapping.ollama_client import call_ollama, call_structured
from src.ai_mapping.models import ColumnMappingResult, QualityReport
from src.ai_mapping.agent import MappingAgent
from src.ai_mapping.tools import check_case_exists, lookup_iid_for_column
from src.observability import PipelineRun

SOURCE = "Endtestdaten_mit_Fehlern_ einheitliche ID/synth_labs.csv"
DB_PATH = "output/health_data.db"

# --- Test 1: raw text (no observability needed) ---
print("=== Test 1: raw text ===")
result = call_ollama("Say hello in one sentence.")
print("Response:", result or "FAILED — check OLLAMA_API_KEY")

# --- Tests 2–4: full agent pipeline with observability ---
print("\n=== Tests 2–4: agent pipeline (observed) ===")
with PipelineRun(source_file=SOURCE, log_dir="logs/") as run:
    agent = MappingAgent(
        source_path=SOURCE,
        target_table="tbImportLabsData",
        db_path=DB_PATH,
        run=run,
    )
    print("Context:", agent.describe_context())

    # Test 2: column mapping
    mapping = agent.get_column_mapping()
    if mapping:
        print(f"Mapped {len(mapping.mappings)} columns, {len(mapping.unmapped_columns)} unmapped")
        for m in mapping.mappings[:5]:
            print(f"  {m.source_column!r:30} → {m.target_column!r}  ({m.transformation_note or ''})")
    else:
        print("FAILED — no mapping returned")

    # Test 3: quality report
    report = agent.get_quality_issues()
    if report:
        print(f"Found {report.total} issues ({len(report.errors)} errors, {len(report.warnings)} warnings)")
        for issue in report.issues[:3]:
            print(f"  [{issue.severity}] row {issue.row_index} col={issue.column!r} → {issue.issue_type}")
    else:
        print("No report returned")

    # Test 4: transformation script
    if mapping:
        script = agent.get_transformation_script(mapping)
        if script:
            print("Script preview (first 300 chars):")
            print(script[:300])
        else:
            print("No script returned")

    print("\nRun summary:", run.summary())

# --- Test 5: DB lookup tool (runs without LLM) ---
print("\n=== Test 5: DB lookup tool ===")
iid = lookup_iid_for_column(column_name="Assessment type")
print(f"IID for 'Assessment type': {iid}")

if os.path.exists(DB_PATH):
    result = check_case_exists(case_id=135, db_path=DB_PATH)
    print(f"Case 135: {result}")
else:
    print(f"DB not found at {DB_PATH} — skipping case lookup")

# --- Test 6: full pipeline run (also observed) ---
print("\n=== Test 6: agent.run() ===")
with PipelineRun(source_file=SOURCE, log_dir="logs/") as run:
    agent = MappingAgent(
        source_path=SOURCE,
        target_table="tbImportLabsData",
        db_path=DB_PATH,
        run=run,
    )
    summary = agent.run(case_ids=[135, 22, 78])
    print(f"Pipeline complete: mapping={summary['column_mapping'] is not None}, "
          f"quality={summary['quality_report'] is not None}, "
          f"script={summary['transformation_script'] is not None}, "
          f"case_actions={summary['case_actions']}")
