"""
Integration tests for the full pipeline (no real LLM required).

These tests run with OLLAMA_ENABLED=false (or Ollama simply unreachable),
exercising the heuristic fallbacks end-to-end with a temporary SQLite DB.

Run with:  pytest tests/test_pipeline_integration.py -v
"""
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

os.environ.setdefault("OLLAMA_ENABLED", "false")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(content: str, suffix: str = ".csv") -> Path:
    f = tempfile.NamedTemporaryFile(
        delete=False, suffix=suffix, mode="w", encoding="utf-8"
    )
    f.write(content)
    f.close()
    return Path(f.name)


def _make_db() -> str:
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# Full pipeline smoke test — medication file
# ---------------------------------------------------------------------------

def test_pipeline_medication_csv():
    content = (
        "case_id;record_type;medication_code_atc;route;dose_unit;order_id\n"
        "CASE-001;ADMIN;A10BK;oral;mg;ORD-001\n"
        "CASE-001;ORDER;A10BK;oral;mg;ORD-002\n"
        "CASE-002;ADMIN;B01AC;iv;ml;ORD-003\n"
    )
    csv_path = _write_csv(content)
    db_path = _make_db()

    from src.pipeline.orchestrator import Pipeline

    with tempfile.TemporaryDirectory() as log_dir:
        pipeline = Pipeline(db_path=db_path, log_dir=log_dir)
        result = pipeline.run(csv_path)

    assert result["profile"]["data_category"] == "medication"
    # split_by_discriminator should have produced ADMIN and ORDER frames
    assert any("ADMIN" in k or "ORDER" in k for k in result["frames"])
    assert len(result["routing_results"]) >= 1
    total_rows = sum(r["row_count"] for r in result["routing_results"])
    assert total_rows == 3


# ---------------------------------------------------------------------------
# Full pipeline smoke test — labs file
# ---------------------------------------------------------------------------

def test_pipeline_labs_csv():
    content = (
        "case_id,sodium_mmol_L,creatinine_mg_dL,specimen_datetime\n"
        "101,138,0.9,2024-01-01\n"
        "102,140,1.1,2024-01-02\n"
    )
    csv_path = _write_csv(content)
    db_path = _make_db()

    from src.pipeline.orchestrator import Pipeline

    with tempfile.TemporaryDirectory() as log_dir:
        pipeline = Pipeline(db_path=db_path, log_dir=log_dir)
        result = pipeline.run(csv_path)

    assert result["profile"]["data_category"] == "labs"
    assert result["frames"]["main"] == 2


# ---------------------------------------------------------------------------
# Full pipeline smoke test — epaAC long-format (SID pivot)
# ---------------------------------------------------------------------------

def test_pipeline_epa_ac_pivot():
    content = (
        "FallID;EPA;SID;SID_value\n"
        "135;EPA0001;EPA001;Yes\n"
        "135;EPA0001;EPA002;No\n"
        "136;EPA0001;EPA001;Yes\n"
        "136;EPA0001;EPA002;Yes\n"
    )
    csv_path = _write_csv(content)
    db_path = _make_db()

    from src.pipeline.orchestrator import Pipeline

    with tempfile.TemporaryDirectory() as log_dir:
        pipeline = Pipeline(db_path=db_path, log_dir=log_dir)
        result = pipeline.run(csv_path)

    assert result["profile"]["data_category"] == "epa_ac"
    # After pivot, should have 2 rows (one per FallID)
    assert result["frames"]["main"] == 2


# ---------------------------------------------------------------------------
# run_all batch processing
# ---------------------------------------------------------------------------

def test_pipeline_run_all():
    with tempfile.TemporaryDirectory() as data_dir:
        db_path = _make_db()

        # Write two small CSV files
        (Path(data_dir) / "labs.csv").write_text(
            "case_id,sodium_mmol_L,creatinine_mg_dL,specimen_datetime\n101,138,0.9,2024-01-01\n"
        )
        (Path(data_dir) / "nursing.csv").write_text(
            "case_id,nursing_note,shift,ward,report_date\n102,all good,morning,cardio,2024-01-01\n"
        )

        from src.pipeline.orchestrator import Pipeline

        with tempfile.TemporaryDirectory() as log_dir:
            pipeline = Pipeline(db_path=db_path, log_dir=log_dir)
            results = pipeline.run_all(data_dir)

    assert len(results) == 2
    for r in results:
        assert "routing_results" in r or "error" in r


# ---------------------------------------------------------------------------
# Observability: run summary contains expected keys
# ---------------------------------------------------------------------------

def test_pipeline_run_summary_keys():
    content = (
        "case_id,sodium_mmol_L,creatinine_mg_dL,specimen_datetime\n"
        "101,138,0.9,2024-01-01\n"
    )
    csv_path = _write_csv(content)
    db_path = _make_db()

    from src.pipeline.orchestrator import Pipeline

    with tempfile.TemporaryDirectory() as log_dir:
        pipeline = Pipeline(db_path=db_path, log_dir=log_dir)
        result = pipeline.run(csv_path)

    summary = result["run_summary"]
    assert "run_id" in summary
    assert "source_file" in summary
    assert "ops_executed" in summary
    assert "total_events" in summary
