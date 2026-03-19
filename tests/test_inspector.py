"""
Unit tests for src/pipeline/inspector.py

Run with:  pytest tests/test_inspector.py -v
"""
import io
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.inspector import (
    _all_headers_base64,
    _classify_category,
    _detect_delimiter,
    _find_discriminator,
    _find_id_columns,
    inspect_file,
)


# ---------------------------------------------------------------------------
# _detect_delimiter
# ---------------------------------------------------------------------------

def _write_tmp(content: str, suffix: str = ".csv") -> Path:
    f = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode="w", encoding="utf-8")
    f.write(content)
    f.close()
    return Path(f.name)


def test_detect_delimiter_semicolon():
    path = _write_tmp("a;b;c\n1;2;3\n")
    assert _detect_delimiter(path, "utf-8") == ";"


def test_detect_delimiter_comma():
    path = _write_tmp("a,b,c\n1,2,3\n")
    assert _detect_delimiter(path, "utf-8") == ","


def test_detect_delimiter_tab():
    path = _write_tmp("a\tb\tc\n1\t2\t3\n")
    assert _detect_delimiter(path, "utf-8") == "\t"


# ---------------------------------------------------------------------------
# _all_headers_base64
# ---------------------------------------------------------------------------

def test_all_headers_base64_true():
    import base64
    encoded = [base64.b64encode(s.encode()).decode() for s in ["sodium", "creatinine", "value"]]
    assert _all_headers_base64(encoded) is True


def test_all_headers_base64_false_for_plain():
    assert _all_headers_base64(["patient_id", "value", "date"]) is False


def test_all_headers_base64_false_for_too_few():
    import base64
    encoded = [base64.b64encode(b"x").decode(), base64.b64encode(b"y").decode()]
    # Less than 3 columns → False
    assert _all_headers_base64(encoded) is False


# ---------------------------------------------------------------------------
# _find_discriminator
# ---------------------------------------------------------------------------

def test_find_discriminator_all_caps_low_cardinality():
    df = pd.DataFrame({"record_type": ["ADMIN", "ADMIN", "ORDER", "ORDER", "ORDER"]})
    col, vals = _find_discriminator(df)
    assert col == "record_type"
    assert set(vals) == {"ADMIN", "ORDER"}


def test_find_discriminator_no_match():
    df = pd.DataFrame({"value": [1, 2, 3, 4, 5]})
    col, vals = _find_discriminator(df)
    assert col is None
    assert vals == []


# ---------------------------------------------------------------------------
# _find_id_columns
# ---------------------------------------------------------------------------

def test_find_id_columns_by_name():
    df = pd.DataFrame({"case_id": ["CASE-001"], "value": [1]})
    cols, pattern = _find_id_columns(df)
    assert "case_id" in cols


def test_find_id_columns_fallid():
    df = pd.DataFrame({"FallID": [135], "x": [1]})
    cols, _ = _find_id_columns(df)
    assert "FallID" in cols


# ---------------------------------------------------------------------------
# _classify_category
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("headers,expected", [
    (["sodium_mmol_L", "creatinine_mg_dL", "specimen_datetime"], "labs"),
    (["medication_code_atc", "route", "dose_unit", "order_id"], "medication"),
    (["primary_icd10_code", "ops_codes", "admission_date"], "icd10_ops"),
    (["nursing_note", "shift", "ward", "report_date"], "nursing"),
    (["EPA0001", "EPAST0BTS", "FallID", "SID", "SID_value"], "epa_ac"),
    (["totally_unknown_col", "another_random_col"], "unknown"),
])
def test_classify_category(headers, expected):
    result = _classify_category(headers, row_count=100)
    assert result == expected


# ---------------------------------------------------------------------------
# inspect_file — integration test with real temp CSV
# ---------------------------------------------------------------------------

def test_inspect_file_basic():
    content = "case_id;medication_code_atc;route;dose_unit;order_id\nCASE-001;A10BK;oral;mg;ORD-001\n"
    path = _write_tmp(content)
    profile = inspect_file(path)
    assert profile.delimiter == ";"
    assert profile.row_count >= 1
    assert profile.data_category == "medication"
    assert "case_id" in profile.id_columns


def test_inspect_file_device_motion():
    content = "patient_id,movement_index,micro_movements,bed_exit,fall_event,impact_magnitude\n"
    content += "001,50,10,0,0,0.5\n"
    path = _write_tmp(content, suffix=".csv")
    profile = inspect_file(path)
    assert profile.data_category in ("device_motion", "device_1hz")


def test_inspect_file_unknown():
    content = "col_a,col_b,col_c\n1,2,3\n4,5,6\n"
    path = _write_tmp(content)
    profile = inspect_file(path)
    # LLM is likely unavailable in tests — should fall back to unknown
    assert profile.data_category in ("unknown", "epa_ac", "labs", "medication",
                                     "icd10_ops", "device_motion", "device_1hz", "nursing")
