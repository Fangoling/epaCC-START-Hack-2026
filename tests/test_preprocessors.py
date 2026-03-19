"""
Unit tests for src/pipeline/preprocessors.py

Run with:  pytest tests/test_preprocessors.py -v
"""
import pandas as pd
import pytest

from src.pipeline.preprocessors import (
    PreprocessorRegistry,
    aggregate_timeseries,
    decode_base64_headers,
    normalize_id_column,
    pivot_sid_rows,
    split_by_discriminator,
    strip_change_tracking_columns,
)


# ---------------------------------------------------------------------------
# decode_base64_headers
# ---------------------------------------------------------------------------

def test_decode_base64_headers_decodes_valid():
    import base64
    encoded = base64.b64encode(b"sodium_mmol_L").decode()
    df = pd.DataFrame({encoded: [1, 2, 3]})
    result = decode_base64_headers(df)
    assert "sodium_mmol_L" in result.columns


def test_decode_base64_headers_leaves_plain_columns():
    df = pd.DataFrame({"patient_id": [1], "value": [2]})
    result = decode_base64_headers(df)
    assert list(result.columns) == ["patient_id", "value"]


# ---------------------------------------------------------------------------
# split_by_discriminator
# ---------------------------------------------------------------------------

def test_split_by_discriminator_splits_correctly():
    df = pd.DataFrame({
        "record_type": ["ADMIN", "ADMIN", "ORDER"],
        "value": [1, 2, 3],
    })
    frames = split_by_discriminator(df, column="record_type")
    assert set(frames.keys()) == {"ADMIN", "ORDER"}
    assert len(frames["ADMIN"]) == 2
    assert len(frames["ORDER"]) == 1
    # discriminator column should be dropped
    assert "record_type" not in frames["ADMIN"].columns


def test_split_by_discriminator_missing_column_returns_main():
    df = pd.DataFrame({"value": [1, 2]})
    frames = split_by_discriminator(df, column="nonexistent")
    assert list(frames.keys()) == ["main"]


def test_split_by_discriminator_respects_values_filter():
    df = pd.DataFrame({
        "record_type": ["ADMIN", "ORDER", "EXTRA"],
        "v": [1, 2, 3],
    })
    frames = split_by_discriminator(df, column="record_type", values=["ADMIN"])
    assert "ADMIN" in frames
    assert "EXTRA" not in frames


# ---------------------------------------------------------------------------
# normalize_id_column
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("CASE-0135-01", 135),
    ("CASE-0135", 135),
    ("0135", 135),
    ("135", 135),
    ("PAT-0042", 42),
])
def test_normalize_id_column(raw, expected):
    df = pd.DataFrame({"case_id": [raw]})
    result = normalize_id_column(df, column="case_id")
    assert result["case_id"].iloc[0] == expected


def test_normalize_id_column_missing_column_returns_unchanged():
    df = pd.DataFrame({"value": [1]})
    result = normalize_id_column(df, column="nonexistent")
    assert list(result.columns) == ["value"]


def test_normalize_id_column_preserves_nan():
    df = pd.DataFrame({"case_id": [None, "CASE-001"]})
    result = normalize_id_column(df, column="case_id")
    assert pd.isna(result["case_id"].iloc[0])
    assert result["case_id"].iloc[1] == 1


# ---------------------------------------------------------------------------
# aggregate_timeseries
# ---------------------------------------------------------------------------

def test_aggregate_timeseries_reduces_rows():
    import numpy as np
    timestamps = pd.date_range("2024-01-01", periods=120, freq="1min")
    df = pd.DataFrame({
        "timestamp": timestamps,
        "value": np.random.rand(120),
    })
    result = aggregate_timeseries(df, time_col="timestamp", interval="1h")
    # 120 minutes → 2 hourly buckets
    assert len(result) == 2


def test_aggregate_timeseries_missing_time_col_returns_unchanged():
    df = pd.DataFrame({"value": [1, 2, 3]})
    result = aggregate_timeseries(df, time_col="timestamp")
    assert len(result) == 3


# ---------------------------------------------------------------------------
# pivot_sid_rows
# ---------------------------------------------------------------------------

def test_pivot_sid_rows_basic():
    df = pd.DataFrame({
        "FallID": [1, 1, 2, 2],
        "SID": ["EPA001", "EPA002", "EPA001", "EPA002"],
        "SID_value": ["A", "B", "C", "D"],
    })
    result = pivot_sid_rows(df, sid_col="SID", value_col="SID_value", group_cols=["FallID"])
    assert "EPA001" in result.columns
    assert "EPA002" in result.columns
    assert len(result) == 2


def test_pivot_sid_rows_missing_columns_returns_unchanged():
    df = pd.DataFrame({"a": [1], "b": [2]})
    result = pivot_sid_rows(df, sid_col="SID", value_col="SID_value")
    assert list(result.columns) == ["a", "b"]


# ---------------------------------------------------------------------------
# strip_change_tracking_columns
# ---------------------------------------------------------------------------

def test_strip_change_tracking_columns():
    df = pd.DataFrame({
        "value": [1],
        "ZWrt_timestamp": ["2024-01-01"],
        "ZDat_user": ["admin"],
    })
    result = strip_change_tracking_columns(df)
    assert "value" in result.columns
    assert "ZWrt_timestamp" not in result.columns
    assert "ZDat_user" not in result.columns


def test_strip_change_tracking_columns_no_match_unchanged():
    df = pd.DataFrame({"a": [1], "b": [2]})
    result = strip_change_tracking_columns(df)
    assert list(result.columns) == ["a", "b"]


# ---------------------------------------------------------------------------
# PreprocessorRegistry
# ---------------------------------------------------------------------------

def test_registry_lists_all_builtin_ops():
    registry = PreprocessorRegistry()
    ops = {o["name"] for o in registry.list_ops()}
    expected = {
        "decode_base64_headers", "split_by_discriminator", "normalize_id_column",
        "aggregate_timeseries", "pivot_sid_rows", "strip_change_tracking_columns",
        "detect_and_apply_delimiter",
    }
    assert expected.issubset(ops)


def test_registry_execute_plan_applies_steps():
    from src.pipeline.models import PreprocessingPlan, PreprocessingStep

    registry = PreprocessorRegistry()
    df = pd.DataFrame({
        "case_id": ["CASE-0001", "CASE-0002"],
        "ZWrt_ts": ["t1", "t2"],
        "value": [10, 20],
    })
    plan = PreprocessingPlan(
        steps=[
            PreprocessingStep(op="strip_change_tracking_columns", params={}, rationale="drop tracking"),
            PreprocessingStep(op="normalize_id_column", params={"column": "case_id"}, rationale="normalize"),
        ],
        llm_rationale="test plan",
    )
    frames = registry.execute_plan(df, plan)
    assert "main" in frames
    result = frames["main"]
    assert "ZWrt_ts" not in result.columns
    assert result["case_id"].iloc[0] == 1


def test_registry_execute_plan_handles_split_ops():
    from src.pipeline.models import PreprocessingPlan, PreprocessingStep

    registry = PreprocessorRegistry()
    df = pd.DataFrame({
        "record_type": ["ADMIN", "ORDER"],
        "value": [1, 2],
    })
    plan = PreprocessingPlan(
        steps=[
            PreprocessingStep(
                op="split_by_discriminator",
                params={"column": "record_type"},
                rationale="split",
            )
        ],
        llm_rationale="split test",
    )
    frames = registry.execute_plan(df, plan)
    assert "ADMIN" in frames
    assert "ORDER" in frames
