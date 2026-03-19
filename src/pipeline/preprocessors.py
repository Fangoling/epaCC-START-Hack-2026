"""
Pre-processor Registry and built-in ops for Stage 3.

Each op signature is one of:
    (df: DataFrame, **params) -> DataFrame
    (df: DataFrame, **params) -> dict[str, DataFrame]   # for split ops

The registry auto-discovers built-in ops at construction. New ops can be
registered at runtime — the LLM planner receives the full op list (name +
docstring) so it can request them by name.
"""

from __future__ import annotations

import base64
import inspect
import re
import time
from typing import Any, Callable

import pandas as pd
from pandas import DataFrame


# ---------------------------------------------------------------------------
# Built-in ops
# ---------------------------------------------------------------------------

def decode_base64_headers(df: DataFrame) -> DataFrame:
    """
    Decode base64-encoded column headers to their original strings.
    Applied when epaAC-Data-5.csv-style files have obfuscated column names.
    Only decodes headers that are valid base64 and decode to printable ASCII.
    """
    new_cols = {}
    for col in df.columns:
        try:
            # Pad to multiple of 4
            padded = col + "=" * (-len(col) % 4)
            decoded = base64.b64decode(padded).decode("latin-1").strip("\x00")
            if decoded.isprintable() and len(decoded) > 0:
                new_cols[col] = decoded
            else:
                new_cols[col] = col
        except Exception:
            new_cols[col] = col
    return df.rename(columns=new_cols)


def split_by_discriminator(
    df: DataFrame,
    column: str,
    values: list[str] | None = None,
) -> dict[str, DataFrame]:
    """
    Split a DataFrame into sub-frames by unique values in a discriminator column.
    Used for medication files where record_type=ADMIN and record_type=ORDER
    represent different logical tables in the same file.
    Returns dict of {value: DataFrame} dropping the discriminator column from each.
    Only non-empty frames are returned.
    """
    if column not in df.columns:
        return {"main": df}

    target_values = values or df[column].dropna().unique().tolist()
    frames: dict[str, DataFrame] = {}
    for val in target_values:
        sub = df[df[column] == val].drop(columns=[column]).reset_index(drop=True)
        if not sub.empty:
            frames[str(val)] = sub
    return frames if frames else {"main": df}


def normalize_id_column(
    df: DataFrame,
    column: str,
    pattern: str | None = None,
) -> DataFrame:
    """
    Strip common ID prefixes (CASE-, PAT-, ENC-, pt_, ID_) and leading zeros
    from an ID column, converting it to a plain integer string.
    pattern: optional regex with a capture group for the numeric part,
             e.g. r'CASE-0*(\d+)' — if omitted, applies default stripping rules.
    """
    if column not in df.columns:
        return df

    def _normalise(val: Any) -> Any:
        if pd.isna(val):
            return val
        s = str(val).strip()
        if pattern:
            m = re.match(pattern, s)
            if m:
                return int(m.group(1))
            return val
        # Default: strip known prefixes and leading zeros
        s = re.sub(r"^(?:CASE|PAT|ENC|pt|ID)[_\-]0*", "", s, flags=re.IGNORECASE)
        # Also strip trailing suffixes like -01 in CASE-0135-01
        s = re.sub(r"-\d+$", "", s)
        s = s.lstrip("0") or "0"
        try:
            return int(s)
        except ValueError:
            return val

    df = df.copy()
    df[column] = df[column].apply(_normalise)
    return df


def aggregate_timeseries(
    df: DataFrame,
    patient_col: str = "patient_id",
    time_col: str = "timestamp",
    interval: str = "1h",
    agg: str = "mean",
) -> DataFrame:
    """
    Downsample a high-frequency time-series DataFrame to a lower frequency.
    Used for device_raw_1hz files to reduce 86400 rows/day/patient to a
    manageable resolution (default: hourly mean).
    interval: pandas offset alias, e.g. '1h', '10min', '1D'.
    agg: aggregation function name, e.g. 'mean', 'max', 'first'.
    """
    if time_col not in df.columns:
        return df

    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if patient_col in df.columns:
        group_keys = [patient_col, pd.Grouper(key=time_col, freq=interval)]
    else:
        group_keys = [pd.Grouper(key=time_col, freq=interval)]

    df = df.set_index(time_col)
    if patient_col in df.columns:
        grouped = df.groupby([patient_col, pd.Grouper(freq=interval)])
    else:
        grouped = df.groupby(pd.Grouper(freq=interval))

    result = getattr(grouped[numeric_cols], agg)().reset_index()
    return result


def pivot_sid_rows(
    df: DataFrame,
    sid_col: str = "SID",
    value_col: str = "SID_value",
    group_cols: list[str] | None = None,
) -> DataFrame:
    """
    Pivot a long-format DataFrame (SID, SID_value columns per row) to wide format.
    Used for epaAC-Data-1..4 files where each assessment item is a separate row.
    group_cols: columns that identify a unique assessment record (e.g. case id, date).
    Only SID codes present in both the DataFrame and the IID-SID mapping are pivoted;
    unknown SIDs are carried as-is with an 'unknown_sid_' prefix.
    """
    if sid_col not in df.columns or value_col not in df.columns:
        return df

    if group_cols is None:
        # Auto-detect: all columns except sid_col and value_col
        group_cols = [c for c in df.columns if c not in (sid_col, value_col)]

    if not group_cols:
        # No grouping columns — just pivot the whole frame
        pivoted = df.set_index(sid_col)[value_col].to_frame().T
        pivoted.columns.name = None
        return pivoted.reset_index(drop=True)

    pivoted = df.pivot_table(
        index=group_cols,
        columns=sid_col,
        values=value_col,
        aggfunc="first",
    ).reset_index()
    pivoted.columns.name = None
    return pivoted


def strip_change_tracking_columns(df: DataFrame) -> DataFrame:
    """
    Drop columns whose names start with ZWrt_ or ZDat_ (SAP change-tracking timestamps).
    These are never part of the EPA target schema.
    """
    drop_cols = [c for c in df.columns if re.match(r"^(ZWrt_|ZDat_)", c, re.IGNORECASE)]
    return df.drop(columns=drop_cols) if drop_cols else df


def detect_and_apply_delimiter(df: DataFrame, file_path: str, delimiter: str) -> DataFrame:
    """
    Re-read the source file with the specified delimiter.
    Used when the auto-detected delimiter was wrong (e.g. single-column frame).
    Returns the correctly-parsed DataFrame.
    """
    return pd.read_csv(file_path, sep=delimiter, encoding="utf-8-sig", low_memory=False)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_BUILTIN_OPS: list[Callable] = [
    decode_base64_headers,
    split_by_discriminator,
    normalize_id_column,
    aggregate_timeseries,
    pivot_sid_rows,
    strip_change_tracking_columns,
    detect_and_apply_delimiter,
]


class PreprocessorRegistry:
    """
    Stores named pre-processor callables. The LLM planner receives
    registry.list_ops() — a list of {name, signature, doc} dicts — so it can
    request ops by exact name when building a PreprocessingPlan.
    """

    def __init__(self) -> None:
        self._ops: dict[str, Callable] = {}
        for fn in _BUILTIN_OPS:
            self.register(fn.__name__, fn)

    def register(self, name: str, fn: Callable) -> None:
        self._ops[name] = fn

    def get(self, name: str) -> Callable | None:
        return self._ops.get(name)

    def list_ops(self) -> list[dict[str, str]]:
        """Return name + signature + first-line docstring for each registered op."""
        result = []
        for name, fn in self._ops.items():
            sig = str(inspect.signature(fn))
            doc = (inspect.getdoc(fn) or "").split("\n")[0]
            result.append({"name": name, "signature": f"{name}{sig}", "description": doc})
        return result

    # -----------------------------------------------------------------------
    # Plan execution
    # -----------------------------------------------------------------------

    def execute_plan(
        self,
        df: DataFrame,
        plan: "PreprocessingPlan",  # noqa: F821
        run: Any | None = None,
    ) -> dict[str, DataFrame]:
        """
        Execute all steps in plan against df. Returns a dict of named DataFrames.
        Ops that return a single DataFrame update the 'main' frame.
        Ops that return a dict (like split_by_discriminator) replace the frame set.
        """
        from src.observability.models import EventType

        frames: dict[str, DataFrame] = {"main": df}

        for step in plan.steps:
            fn = self.get(step.op)
            if fn is None:
                _warn = f"Op {step.op!r} not found in registry — skipping."
                print(f"[pipeline] WARNING: {_warn}")
                if run:
                    run.log(EventType.OP_FAILED, stage=f"op:{step.op}",
                            data={"op": step.op, "error": _warn, "input_rows": 0})
                continue

            total_in = sum(len(f) for f in frames.values())
            t0 = time.monotonic() * 1000

            if run:
                run.log(EventType.OP_STARTED, stage=f"op:{step.op}",
                        data={"op": step.op, "params": step.params, "input_rows": total_in})
            try:
                # Apply to each frame in the current set
                new_frames: dict[str, DataFrame] = {}
                for fname, frame in frames.items():
                    result = fn(frame, **step.params)
                    if isinstance(result, dict):
                        # Split op: prefix sub-frame names to avoid collisions
                        for sub_name, sub_df in result.items():
                            new_frames[f"{fname}_{sub_name}" if fname != "main" else sub_name] = sub_df
                    else:
                        new_frames[fname] = result
                frames = new_frames

                elapsed = time.monotonic() * 1000 - t0
                total_out = sum(len(f) for f in frames.values())
                if run:
                    run.op_completed(
                        op_name=step.op,
                        params=step.params,
                        input_rows=total_in,
                        output_rows=total_out,
                        output_frames=list(frames.keys()),
                        duration_ms=elapsed,
                    )
            except Exception as exc:
                elapsed = time.monotonic() * 1000 - t0
                print(f"[pipeline] Op {step.op!r} failed: {exc}")
                if run:
                    run.log(EventType.OP_FAILED, stage=f"op:{step.op}",
                            duration_ms=elapsed,
                            data={"op": step.op, "params": step.params,
                                  "input_rows": total_in, "error": str(exc)})
                # Keep frames unchanged — skip failed op

        return frames
