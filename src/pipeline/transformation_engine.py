"""
Transformation Engine — converts a raw DataFrame into a normalised form
ready for DB insertion, driven entirely by SchemaConfig.

Steps (in order):
  1. Skip noise rows (header_row_index > 0)
  2. Drop flagged columns
  3. Decode column names (base64 / SAP EPA encoding)
  4. If long-format: pivot to wide
  5. Rename columns: SID/IID → DDL column names
  5b. Deduplicate columns (multiple source cols mapping to same DDL target)
  6. Apply per-category cleaning to cell values
  7. Normalise case/patient ID columns

Returns (cleaned_df, unmapped_columns).
"""

from __future__ import annotations

import base64
import re
from typing import TYPE_CHECKING

import pandas as pd

from src.ai_mapping.cleaners import (
    clean_numeric, clean_integer, clean_text, parse_date,
    normalize_flag, normalize_gender, normalize_boolean,
    normalize_id, is_null,
)

if TYPE_CHECKING:
    from src.pipeline.models import SchemaConfig
    from src.pipeline.mapping_engine import MappingEngine
    from src.observability import PipelineRun


def transform(
    raw_df: pd.DataFrame,
    config: "SchemaConfig",
    mapping_engine: "MappingEngine",
    run: "PipelineRun | None" = None,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Apply all transformations and return (normalised_df, unmapped_columns).
    unmapped_columns lists headers that could not be resolved to a DDL column.
    """
    from src.observability.models import EventType

    df = raw_df.copy()

    if run:
        run.log(
            EventType.TRANSFORM_STARTED,
            stage="transform",
            data={
                "rows": len(df),
                "columns": len(df.columns),
                "category": config.data_category,
                "format_type": config.format_type,
                "header_row_index": config.header_row_index,
            },
        )

    # 1. Skip noise rows
    if config.header_row_index > 0:
        rows_before = len(df)
        df = _skip_noise_rows(df, config.header_row_index)
        if run:
            run.log(EventType.TRANSFORM_STEP, stage="transform", data={
                "step": "skip_noise_rows",
                "header_row_index": config.header_row_index,
                "rows_before": rows_before,
                "rows_after": len(df),
            })

    # 2. Drop flagged columns
    cols_before = list(df.columns)
    dropped: list[str] = []
    drop_samples: dict[str, list] = {}

    if config.columns_to_drop:
        drop_targets = [c for c in config.columns_to_drop if c in df.columns]
        # Capture sample values before dropping
        for col in drop_targets:
            vals = df[col].dropna().head(3).tolist()
            drop_samples[col] = [str(v) for v in vals]
        df = df.drop(columns=drop_targets, errors="ignore")
        dropped.extend(drop_targets)

    # Also drop change-tracking columns ZWrt_* and ZDat_*
    change_cols = [c for c in df.columns if re.match(r"^Z(Wrt|Dat)_", str(c))]
    if change_cols:
        for col in change_cols:
            vals = df[col].dropna().head(2).tolist()
            drop_samples[col] = [str(v) for v in vals]
        df = df.drop(columns=change_cols)
        dropped.extend(change_cols)

    if run and dropped:
        run.log(EventType.TRANSFORM_STEP, stage="transform", data={
            "step": "drop_columns",
            "dropped": dropped,
            "dropped_samples": drop_samples,
            "columns_before": len(cols_before),
            "columns_after": len(df.columns),
        })

    # 2b. Detect IID codes in first data row (Data-3: row 0 = German, row 1 = IID)
    if len(df) > 0 and not (config.epa and config.epa.encoding_type):
        first_row_vals = [str(v).strip() for v in df.iloc[0].values]
        iid_count = sum(1 for v in first_row_vals if _looks_like_iid(v))
        if iid_count >= len(first_row_vals) * 0.3 and iid_count >= 5:
            new_header = first_row_vals
            df = df.iloc[1:].reset_index(drop=True)
            df.columns = new_header
            if run:
                run.log(EventType.TRANSFORM_STEP, stage="transform", data={
                    "step": "iid_header_detection",
                    "iid_columns_found": iid_count,
                    "total_columns": len(new_header),
                    "sample_headers": new_header[:5],
                })

    # 3. Decode column names if EPA encoded
    if config.epa and config.epa.encoding_type:
        cols_before_decode = list(df.columns)
        df = _decode_columns(df, config.epa.encoding_type)
        if run:
            run.log(EventType.TRANSFORM_STEP, stage="transform", data={
                "step": "decode_columns",
                "encoding_type": config.epa.encoding_type,
                "sample_before": cols_before_decode[:5],
                "sample_after": list(df.columns)[:5],
            })

    # 4. Pivot long → wide if needed
    if config.format_type == "long" and config.epa:
        rows_before_pivot = len(df)
        cols_before_pivot = len(df.columns)
        df = _pivot_long_to_wide(df, config)
        if run:
            run.log(EventType.TRANSFORM_STEP, stage="transform", data={
                "step": "pivot_long_to_wide",
                "sid_column": config.epa.sid_column if config.epa else None,
                "value_column": config.epa.value_column if config.epa else None,
                "rows_before": rows_before_pivot,
                "rows_after": len(df),
                "columns_before": cols_before_pivot,
                "columns_after": len(df.columns),
            })

    # 5. Rename SID/IID columns → DDL names
    df, unmapped = _rename_to_ddl_return(df, mapping_engine, run=run)
    if run:
        mapped_count = len(df.columns) - len(unmapped)
        run.log(EventType.TRANSFORM_STEP, stage="transform", data={
            "step": "rename_to_ddl",
            "mapped": mapped_count,
            "unmapped": len(unmapped),
            "unmapped_sample": unmapped[:10],
        })

    # 5b. Deduplicate columns (multiple source columns may map to same DDL target)
    cols_before_dedup = len(df.columns)
    df = _deduplicate_columns(df, run=run)
    if run and len(df.columns) < cols_before_dedup:
        run.log(EventType.TRANSFORM_STEP, stage="transform", data={
            "step": "deduplicate_columns_summary",
            "columns_before": cols_before_dedup,
            "columns_after": len(df.columns),
            "columns_removed": cols_before_dedup - len(df.columns),
        })

    # 6. Per-category cell cleaning
    cleaned_columns = _get_cleaning_plan(df, config.data_category)
    df = _apply_category_cleaning(df, config.data_category)
    if run:
        run.log(EventType.TRANSFORM_STEP, stage="transform", data={
            "step": "category_cleaning",
            "category": config.data_category,
            "cleaning_plan": cleaned_columns,
            "total_columns": len(df.columns),
            "columns_cleaned": len(cleaned_columns),
        })

    # 7. Normalise ID columns
    df = _normalise_id_columns(df, config)
    if run:
        run.log(EventType.TRANSFORM_STEP, stage="transform", data={
            "step": "normalise_id_columns",
            "case_id_column": config.case_id_column,
            "patient_id_column": config.patient_id_column,
        })

    if run:
        run.log(
            EventType.TRANSFORM_COMPLETED,
            stage="transform",
            data={
                "rows_out": len(df),
                "columns_out": len(df.columns),
                "unmapped_count": len(unmapped),
                "unmapped_columns": unmapped,
            },
        )

    return df, unmapped


# ---------------------------------------------------------------------------
# Step 1: Skip noise rows
# ---------------------------------------------------------------------------

def _skip_noise_rows(df: pd.DataFrame, header_row_index: int) -> pd.DataFrame:
    """
    When header is not on row 0, the actual column names are in data row
    (header_row_index - 1) after pandas reads with default header=0.
    Re-read the DataFrame with the correct header row.
    """
    # The df was already read; column names are the first pandas header row.
    # Row 0 in df.values corresponds to the second file row.
    # For header_row_index=1: row 0 of df contains the real headers.
    if header_row_index < 1 or len(df) < header_row_index:
        return df

    new_header = df.iloc[header_row_index - 1].astype(str).tolist()
    df = df.iloc[header_row_index:].reset_index(drop=True)
    df.columns = new_header
    return df


# ---------------------------------------------------------------------------
# Step 3: Decode column names
# ---------------------------------------------------------------------------

def _decode_columns(df: pd.DataFrame, encoding_type: str) -> pd.DataFrame:
    if encoding_type == "sap_epa":
        df.columns = [_map_sap_epa(c) or c for c in df.columns]
    elif encoding_type == "base64":
        new_cols = []
        for c in df.columns:
            try:
                padded = c + "=" * (-len(c) % 4)
                decoded = base64.b64decode(padded).decode("utf-8", errors="replace")
                new_cols.append(decoded)
            except Exception:
                new_cols.append(c)
        df.columns = new_cols
    return df


_SAP_EPA_RE = re.compile(r"^EPA(\d{2})(\d{2,})$")


def _map_sap_epa(col: str) -> str | None:
    """EPA{ss}{ii...} → coE{series}I{item:03d}  (handles 7+ char EPA codes)"""
    m = _SAP_EPA_RE.match(col)
    if m:
        series = int(m.group(1))
        item = int(m.group(2))
        return f"coE{series}I{item:03d}"
    return None


# ---------------------------------------------------------------------------
# Step 4: Pivot long → wide
# ---------------------------------------------------------------------------

def _pivot_long_to_wide(df: pd.DataFrame, config: "SchemaConfig") -> pd.DataFrame:
    """
    Pivot a long-format EPA DataFrame where each row is one SID observation.
    Requires config.epa.sid_column and config.epa.value_column.
    """
    epa = config.epa
    if not epa or not epa.sid_column or not epa.value_column:
        return df

    id_col = config.case_id_column or config.patient_id_column
    if not id_col or id_col not in df.columns:
        # Attempt to find an ID column
        for candidate in ("case_id", "FallID", "PATFAL", "patient_id", "coCaseId"):
            if candidate in df.columns:
                id_col = candidate
                break

    if not id_col:
        return df  # Cannot pivot without an ID column

    sid_col = epa.sid_column
    val_col = epa.value_column

    if sid_col not in df.columns or val_col not in df.columns:
        return df

    try:
        pivoted = df.pivot_table(
            index=id_col,
            columns=sid_col,
            values=val_col,
            aggfunc="first",
        ).reset_index()
        pivoted.columns.name = None
        return pivoted
    except Exception:
        return df


# ---------------------------------------------------------------------------
# Step 5: Rename SID/IID → DDL
# ---------------------------------------------------------------------------

_IID_RAW_RE = re.compile(r"^E\d+_I_\d+")


def _looks_like_iid(val: str) -> bool:
    """Check if a value looks like an IID code (e.g. 'E2_I_222' or 'E2_I_222 (E3_I_0889)(STRING)')."""
    return bool(_IID_RAW_RE.match(val))


_DDL_PATTERN = re.compile(r"^co[A-Z]\d+I\d+$")
_IID_SUFFIX_RE = re.compile(r"\s*\(.*?\)\s*$")


def _strip_iid_suffix(col: str) -> str:
    """Strip SAP-style suffixes like ' (E3_I_0889)(STRING)' or ' (STRING)'."""
    return _IID_SUFFIX_RE.sub("", col).strip()


def _rename_to_ddl_return(
    df: pd.DataFrame,
    mapping_engine: "MappingEngine",
    run: "PipelineRun | None" = None,
) -> tuple[pd.DataFrame, list[str]]:
    rename_map: dict[str, str] = {}
    unmapped: list[str] = []

    for col in df.columns:
        raw = str(col)

        # 1. Already a DDL column name (e.g. coE0I001 from SAP decode)
        if _DDL_PATTERN.match(raw):
            if run:
                run.column_resolved(raw, raw, method="already_ddl")
            continue

        # 2. Try exact match first (IID or SID)
        ddl = mapping_engine.resolve(raw)
        if ddl:
            method = "iid_lookup" if mapping_engine.is_iid(raw) else "sid_lookup"
            rename_map[col] = ddl
            if run:
                iid = mapping_engine.sid_to_iid(raw) if mapping_engine.is_sid(raw) else raw
                run.column_resolved(raw, ddl, method=method,
                                    transformation_note=f"{raw} → IID={iid} → DDL={ddl}")
            continue

        # 3. Strip IID suffixes like "(E3_I_0889)(STRING)" and retry
        stripped = _strip_iid_suffix(raw)
        if stripped != raw:
            ddl = mapping_engine.resolve(stripped)
            if ddl:
                rename_map[col] = ddl
                if run:
                    run.column_resolved(raw, ddl, method="iid_suffix_strip",
                                        transformation_note=f"{raw} → stripped={stripped} → DDL={ddl}")
                continue

        unmapped.append(raw)
        if run:
            run.column_unmapped(raw, reason=f"no DDL mapping: not DDL pattern, not IID, not SID, stripped='{stripped}'")

    df = df.rename(columns=rename_map)
    return df, unmapped


# ---------------------------------------------------------------------------
# Step 5b: Deduplicate columns with same target name
# ---------------------------------------------------------------------------

def _deduplicate_columns(
    df: pd.DataFrame,
    run: "PipelineRun | None" = None,
) -> pd.DataFrame:
    """
    Handle duplicate column names by coalescing values (first non-null wins).
    
    This can happen when multiple source columns (e.g., 'E2_I_070' and 
    'E2_I_070 (E3_I_0237)(STRING)') both map to the same DDL column ('coE2I070').
    
    Without this fix, pandas concatenates values from duplicate columns,
    producing corrupted data like '442070.0207064' instead of '44' or '2070.02'.
    """
    cols = df.columns.tolist()
    seen: dict[str, int] = {}
    duplicates: dict[str, list[int]] = {}
    
    # Find duplicate column indices
    for i, col in enumerate(cols):
        if col in seen:
            if col not in duplicates:
                duplicates[col] = [seen[col]]
            duplicates[col].append(i)
        else:
            seen[col] = i
    
    if not duplicates:
        return df
    
    # Log duplicate detection
    if run:
        from src.observability.models import EventType
        run.log(EventType.TRANSFORM_STEP, stage="transform", data={
            "step": "deduplicate_columns",
            "duplicate_columns": {k: len(v) for k, v in duplicates.items()},
            "total_duplicates": sum(len(v) for v in duplicates.values()),
        })
    
    # Coalesce duplicate columns: for each row, take first non-null value
    for col_name, indices in duplicates.items():
        # Get all columns with this name as a DataFrame
        dup_df = df.iloc[:, indices]
        
        # Coalesce: for each row, take first non-null value across duplicate columns
        coalesced = dup_df.bfill(axis=1).iloc[:, 0]
        
        # Replace the first occurrence with coalesced values
        df.iloc[:, indices[0]] = coalesced
    
    # Drop all duplicate columns (keep only first occurrence of each)
    cols_to_keep = []
    seen_names = set()
    for i, col in enumerate(cols):
        if col not in seen_names:
            cols_to_keep.append(i)
            seen_names.add(col)
    
    df = df.iloc[:, cols_to_keep]
    
    return df


# ---------------------------------------------------------------------------
# Step 6: Per-category cell cleaning
# ---------------------------------------------------------------------------

_NUMERIC_DTYPE_PATTERNS = re.compile(r"^co[A-Z]\d[A-Z]\d{3}$")  # coE2I042 style


def _get_cleaning_plan(df: pd.DataFrame, category: str) -> dict[str, str]:
    """Return a dict of column → cleaning_function describing what will be cleaned."""
    plan: dict[str, str] = {}
    if category == "epa_ac":
        for col in df.columns:
            if _NUMERIC_DTYPE_PATTERNS.match(col):
                plan[col] = "clean_numeric"
    elif category == "labs":
        date_hints = {"specimen_datetime", "result_datetime", "collection_date"}
        flag_hints = {"result_flag", "flag", "abnormal_flag"}
        numeric_hints = {"result_value", "lower_ref", "upper_ref"}
        for col in df.columns:
            cl = col.lower()
            if any(h in cl for h in date_hints):
                plan[col] = "parse_date"
            elif any(h in cl for h in flag_hints):
                plan[col] = "normalize_flag"
            elif any(h in cl for h in numeric_hints):
                plan[col] = "clean_numeric"
    elif category in ("device_motion", "device_1hz"):
        for col in df.columns:
            cl = col.lower()
            if "timestamp" in cl or "datetime" in cl or "time" in cl:
                plan[col] = "parse_date"
            elif "event" in cl or "flag" in cl or "exit" in cl:
                plan[col] = "normalize_boolean"
            else:
                plan[col] = "clean_numeric"
    elif category == "medication":
        for col in df.columns:
            cl = col.lower()
            if "date" in cl or "time" in cl:
                plan[col] = "parse_date"
            elif "dose" in cl or "quantity" in cl:
                plan[col] = "clean_numeric"
    elif category == "nursing":
        for col in df.columns:
            cl = col.lower()
            if "date" in cl or "time" in cl:
                plan[col] = "parse_date"
            elif "note" in cl or "report" in cl or "text" in cl:
                plan[col] = "clean_text"
    elif category == "icd10_ops":
        for col in df.columns:
            cl = col.lower()
            if "date" in cl or "admission" in cl or "discharge" in cl:
                plan[col] = "parse_date"
            elif "length" in cl or "los" in cl:
                plan[col] = "clean_integer"
    return plan


def _apply_category_cleaning(df: pd.DataFrame, category: str) -> pd.DataFrame:
    if category == "labs":
        return _clean_labs(df)
    if category in ("device_motion", "device_1hz"):
        return _clean_device(df)
    if category == "medication":
        return _clean_medication(df)
    if category == "nursing":
        return _clean_nursing(df)
    if category == "icd10_ops":
        return _clean_icd10(df)
    if category == "epa_ac":
        return _clean_epa(df)
    return df


def _clean_epa(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if _NUMERIC_DTYPE_PATTERNS.match(col):
            df[col] = df[col].apply(clean_numeric)
    return df


def _clean_labs(df: pd.DataFrame) -> pd.DataFrame:
    date_hints = {"specimen_datetime", "result_datetime", "collection_date"}
    flag_hints = {"result_flag", "flag", "abnormal_flag"}
    numeric_hints = {"result_value", "lower_ref", "upper_ref"}

    for col in df.columns:
        cl = col.lower()
        if any(h in cl for h in date_hints):
            df[col] = df[col].apply(parse_date)
        elif any(h in cl for h in flag_hints):
            df[col] = df[col].apply(normalize_flag)
        elif any(h in cl for h in numeric_hints):
            df[col] = df[col].apply(clean_numeric)
    return df


def _clean_device(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        cl = col.lower()
        if "timestamp" in cl or "datetime" in cl or "time" in cl:
            df[col] = df[col].apply(parse_date)
        elif "event" in cl or "flag" in cl or "exit" in cl:
            df[col] = df[col].apply(normalize_boolean)
        else:
            df[col] = df[col].apply(clean_numeric)
    return df


def _clean_medication(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        cl = col.lower()
        if "date" in cl or "time" in cl:
            df[col] = df[col].apply(parse_date)
        elif "dose" in cl or "quantity" in cl:
            df[col] = df[col].apply(clean_numeric)
    return df


def _clean_nursing(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        cl = col.lower()
        if "date" in cl or "time" in cl:
            df[col] = df[col].apply(parse_date)
        elif "note" in cl or "report" in cl or "text" in cl:
            df[col] = df[col].apply(clean_text)
    return df


def _clean_icd10(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        cl = col.lower()
        if "date" in cl or "admission" in cl or "discharge" in cl:
            df[col] = df[col].apply(parse_date)
        elif "length" in cl or "los" in cl:
            df[col] = df[col].apply(clean_integer)
    return df


# ---------------------------------------------------------------------------
# Step 7: Normalise ID columns
# ---------------------------------------------------------------------------

def _normalise_id_columns(df: pd.DataFrame, config: "SchemaConfig") -> pd.DataFrame:
    case_col = config.case_id_column
    if case_col and case_col in df.columns:
        df[case_col] = df[case_col].apply(_parse_case_id_safe)

    pat_col = config.patient_id_column
    if pat_col and pat_col in df.columns:
        df[pat_col] = df[pat_col].apply(normalize_id)

    return df


def _parse_case_id_safe(raw) -> int | None:
    if is_null(raw):
        return None
    s = re.sub(r"(?i)^case-?0*", "", str(raw).strip())
    s = re.sub(r"-.*$", "", s)
    try:
        return int(s) if s else None
    except (ValueError, TypeError):
        return None
