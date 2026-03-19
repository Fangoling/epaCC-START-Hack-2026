"""
Stage 1 — File Inspector.

Combines fast heuristics with an optional LLM confirmation pass to produce
a FileProfile for any source file. The LLM call is skipped when
OLLAMA_ENABLED=false or when the heuristics are already high-confidence.
"""

from __future__ import annotations

import re
import base64
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from src.pipeline.models import FileProfile
from src.pipeline.prompts import INSPECTOR_TEMPLATE
from src.ai_mapping.ollama_client import call_structured

if TYPE_CHECKING:
    from src.observability import PipelineRun

# Encoding detection — chardet is optional
try:
    import chardet
    _HAS_CHARDET = True
except ImportError:
    _HAS_CHARDET = False

# Category keywords — used for fast heuristic classification
_CATEGORY_SIGNALS: dict[str, list[str]] = {
    "labs": ["sodium", "creatinine", "hemoglobin", "specimen_datetime", "mmol", "mg_dl"],
    "medication": ["medication_code_atc", "route", "dose_unit", "order_id", "atc"],
    "icd10_ops": ["icd10", "ops_code", "admission_date", "discharge_date", "primary_icd"],
    "device_1hz": ["movement_index", "micro_movements", "bed_exit", "fall_event", "impact_magnitude"],
    "device_motion": ["movement_index", "micro_movements", "bed_exit", "fall_event", "impact_magnitude"],
    "nursing": ["nursing_note", "shift", "ward", "report_date"],
    "epa_ac": ["EPA", "EPAST", "FallID", "SID", "SID_value", "Einschätzung"],
}

# Known discriminator columns and their typical values
_DISCRIMINATOR_HINTS: dict[str, list[str]] = {
    "record_type": ["ADMIN", "ORDER"],
    "type": [],
    "category": [],
}

# ID column name patterns
_ID_COLUMN_REGEX = re.compile(r"(case.?id|patient.?id|encounter.?id|fall.?id|fallid|patid|caseid)", re.IGNORECASE)
_ID_VALUE_REGEX = re.compile(r"^(CASE|PAT|ENC|pt|ID)[_\-]\d", re.IGNORECASE)


def inspect_file(path: str | Path, run: "PipelineRun | None" = None) -> FileProfile:
    """
    Produce a FileProfile for the given file using heuristics + optional LLM.
    Logs EventType.FILE_INSPECTED to run if provided.
    """
    from src.observability.models import EventType

    path = Path(path)

    # ── 1. Encoding detection ──────────────────────────────────────────────
    raw_bytes = path.read_bytes()[:8192]
    if _HAS_CHARDET:
        detected = chardet.detect(raw_bytes)
        encoding = detected.get("encoding") or "utf-8"
    else:
        encoding = "utf-8-sig"

    # ── 2. Delimiter detection ─────────────────────────────────────────────
    delimiter = _detect_delimiter(path, encoding)

    # ── 3. Read sample ─────────────────────────────────────────────────────
    try:
        df_sample = pd.read_csv(
            path, sep=delimiter, nrows=10,
            encoding=encoding, low_memory=False, on_bad_lines="skip"
        )
    except Exception:
        df_sample = pd.read_csv(
            path, sep=",", nrows=10,
            encoding="utf-8-sig", low_memory=False, on_bad_lines="skip"
        )
        delimiter = ","

    headers = list(df_sample.columns)

    # ── 4. Count rows cheaply ──────────────────────────────────────────────
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            row_count = sum(1 for _ in f) - 1  # subtract header
    except Exception:
        row_count = len(df_sample)

    # ── 5. Heuristic: base64 headers ──────────────────────────────────────
    has_base64 = _all_headers_base64(headers)

    # ── 6. Heuristic: discriminator column ────────────────────────────────
    disc_col, disc_values = _find_discriminator(df_sample)

    # ── 7. Heuristic: ID columns ──────────────────────────────────────────
    id_cols, id_pattern = _find_id_columns(df_sample)

    # ── 8. Heuristic: data category ───────────────────────────────────────
    category = _classify_category(headers, row_count)

    # Promote device_motion to device_1hz for very high-frequency data
    if category == "device_motion" and row_count > 50_000:
        category = "device_1hz"

    profile = FileProfile(
        file_path=str(path),
        delimiter=delimiter,
        encoding=encoding,
        row_count=row_count,
        column_count=len(headers),
        headers_raw=headers,
        has_base64_headers=has_base64,
        has_discriminator_column=disc_col is not None,
        discriminator_column=disc_col,
        discriminator_values=disc_values,
        id_columns=id_cols,
        id_format_pattern=id_pattern,
        data_category=category,
        anomalies=[],
    )

    # ── 9. Optional LLM confirmation for ambiguous cases ──────────────────
    if category == "unknown" or has_base64:
        profile = _llm_confirm(profile, df_sample)

    if run:
        run.log(
            EventType.FILE_INSPECTED,
            stage="inspection",
            data={"profile": profile.model_dump(), "anomalies": profile.anomalies},
        )

    return profile


# ---------------------------------------------------------------------------
# Heuristic helpers
# ---------------------------------------------------------------------------

def _detect_delimiter(path: Path, encoding: str) -> str:
    """Try ; , \t — pick whichever gives the most columns on the header row."""
    try:
        first_line = path.open(encoding=encoding, errors="replace").readline()
    except Exception:
        return ","

    best = ","
    best_count = first_line.count(",")
    for sep in (";", "\t", "|"):
        count = first_line.count(sep)
        if count > best_count:
            best_count = count
            best = sep
    return best


def _all_headers_base64(headers: list[str]) -> bool:
    """Return True if ALL headers look like base64-encoded strings."""
    if len(headers) < 3:
        return False
    b64_re = re.compile(r"^[A-Za-z0-9+/=]{4,}$")
    for h in headers:
        if not b64_re.match(str(h)):
            return False
        # Must decode to printable content
        try:
            padded = h + "=" * (-len(h) % 4)
            decoded = base64.b64decode(padded).decode("latin-1")
            if not decoded.isprintable():
                return False
        except Exception:
            return False
    return True


def _find_discriminator(df: pd.DataFrame) -> tuple[str | None, list[str]]:
    """Return (column_name, unique_values) if a discriminator column is found."""
    for col in df.columns:
        if col.lower() in _DISCRIMINATOR_HINTS:
            vals = df[col].dropna().unique().tolist()
            if 2 <= len(vals) <= 10:
                return col, [str(v) for v in vals]
        # Generic: low-cardinality, short string values, all-caps
        try:
            unique_vals = df[col].dropna().unique()
            if (
                2 <= len(unique_vals) <= 5
                and all(isinstance(v, str) and v.isupper() and len(v) <= 10
                        for v in unique_vals)
                and df[col].nunique() / max(len(df), 1) < 0.2
            ):
                return col, [str(v) for v in unique_vals]
        except Exception:
            continue
    return None, []


def _find_id_columns(df: pd.DataFrame) -> tuple[list[str], str | None]:
    """Return (id_columns, format_pattern) by matching column names and values."""
    id_cols: list[str] = []
    pattern: str | None = None

    for col in df.columns:
        if _ID_COLUMN_REGEX.search(str(col)):
            id_cols.append(col)
            # Detect format from first non-null value
            sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if sample_val and _ID_VALUE_REGEX.match(str(sample_val)):
                prefix = re.match(r"^([A-Za-z]+[_\-])", str(sample_val))
                if prefix:
                    pattern = f"{prefix.group()}NNNN"

    return id_cols, pattern


def _classify_category(headers: list[str], row_count: int) -> str:
    """Score each category by how many of its keywords appear in the headers."""
    lowered = {h.lower() for h in headers}
    scores: dict[str, int] = {}
    for category, signals in _CATEGORY_SIGNALS.items():
        score = sum(1 for s in signals if any(s.lower() in h for h in lowered))
        if score > 0:
            scores[category] = score

    if not scores:
        return "unknown"

    # device_1hz and device_motion share signals — distinguish by row count
    best = max(scores, key=lambda k: scores[k])
    if best in ("device_1hz", "device_motion"):
        return "device_1hz" if row_count > 50_000 else "device_motion"
    return best


def _llm_confirm(profile: FileProfile, df_sample: pd.DataFrame) -> FileProfile:
    """
    Run one LLM call to confirm or correct the heuristic FileProfile.
    Returns the original profile if LLM is unavailable.
    """
    sample_str = df_sample.head(5).to_string(index=False, max_cols=20)
    raw_headers = ", ".join(profile.headers_raw[:20])

    prompt = INSPECTOR_TEMPLATE.format(
        file_path=profile.file_path,
        row_count=profile.row_count,
        column_count=profile.column_count,
        detected_delimiter=profile.delimiter,
        encoding=profile.encoding,
        raw_headers=raw_headers,
        sample_rows=sample_str,
    )

    confirmed = call_structured(prompt, FileProfile)
    if confirmed is None:
        return profile

    # Merge: prefer LLM values for ambiguous fields, keep heuristic for stable ones
    profile = profile.model_copy(update={
        "data_category": confirmed.data_category if confirmed.data_category != "unknown" else profile.data_category,
        "has_base64_headers": confirmed.has_base64_headers,
        "headers_decoded": confirmed.headers_decoded,
        "discriminator_column": confirmed.discriminator_column or profile.discriminator_column,
        "discriminator_values": confirmed.discriminator_values or profile.discriminator_values,
        "anomalies": confirmed.anomalies,
    })
    return profile
