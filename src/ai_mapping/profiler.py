"""
profiler.py — Describe every column in a DataFrame.

For each column we produce a ColumnProfile that captures:
  - Representative sample values
  - Dominant data type (float / integer / datetime / categorical / text / binary / id)
  - Numeric range (for numeric columns)
  - Null rate
  - Error flags (embedded special chars, literal NULL strings, tab chars, etc.)
  - Whether the column name is already an IID code (so the LLM can skip inference)

The resulting FileProfile is embedded directly in the LLM prompt.
"""

import re
import math
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .cleaners import clean_numeric, parse_date

# ─── Result dataclasses ──────────────────────────────────────────────────────

@dataclass
class ColumnProfile:
    col_index: int
    col_name: str
    human_name: str

    total_rows: int
    null_rate: float
    sample_values: list

    dtype_dist: dict
    looks_like: str   # float | integer | datetime | categorical | text | binary | id | unknown

    numeric_range: Optional[tuple]
    cardinality: int
    is_high_cardinality: bool

    error_flags: list[str]
    error_rate: float

    is_iid_mapped: bool

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        d["numeric_range"] = list(self.numeric_range) if self.numeric_range else None
        return d


@dataclass
class FileProfile:
    source_file: str
    file_type: str
    encoding: str
    delimiter: str
    header_count: int
    total_rows: int
    total_cols: int
    columns: list[ColumnProfile]

    def to_dict(self) -> dict:
        return {
            "source_file":   self.source_file,
            "file_type":     self.file_type,
            "encoding":      self.encoding,
            "delimiter":     self.delimiter,
            "header_count":  self.header_count,
            "total_rows":    self.total_rows,
            "total_cols":    self.total_cols,
            "columns":       [c.to_dict() for c in self.columns],
        }


# ─── Public entry point ──────────────────────────────────────────────────────

_SAMPLE_LIMIT = 1000
_N_SAMPLES = 5


def profile(
    df: "pd.DataFrame",
    source_file: str = "",
    file_type: str = "csv",
    encoding: str = "utf-8",
    delimiter: str = ",",
    header_count: int = 0,
) -> FileProfile:
    """Build a FileProfile from a DataFrame."""
    if len(df) > _SAMPLE_LIMIT:
        df_sample = df.sample(n=_SAMPLE_LIMIT, random_state=42)
    else:
        df_sample = df

    columns = []
    for i, col in enumerate(df_sample.columns):
        columns.append(_profile_column(
            series=df_sample[col],
            col_index=i,
            col_name=str(col),
            human_name=str(col),
            total_rows=len(df),
        ))

    return FileProfile(
        source_file=source_file,
        file_type=file_type,
        encoding=encoding,
        delimiter=delimiter,
        header_count=header_count,
        total_rows=len(df),
        total_cols=len(df.columns),
        columns=columns,
    )


# ─── Per-column profiling ────────────────────────────────────────────────────

_DATE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"|^\d{2}\.\d{2}\.\d{4}"
    r"|^\d{2}/\d{2}/\d{4}"
    r"|^\d{2}-[A-Za-z]{3}-\d{4}"
    r"|^\d{8}$"
)
_ID_RE = re.compile(r"^[A-Za-z]{2,}[-_]?\d+$", re.IGNORECASE)
_GARBAGE_RE = re.compile(r"[ßäöüÄÖÜ@#\^]")
_TAB_RE = re.compile(r"\t")

_NULL_STRS = {
    "null", "none", "na", "n/a", "n.a.", "n.a", "missing",
    "unknown", "undefined", "", "leer", "fehlt",
}


def _profile_column(
    series: "pd.Series",
    col_index: int,
    col_name: str,
    human_name: str,
    total_rows: int,
) -> ColumnProfile:
    raw_values = list(series.astype(str))

    null_mask = [v.strip().lower() in _NULL_STRS for v in raw_values]
    null_count = sum(null_mask)
    null_rate = null_count / len(raw_values) if raw_values else 0.0
    non_null_vals = [v for v, is_n in zip(raw_values, null_mask) if not is_n]

    error_flags, error_count = _detect_errors(raw_values, null_mask)
    error_rate = error_count / len(raw_values) if raw_values else 0.0

    dtype_dist, looks_like = _sniff_types(non_null_vals)

    numeric_range = None
    if looks_like in ("float", "integer"):
        nums = [n for v in non_null_vals if (n := clean_numeric(v)) is not None and not math.isnan(n)]
        if nums:
            numeric_range = (min(nums), max(nums))

    unique_vals = set(v for v in non_null_vals if v)
    cardinality = min(len(unique_vals), 500)

    clean_candidates = [v for v in non_null_vals if not _GARBAGE_RE.search(v)]
    sample_pool = clean_candidates if len(clean_candidates) >= 3 else non_null_vals
    seen: set = set()
    samples = []
    for v in sample_pool:
        stripped = v.strip()
        if stripped not in seen:
            seen.add(stripped)
            samples.append(stripped)
        if len(samples) == _N_SAMPLES:
            break

    return ColumnProfile(
        col_index=col_index,
        col_name=col_name,
        human_name=human_name,
        total_rows=total_rows,
        null_rate=round(null_rate, 3),
        sample_values=samples,
        dtype_dist=dtype_dist,
        looks_like=looks_like,
        numeric_range=numeric_range,
        cardinality=cardinality,
        is_high_cardinality=cardinality > 20,
        error_flags=error_flags,
        error_rate=round(error_rate, 3),
        is_iid_mapped=_is_iid_code(col_name),
    )


# ─── Type sniffing ────────────────────────────────────────────────────────────

def _sniff_types(values: list[str]) -> tuple[dict, str]:
    if not values:
        return {}, "unknown"

    counts: dict[str, int] = {
        "float": 0, "integer": 0, "datetime": 0,
        "binary": 0, "id": 0, "categorical": 0, "text": 0,
    }
    for v in values:
        t = _classify_value(v)
        counts[t] = counts.get(t, 0) + 1

    total = len(values)
    dist = {k: round(v / total, 3) for k, v in counts.items() if v > 0}
    dominant = max(counts, key=counts.get)

    if dominant == "float":
        clean_nums = {clean_numeric(v) for v in values}
        clean_nums.discard(None)
        if clean_nums and clean_nums.issubset({0.0, 1.0}):
            dominant = "binary"

    return dist, dominant


def _classify_value(v: str) -> str:
    v = v.strip()
    if v in ("0", "1"):
        return "binary"
    if _DATE_RE.match(v) or parse_date(v) is not None:
        return "datetime"
    if _ID_RE.match(v):
        return "id"
    if re.match(r"^-?\d+$", v):
        return "integer"
    cleaned = re.sub(r"[^\d.\-]", "", v)
    if cleaned and cleaned not in (".", "-"):
        try:
            float(cleaned)
            return "float"
        except ValueError:
            pass
    if len(v) > 50:
        return "text"
    return "categorical"


# ─── Error detection ─────────────────────────────────────────────────────────

def _detect_errors(raw_values: list[str], null_mask: list[bool]) -> tuple[list[str], int]:
    flag_set: set[str] = set()
    error_rows = 0
    date_formats_seen: set[str] = set()

    for v, is_n in zip(raw_values, null_mask):
        row_has_error = False
        if is_n:
            if v.strip() and v.strip().lower() in ("null", "none", "na", "n/a", "missing", "unknown"):
                flag_set.add("null_string")
                row_has_error = True
            continue

        if _TAB_RE.search(v):
            flag_set.add("tab_char")
            row_has_error = True
        if _GARBAGE_RE.search(v) and re.search(r"\d", v):
            flag_set.add("embedded_special_char")
            row_has_error = True

        fmt = _date_format_signature(v)
        if fmt:
            date_formats_seen.add(fmt)
        if fmt and parse_date(v) is None:
            flag_set.add("impossible_date")
            row_has_error = True

        if row_has_error:
            error_rows += 1

    if len(date_formats_seen) > 2:
        flag_set.add("mixed_date_formats")

    return sorted(flag_set), error_rows


_DATE_FORMAT_PATTERNS = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}"),        "ISO"),
    (re.compile(r"^\d{2}\.\d{2}\.\d{4}"),      "DE"),
    (re.compile(r"^\d{2}/\d{2}/\d{4}"),        "US"),
    (re.compile(r"^\d{4}/\d{2}/\d{2}"),        "ISO_SLASH"),
    (re.compile(r"^\d{8}$"),                    "COMPACT"),
    (re.compile(r"^\d{2}-[A-Za-z]{3}-\d{4}"), "DD-Mon-YYYY"),
    (re.compile(r"^[A-Za-z]{3}\s+\d"),         "Mon-DD"),
    (re.compile(r"^\d{2}_\d{2}_\d{4}"),        "UNDERSCORE"),
    (re.compile(r"^\d{4}\.\d{2}\.\d{2}"),      "ISO_DOT"),
]


def _date_format_signature(v: str) -> Optional[str]:
    for pattern, name in _DATE_FORMAT_PATTERNS:
        if pattern.match(v.strip()):
            return name
    return None


# ─── IID code detection ───────────────────────────────────────────────────────

_IID_NORM_RE = re.compile(r"^E[023]\d*I\d+$", re.IGNORECASE)


def _is_iid_code(col_name: str) -> bool:
    return bool(_IID_NORM_RE.match(col_name.strip()))
