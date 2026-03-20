"""
Context provisioning for the AI Mapping Agent.
Loads and formats:
  1. IID/SID mapping  (IID-SID-ITEM.csv)
  2. Unification rules (unification_rules.md  — generated from Hack2026_README.md)
  3. Target schema     (DB/CreateImportTables.sql → JSON summary)
  4. Source sample     (first N rows of a CSV/XLSX)
"""

from __future__ import annotations

import csv
import functools
import io
import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths (relative to project root — adjust via env if needed)
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent
PROJECT_ROOT = _HERE.parent.parent

IID_SID_PATH = PROJECT_ROOT / "IID-SID-ITEM.csv"
RULES_PATH = PROJECT_ROOT / "unification_rules.md"
SQL_PATH = PROJECT_ROOT / "DB" / "CreateImportTables.sql"

SAMPLE_ROWS = 5  # how many source rows to include in the prompt


# ---------------------------------------------------------------------------
# 1. IID / SID Mapping
# ---------------------------------------------------------------------------

def build_sid_to_ddl_column() -> dict[str, str]:
    """
    Build a lookup dict mapping SID codes (e.g. '08_02') to their DDL column
    names (e.g. 'coE2I042') by reading IID-SID-ITEM.csv.

    Conversion: IID 'E2_I_042' → strip underscores → 'E2I042' → add 'co' → 'coE2I042'.
    Only the first IID encountered for each SID is kept.
    """
    mapping: dict[str, str] = {}
    with open(IID_SID_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            sid = row["ItmSID"].strip()
            iid = row["ItmIID"].strip()
            if sid and iid and sid not in mapping:
                ddl_col = "co" + iid.replace("_", "")
                mapping[sid] = ddl_col
    return mapping


def load_iid_sid_mapping(max_rows: int = 200) -> str:
    """
    Return a compact text table of IID → SID → German name → English name.
    Truncated to max_rows to keep the prompt short.
    """
    rows: list[str] = []
    with open(IID_SID_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for i, row in enumerate(reader):
            if i >= max_rows:
                rows.append(f"... (truncated after {max_rows} rows)")
                break
            rows.append(
                f"{row['ItmIID']:<14} | {row['ItmSID']:<10} | "
                f"{row['ItmName255_DE']:<40} | {row['ItmName255_EN']}"
            )
    header = f"{'IID':<14} | {'SID':<10} | {'German name':<40} | English name"
    sep = "-" * len(header)
    return "\n".join([header, sep] + rows)


# ---------------------------------------------------------------------------
# 2. Unification Rules
# ---------------------------------------------------------------------------

def load_unification_rules() -> str:
    """
    Return the content of unification_rules.md.
    If the file doesn't exist yet, return a compact inline fallback derived
    from the known rules in Hack2026_README.md.
    """
    if RULES_PATH.exists():
        return RULES_PATH.read_text(encoding="utf-8")

    # Inline fallback (key rules extracted from Hack2026_README.md)
    return """\
## Null Sentinels
Values that must be treated as missing/null:
  NULL, null, Missing, missing, unknow, NaN, nan, N/A, n/a, <empty string>

## Case ID (case_id / coE2I222)
Formats seen: "CASE-0135-01", "CASE-0135", "0135", "135"
Rule: strip "CASE-" prefix, strip any suffix after the second "-", convert to integer.
Example: "CASE-0135-01" → 135

## Patient ID (coPatientId)
Formats seen: "000135", "135"
Rule: strip leading zeros, convert to integer.

## Date Formats
Multiple formats exist in source data:
  DD.MM.YYYY, YYYYMMDD, YYYY.MM.DD, Mon DD YYYY, YYYY-MM-DD
Rule: parse with dateutil or pandas, store as ISO 8601 string "YYYY-MM-DD".

## German Decimals
Decimal comma used: "23,7" → 23.7
Rule: replace "," with "." before float conversion.

## Gender Normalisation
Source values: "M", "male", "männlich", "F", "female", "weiblich"
Rule: map to single character "M" or "F". Unknown → null.

## EPA Assessment Validity
A valid EPA record requires all three fields:
  - Einschätzungstyp (coE0I001) — not null
  - Einschätzungsdatum (coE2I222-related date) — not null
  - FallNr / case_id — parseable integer

## Column Exclusions
Drop columns whose names start with ZWrt_ or ZDat_ (change-tracking timestamps).
"""


# ---------------------------------------------------------------------------
# 3. Target Schema
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=None)
def _read_sql_ddl() -> str:
    """Read and cache the SQL DDL file contents.

    Note: cached for the process lifetime. Call ``_read_sql_ddl.cache_clear()``
    if the DDL file changes at runtime.
    """
    return SQL_PATH.read_text(encoding="utf-8", errors="replace")


def load_target_schema(table_name: str) -> str:
    """
    Extract the CREATE TABLE block for `table_name` from the SQL DDL file
    and return it as a plain text string.

    Uses depth-tracking parenthesis matching so that type expressions like
    nvarchar(256) do not prematurely end the match.
    """
    sql = _read_sql_ddl()
    # Find the opening of the CREATE TABLE block
    pattern = rf"create\s+table\s+{re.escape(table_name)}\s*\("
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
        return f"(table {table_name!r} not found in DDL)"
    start = match.start()
    # Walk from the opening '(' tracking depth to find the matching close paren
    depth = 0
    for i in range(match.end() - 1, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start : i + 1].strip()
    return f"(table {table_name!r} not found in DDL)"


def list_target_tables() -> list[str]:
    """Return all table names defined in the DDL file."""
    sql = _read_sql_ddl()
    return re.findall(r"create\s+table\s+(\w+)", sql, re.IGNORECASE)


# ---------------------------------------------------------------------------
# 4. Source Sample
# ---------------------------------------------------------------------------

def load_source_sample_from_df(df: "pd.DataFrame", n: int = SAMPLE_ROWS) -> tuple[str, str]:
    """
    Build (headers_str, sample_str) from an already-loaded DataFrame.
    Used by the pipeline router when the DataFrame has already been preprocessed.
    """
    df = df.head(n)
    headers_str = ", ".join(str(c) for c in df.columns)
    sample_str = df.to_string(index=False, max_cols=30)
    return headers_str, sample_str


def load_source_sample(source_path: str | Path, n: int = SAMPLE_ROWS) -> tuple[str, str]:
    """
    Read the first `n` data rows from a CSV or XLSX file.
    Returns (headers_str, sample_str) — both as formatted text for the prompt.
    """
    path = Path(source_path)
    suffix = path.suffix.lower()

    if suffix == ".xlsx":
        try:
            df = pd.read_excel(path, nrows=n + 2)  # +2 for potential header rows
        except Exception as exc:
            logger.warning("XLSX parse failed: %s", exc)
            df = pd.DataFrame()
    else:
        # Try semicolon first, fall back to comma
        try:
            df = pd.read_csv(path, sep=";", nrows=n, encoding="utf-8-sig")
            if len(df.columns) == 1:
                df = pd.read_csv(path, sep=",", nrows=n, encoding="utf-8-sig")
        except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeDecodeError) as exc:
            logger.warning("CSV parse failed with ';' delimiter, retrying with ',': %s", exc)
            try:
                df = pd.read_csv(path, sep=",", nrows=n, encoding="utf-8-sig",
                                 errors="replace")
            except Exception as exc2:
                logger.warning("CSV parse failed entirely: %s", exc2)
                df = pd.DataFrame()

    df = df.head(n)
    headers_str = ", ".join(str(c) for c in df.columns)
    sample_str = df.to_string(index=False, max_cols=30)
    return headers_str, sample_str
