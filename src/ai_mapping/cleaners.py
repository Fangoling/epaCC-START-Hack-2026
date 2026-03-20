"""
cleaners.py — Deterministic data cleaning helpers.

LLM-generated transform scripts call these functions by name.
All functions are safe: they never raise, always return a clean value or None.
"""

import re
from datetime import datetime
from typing import Optional

# ─── Null sentinel values ────────────────────────────────────────────────────

_NULL_STRINGS = {
    "null", "none", "na", "n/a", "n.a.", "n.a", "missing",
    "unknown", "undefined", "leer", "fehlt", "",
}


def is_null(val) -> bool:
    """True if val represents a missing value."""
    if val is None:
        return True
    return str(val).strip().lower() in _NULL_STRINGS


# ─── Numeric cleaning ────────────────────────────────────────────────────────

# Characters that should never appear in a numeric field
_NUMERIC_GARBAGE = re.compile(r"[^\d.\-]")


def clean_numeric(val) -> Optional[float]:
    """
    Extract a float from a potentially dirty numeric string.

    Examples:
        "1.12ß"   → 1.12
        "73.9@"   → 73.9
        "NULL"    → None
        "missing" → None
        ""        → None
        3.5       → 3.5   (already a number — pass through)
    """
    if is_null(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    cleaned = _NUMERIC_GARBAGE.sub("", str(val).strip())
    if not cleaned or cleaned in (".", "-"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_integer(val) -> Optional[int]:
    """Same as clean_numeric but returns int."""
    result = clean_numeric(val)
    return int(result) if result is not None else None


# ─── Date/datetime parsing ───────────────────────────────────────────────────

# Month abbreviations in both English and German
_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "mai": 5,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "okt": 10,
    "nov": 11, "dec": 12, "dez": 12,
    "mär": 3, "mrz": 3, "juni": 6, "juli": 7,
}

# Ordered list of format strings to try (most specific first)
_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y.%m.%d",
    "%Y%m%d",
]

# Handles underscore-separated dates: 29_11_2025 or 16_01_2026
_UNDERSCORE_DATE = re.compile(r"^(\d{1,2})_(\d{1,2})_(\d{4})$")

# Handles "08-Jul-2025" or "2026-Feb-02"
_ALPHA_MONTH = re.compile(
    r"^(\d{1,4})[-\s]([a-zA-Zä]+)[-\s](\d{1,4})(?:\s+\d{2}:\d{2}:\d{2})?$"
)

# Handles "Oct 20 2025" or "Mai 32" (Excel artifact)
_MONTH_FIRST = re.compile(r"^([a-zA-Zä]+)\s+(\d{1,4})(?:\s+(\d{4}))?$")


def parse_date(val) -> Optional[str]:
    """
    Parse a date/datetime string in any of the observed formats.
    Returns ISO 8601 string ("YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS") or None.
    """
    if is_null(val):
        return None

    s = str(val).strip()

    # Reject known-invalid sentinel dates
    if s in ("00/00/0000", "00.00.0000", "0000-00-00"):
        return None

    # Try standard formats first
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if not _date_plausible(dt):
                return None
            return _fmt_datetime(dt)
        except ValueError:
            continue

    # Underscore-separated: 29_11_2025
    m = _UNDERSCORE_DATE.match(s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _build_date(y, mo, d)

    # Alpha-month: "08-Jul-2025" or "2026-Feb-02"
    m = _ALPHA_MONTH.match(s)
    if m:
        a, b, c = m.group(1), m.group(2).lower()[:3], m.group(3)
        month = _MONTH_MAP.get(b)
        if month is None:
            return None
        a_int, c_int = int(a), int(c)
        if a_int > 31:           # year-Mon-day
            return _build_date(a_int, month, c_int)
        else:                    # day-Mon-year
            return _build_date(c_int, month, a_int)

    # Month-first: "Oct 20 2025" or Excel artifact "Mai 32" → 5.32 (not a date)
    m = _MONTH_FIRST.match(s)
    if m:
        mon_str = m.group(1).lower()[:3]
        month = _MONTH_MAP.get(mon_str)
        if month is None:
            return None
        day = int(m.group(2))
        year = int(m.group(3)) if m.group(3) else None
        if year is None or day > 31:
            return None          # Likely an Excel decimal artifact, not a date
        return _build_date(year, month, day)

    return None


def _build_date(year: int, month: int, day: int) -> Optional[str]:
    if not (1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100):
        return None
    try:
        dt = datetime(year, month, day)
        return _fmt_datetime(dt)
    except ValueError:
        return None


def _date_plausible(dt: datetime) -> bool:
    return 1900 <= dt.year <= 2100 and 1 <= dt.month <= 12


def _fmt_datetime(dt: datetime) -> str:
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ─── Lab flag normalization ──────────────────────────────────────────────────

_FLAG_MAP = {
    "hh": "H", "ll": "L",          # non-standard doubles
    "h": "H",  "l": "L",
    "n": "N",  "nl": "N",
    "hl": "HL",
    "normal": "N",
    "high": "H", "low": "L",
    "hoch": "H", "niedrig": "L",
}


def normalize_flag(val) -> Optional[str]:
    """Normalize a lab result flag to H / L / HL / N or None."""
    if is_null(val):
        return None
    cleaned = re.sub(r"[\t\s@#ßäöü]", "", str(val)).strip().lower()
    return _FLAG_MAP.get(cleaned)   # None if not recognized


# ─── ID normalization ────────────────────────────────────────────────────────

_DIGITS_ONLY = re.compile(r"\d+")


def normalize_id(val) -> Optional[int]:
    """
    Strip any prefix/suffix and return a plain integer ID.

    Examples:
        "CASE-2300"     → 2300
        "CASE-000001"   → 1
        "PAT-6412"      → 6412
        "C_0001"        → 1
        "PATCASE0013"   → 13
        "ID_0007177"    → 7177
        "ENC-0001"      → 1
        "pt_0001538"    → 1538
        206205          → 206205
        "206205"        → 206205
    """
    if is_null(val):
        return None
    if isinstance(val, (int, float)):
        return int(val)
    digits = _DIGITS_ONLY.findall(str(val).strip())
    if not digits:
        return None
    return int("".join(digits))


def normalize_id_alpha(val) -> Optional[str]:
    """Keep the original alpha ID as-is (for coCaseIdAlpha field)."""
    if is_null(val):
        return None
    return str(val).strip()


# ─── Gender normalization ────────────────────────────────────────────────────

_GENDER_MAP = {
    "m": "M", "male": "M", "mann": "M", "männlich": "M", "maennlich": "M",
    "f": "F", "female": "F", "frau": "F", "weiblich": "F", "w": "F",
    "d": "D", "divers": "D",
}


def normalize_gender(val) -> Optional[str]:
    """Normalize gender to M / F / D or None."""
    if is_null(val):
        return None
    return _GENDER_MAP.get(str(val).strip().lower())


# ─── Boolean / binary fields ─────────────────────────────────────────────────

_TRUE_VALS  = {"1", "true", "yes", "ja", "wahr"}
_FALSE_VALS = {"0", "false", "no", "nein", "falsch"}


def normalize_boolean(val) -> Optional[int]:
    """Normalize a 0/1 binary field, stripping garbage chars first."""
    if is_null(val):
        return None
    cleaned = re.sub(r"[^\w]", "", str(val)).strip().lower()
    if cleaned in _TRUE_VALS:
        return 1
    if cleaned in _FALSE_VALS:
        return 0
    return None


# ─── Text / string cleaning ──────────────────────────────────────────────────

_TEXT_GARBAGE = re.compile(r"[\t\x00]")   # only strip control chars, keep umlauts


def clean_text(val, max_len: int = 512) -> Optional[str]:
    """
    Clean a free-text or identifier string.
    Strips control characters, trims whitespace, truncates to max_len.
    Preserves German umlauts (ä, ö, ü, ß).
    """
    if is_null(val):
        return None
    s = _TEXT_GARBAGE.sub("", str(val)).strip()
    return s[:max_len] if s else None


def clean_id_string(val) -> Optional[str]:
    """
    Clean an ID-like string: strip @, #, ^ but keep alphanumeric and hyphens.

    Examples:
        "MAT-5012#"  → "MAT-5012"
        "ORD@001"    → "ORD001"
    """
    if is_null(val):
        return None
    return re.sub(r"[^A-Za-z0-9\-_]", "", str(val)).strip() or None


# ─── Excel date-artifact reversal ────────────────────────────────────────────

def reverse_excel_date(val) -> Optional[float]:
    """
    Reverse Excel's locale-based date misinterpretation of decimal numbers.

    Examples:
        "03. Mai"  → 3.5
        "Mai 32"   → 5.32
        "05. Jan"  → 5.1
    """
    if is_null(val):
        return None
    s = str(val).strip()

    # Pattern 1: "03. Mai" → day=3, month_abbr="Mai" → 3.5
    m = re.match(r"^(\d{1,2})\.\s*([A-Za-zä]+)$", s)
    if m:
        day = int(m.group(1))
        month = _MONTH_MAP.get(m.group(2).lower()[:3])
        if month is not None:
            return float(f"{day}.{month}")

    # Pattern 2: "Mai 32" → month_abbr="Mai", day=32 → 5.32
    m = re.match(r"^([A-Za-zä]+)\s+(\d{1,2})$", s)
    if m:
        month = _MONTH_MAP.get(m.group(1).lower()[:3])
        day = int(m.group(2))
        if month is not None:
            return float(f"{month}.{day}")

    # Not an Excel artifact — try as a plain number
    return clean_numeric(val)
