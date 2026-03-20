"""
Stage 1 — Preflight.

Deterministic structural analysis of a source file.
No category classification, no LLM calls.
Produces a FileProfile with raw file characteristics.
"""

from __future__ import annotations

import re
import base64
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from src.pipeline.models import FileProfile

if TYPE_CHECKING:
    from src.observability import PipelineRun

try:
    import chardet
    _HAS_CHARDET = True
except ImportError:
    _HAS_CHARDET = False


def preflight(path: str | Path, run: "PipelineRun | None" = None) -> FileProfile:
    """
    Produce a FileProfile for the given file.
    Detects encoding, delimiter, row/column counts, raw headers, and base64 headers.
    """
    from src.observability.models import EventType

    path = Path(path)

    # 1. Encoding detection
    raw_bytes = path.read_bytes()[:8192]
    if _HAS_CHARDET:
        detected = chardet.detect(raw_bytes)
        encoding = detected.get("encoding") or "utf-8"
        if run:
            run.log(EventType.OP_STARTED, stage="preflight",
                    data={"op": "encoding_detection", "input_rows": 0,
                          "decision": "chardet", "detected_encoding": encoding,
                          "chardet_confidence": detected.get("confidence"),
                          "chardet_language": detected.get("language"),
                          "bytes_sampled": len(raw_bytes)})
    else:
        encoding = "utf-8-sig"
        if run:
            run.log(EventType.OP_STARTED, stage="preflight",
                    data={"op": "encoding_detection", "input_rows": 0,
                          "decision": "fallback_utf8sig", "reason": "chardet not installed"})

    # 2. Delimiter detection
    delimiter = _detect_delimiter(path, encoding)
    if run:
        run.log(EventType.OP_STARTED, stage="preflight",
                data={"op": "delimiter_detection", "input_rows": 0,
                      "decision": repr(delimiter),
                      "tested_delimiters": [",", ";", "\\t", "|"]})

    # 3. Read header row only
    try:
        df_head = pd.read_csv(
            path, sep=delimiter, nrows=3,
            encoding=encoding, low_memory=False, on_bad_lines="skip",
        )
    except Exception:
        df_head = pd.read_csv(
            path, sep=",", nrows=3,
            encoding="utf-8-sig", low_memory=False, on_bad_lines="skip",
        )
        delimiter = ","

    headers = list(df_head.columns)

    # 4. Count rows cheaply
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            row_count = sum(1 for _ in f) - 1
    except Exception:
        row_count = len(df_head)

    # 5. Detect base64 headers
    has_base64 = _all_headers_base64(headers)
    if run:
        run.log(EventType.OP_STARTED, stage="preflight",
                data={"op": "base64_header_detection", "input_rows": 0,
                      "decision": "base64" if has_base64 else "plain",
                      "has_base64_headers": has_base64,
                      "header_count": len(headers),
                      "headers_sample": headers[:10]})

    profile = FileProfile(
        file_path=str(path),
        delimiter=delimiter,
        encoding=encoding,
        row_count=row_count,
        column_count=len(headers),
        headers_raw=headers,
        has_base64_headers=has_base64,
    )

    if run:
        run.log(
            EventType.FILE_INSPECTED,
            stage="preflight",
            data=profile.model_dump(),
        )

    return profile


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detect_delimiter(path: Path, encoding: str) -> str:
    """Pick the delimiter that produces the most columns on the header row."""
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
        try:
            padded = h + "=" * (-len(h) % 4)
            decoded = base64.b64decode(padded).decode("latin-1")
            if not decoded.isprintable():
                return False
        except Exception:
            return False
    return True
