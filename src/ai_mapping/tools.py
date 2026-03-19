"""
DB lookup tools for the AI Mapping Agent.
Decorated with @tool from smolagents so the agent can call them directly.
Each tool is also usable as a plain Python function when smolagents is not needed.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING

from smolagents import tool

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

# Path to IID-SID-ITEM.csv — used by lookup_iid_for_column
_IID_SID_PATH = Path(__file__).parent.parent.parent / "IID-SID-ITEM.csv"

# Lazily built lookup dict: normalised_name → IID code
_NAME_TO_IID: dict[str, str] | None = None


def _build_name_index() -> dict[str, str]:
    """Build a {lower_name: iid} dict from IID-SID-ITEM.csv (built once)."""
    index: dict[str, str] = {}
    with open(_IID_SID_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            iid = row["ItmIID"].strip()
            for field in ("ItmName255_DE", "ItmName255_EN", "ItmSID"):
                val = row.get(field, "").strip().lower()
                if val:
                    index[val] = iid
    return index


# ---------------------------------------------------------------------------
# Tool 1 — Check whether a case_id exists in tbCaseData
# ---------------------------------------------------------------------------

@tool
def check_case_exists(case_id: int, db_path: str) -> dict:
    """
    Check whether a case already exists in the tbCaseData table.

    Args:
        case_id: The integer case ID to look up (coE2I222 column).
        db_path: Absolute path to the SQLite database file.

    Returns:
        A dict with keys:
          - exists (bool): True if the case was found.
          - action (str): "INSERT" or "UPDATE".
          - row_id (int | None): The coId of the existing row, or None.
          - existing_data (dict | None): Current field values, or None.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT coId, coPatientId, coE2I223, coE2I228, coGender "
                 "FROM tbCaseData WHERE coE2I222 = :cid LIMIT 1"),
            {"cid": case_id},
        ).fetchone()

    if row is None:
        return {"exists": False, "action": "INSERT", "row_id": None, "existing_data": None}

    existing = {
        "coId": row[0],
        "coPatientId": row[1],
        "coE2I223": row[2],
        "coE2I228": row[3],
        "coGender": row[4],
    }
    return {"exists": True, "action": "UPDATE", "row_id": row[0], "existing_data": existing}


# ---------------------------------------------------------------------------
# Tool 2 — Look up the IID code for a column name
# ---------------------------------------------------------------------------

@tool
def lookup_iid_for_column(column_name: str) -> str | None:
    """
    Look up the EPA IID code (e.g. E0_I_001) for a given column name.
    Searches German names, English names, and SID codes in IID-SID-ITEM.csv.

    Args:
        column_name: The source column header or descriptive name to look up.

    Returns:
        The IID code string if found, or None.
    """
    global _NAME_TO_IID
    if _NAME_TO_IID is None:
        _NAME_TO_IID = _build_name_index()

    key = column_name.strip().lower()
    return _NAME_TO_IID.get(key)


# ---------------------------------------------------------------------------
# Tool 3 — Get all table names in the DB
# ---------------------------------------------------------------------------

@tool
def list_db_tables(db_path: str) -> list[str]:
    """
    Return all table names present in the SQLite database.

    Args:
        db_path: Absolute path to the SQLite database file.

    Returns:
        List of table name strings.
    """
    from sqlalchemy import create_engine, inspect

    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    return inspect(engine).get_table_names()


# ---------------------------------------------------------------------------
# Tool 4 — Get column names for a specific table
# ---------------------------------------------------------------------------

@tool
def get_table_columns(table_name: str, db_path: str) -> list[str]:
    """
    Return the column names for a given table in the SQLite database.

    Args:
        table_name: Name of the table (e.g. "tbCaseData").
        db_path:    Absolute path to the SQLite database file.

    Returns:
        List of column name strings.
    """
    from sqlalchemy import create_engine, inspect

    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    cols = inspect(engine).get_columns(table_name)
    return [c["name"] for c in cols]
