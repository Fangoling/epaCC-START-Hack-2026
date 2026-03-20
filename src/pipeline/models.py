"""
Pydantic models for the multistage pipeline.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class FileProfile(BaseModel):
    """Output of preflight(). Describes raw file characteristics only — no semantic classification."""

    file_path: str
    delimiter: str = ","
    encoding: str = "utf-8"
    row_count: int = 0
    column_count: int = 0
    headers_raw: list[str] = Field(default_factory=list)
    has_base64_headers: bool = False


class EpaStructure(BaseModel):
    """EPA-specific structural details discovered by AI schema discovery."""

    header_row_index: int = 0
    # Free-form string: AI describes the identifier type/case (not an enum).
    # Examples: "SID as column headers", "IID as column headers",
    #           "SID in second row, first row is metadata",
    #           "long format: each row is one SID observation",
    #           "SAP EPA encoded column names"
    identifier_type: str = ""
    encoding_type: Optional[str] = None   # "sap_epa" | "base64" | None
    # For long-format (Case 5): which column holds the SID/IID identifier
    sid_column: Optional[str] = None
    # For long-format: which column holds the numeric value
    value_column: Optional[str] = None


class SchemaConfig(BaseModel):
    """
    Output of AI schema discovery (discover_schema).
    Describes the semantic structure of any clinical data file.
    """

    # One of: epa_ac | labs | icd10_ops | medication | nursing | device_motion | device_1hz | unknown
    data_category: str = "unknown"
    # "wide" (one row per patient/case) or "long" (multiple rows per patient/case)
    format_type: str = "wide"
    # 0-based index of the header row (handles Case 3 where header is on row 2)
    header_row_index: int = 0
    # Column names for key identifiers (None if not found)
    patient_id_column: Optional[str] = None
    case_id_column: Optional[str] = None
    # Columns flagged by AI as noise/metadata to drop
    columns_to_drop: list[str] = Field(default_factory=list)
    # For discriminator-split files (e.g. medication ADMIN/ORDER)
    record_type_column: Optional[str] = None
    record_type_values: list[str] = Field(default_factory=list)
    # EPA-specific sub-config (only set when data_category == "epa_ac")
    epa: Optional[EpaStructure] = None
    # Free-form anomalies noted by the AI
    anomalies: list[str] = Field(default_factory=list)


class RoutingResult(BaseModel):
    """Result of writing one DataFrame to a target table."""

    table: str
    frame_name: str
    row_count: int
    inserts: int = 0
    updates: int = 0
    errors: int = 0
