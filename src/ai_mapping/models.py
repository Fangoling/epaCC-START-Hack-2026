"""
Pydantic output models for the AI Mapping Agent.
Used by instructor to validate and coerce LLM responses into typed structures.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Column Mapping
# ---------------------------------------------------------------------------

class ColumnMapping(BaseModel):
    source_column: str = Field(description="Exact header name from the source CSV")
    target_column: str | None = Field(
        description="Matching column name in the target DB table, or null if no match"
    )
    transformation_note: str | None = Field(
        default=None,
        description="Optional note about required transformation (e.g. 'parse date', 'strip CASE- prefix')",
    )


class ColumnMappingResult(BaseModel):
    mappings: list[ColumnMapping] = Field(
        description="One entry per source column header"
    )
    unmapped_columns: list[str] = Field(
        default_factory=list,
        description="Source columns that could not be mapped to any target column",
    )

    def to_dict(self) -> dict[str, str | None]:
        """Return a flat {source_col: target_col} dict for backwards compatibility."""
        return {m.source_column: m.target_column for m in self.mappings}


# ---------------------------------------------------------------------------
# Quality Issues
# ---------------------------------------------------------------------------

IssueType = Literal[
    "MISSING_MANDATORY",
    "NULL_VARIANT",
    "INVALID_FORMAT",
    "OUT_OF_RANGE",
    "DUPLICATE_ID",
    "UNKNOWN_CLINIC_ID",
]

Severity = Literal["WARNING", "ERROR"]


class QualityIssue(BaseModel):
    column: str
    row_index: int = Field(description="0-based row index in the source file")
    original_value: str | None
    issue_type: IssueType
    severity: Severity
    suggestion: str


class QualityReport(BaseModel):
    issues: list[QualityIssue] = Field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.issues)

    @property
    def errors(self) -> list[QualityIssue]:
        return [i for i in self.issues if i.severity == "ERROR"]

    @property
    def warnings(self) -> list[QualityIssue]:
        return [i for i in self.issues if i.severity == "WARNING"]

    def to_dicts(self) -> list[dict]:
        return [i.model_dump() for i in self.issues]


# ---------------------------------------------------------------------------
# DB Lookup / Case Existence
# ---------------------------------------------------------------------------

class CaseLookupResult(BaseModel):
    case_id: int
    exists: bool
    action: Literal["INSERT", "UPDATE"]
    existing_row_id: int | None = Field(
        default=None,
        description="coId of the existing row if action=UPDATE",
    )
    existing_data: dict | None = Field(
        default=None,
        description="Current field values for the existing case",
    )
