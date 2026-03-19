"""
Pydantic models for the multistage pipeline.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FileProfile(BaseModel):
    """Output of Stage 1 (File Inspector). Describes everything about a source file."""

    file_path: str
    delimiter: str = ","
    encoding: str = "utf-8"
    row_count: int = 0
    column_count: int = 0

    # Raw vs decoded headers
    headers_raw: list[str] = Field(default_factory=list)
    headers_decoded: list[str] | None = None      # populated after base64 decode

    # Structural anomalies
    has_base64_headers: bool = False
    has_discriminator_column: bool = False
    discriminator_column: str | None = None        # e.g. "record_type"
    discriminator_values: list[str] = Field(default_factory=list)   # e.g. ["ADMIN", "ORDER"]

    # Identity columns
    id_columns: list[str] = Field(default_factory=list)
    id_format_pattern: str | None = None           # e.g. "PAT-NNNN" or "CASE-NNNN-NN"

    # Semantic classification
    data_category: str = "unknown"
    # known categories: epa_ac | labs | medication | icd10_ops | device_motion
    #                   device_1hz | nursing | unknown

    anomalies: list[str] = Field(default_factory=list)


class PreprocessingStep(BaseModel):
    """One op in the PreprocessingPlan."""

    op: str                                        # key in PreprocessorRegistry
    params: dict[str, Any] = Field(default_factory=dict)
    rationale: str = ""                            # LLM reasoning for this step


class PreprocessingPlan(BaseModel):
    """Output of Stage 2 (Planner). Ordered list of ops to apply."""

    steps: list[PreprocessingStep] = Field(default_factory=list)
    llm_rationale: str = ""


class RoutingDecision(BaseModel):
    """Maps one named DataFrame to a target table."""

    frame_name: str
    target_table: str
    confidence: float = 1.0


class RoutingResult(BaseModel):
    """Result of writing one DataFrame to a target table."""

    table: str
    frame_name: str
    row_count: int
    inserts: int = 0
    updates: int = 0
    errors: int = 0
