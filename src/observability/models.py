"""
Structured event models for the pipeline observability layer.
Every stage emits typed PipelineEvents — serialised to JSONL for audit + debugging.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    # ── Inspection ──────────────────────────────────────────────────────────
    FILE_INSPECTED = "file_inspected"       # FileProfile produced

    # ── Schema discovery ────────────────────────────────────────────────────
    SCHEMA_DISCOVERED = "schema_discovered" # SchemaConfig produced by AI

    # ── LLM calls ───────────────────────────────────────────────────────────
    LLM_CALL_STARTED = "llm_call_started"   # prompt + model sent to LLM
    LLM_CALL_COMPLETED = "llm_call_completed" # response received + timing
    LLM_CALL_RETRY = "llm_call_retry"       # connection retry with backoff

    # ── Planning ────────────────────────────────────────────────────────────
    PLAN_GENERATED = "plan_generated"       # PreprocessingPlan + LLM rationale

    # ── Pre-processing ops ──────────────────────────────────────────────────
    OP_STARTED = "op_started"               # op name + params logged
    OP_COMPLETED = "op_completed"           # row-count delta + output df names
    OP_FAILED = "op_failed"                 # exception info

    # ── Column mapping ──────────────────────────────────────────────────────
    COLUMN_RESOLVED = "column_resolved"     # one source col → target col + method
    COLUMN_UNMAPPED = "column_unmapped"     # col that couldn't be mapped
    MAPPING_COMPLETED = "mapping_completed" # summary after all columns processed

    # ── Quality ─────────────────────────────────────────────────────────────
    QUALITY_ISSUE = "quality_issue"         # one flagged cell / row
    QUALITY_COMPLETED = "quality_completed" # summary: N errors, M warnings

    # ── Transformation ──────────────────────────────────────────────────────
    TRANSFORM_STARTED = "transform_started"     # raw df shape + config summary
    TRANSFORM_STEP = "transform_step"           # one named step with row/col delta
    TRANSFORM_COMPLETED = "transform_completed" # final shape + unmapped count

    # ── Routing ─────────────────────────────────────────────────────────────
    ROUTE_DECIDED = "route_decided"         # df_name → target_table
    ROUTING_COMPLETED = "routing_completed" # insert_count + update_count per table
    WRITE_ERROR = "write_error"             # single row write failure

    # ── Pipeline lifecycle ──────────────────────────────────────────────────
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"


class PipelineEvent(BaseModel):
    run_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    event_type: EventType
    source_file: str
    stage: str                          # e.g. "inspection", "planning", "op:split_by_discriminator"
    duration_ms: float | None = None    # wall-clock ms for the emitting operation
    data: dict[str, Any] = Field(default_factory=dict)
    # data is intentionally untyped — each event_type documents its keys below.
    #
    # FILE_INSPECTED.data:
    #   profile: dict          — serialised FileProfile
    #   anomalies: list[str]
    #
    # PLAN_GENERATED.data:
    #   plan: dict             — serialised PreprocessingPlan
    #   llm_rationale: str     — raw LLM reasoning text
    #
    # OP_STARTED / OP_COMPLETED / OP_FAILED.data:
    #   op: str                — op name
    #   params: dict
    #   input_rows: int        — row count before
    #   output_rows: int       — row count after (COMPLETED only)
    #   output_frames: list[str]   — names of resulting DataFrames (COMPLETED only)
    #   error: str             — exception message (FAILED only)
    #
    # COLUMN_RESOLVED.data:
    #   source_column: str
    #   target_column: str
    #   method: str            — "iid_lookup" | "sid_lookup" | "llm" | "direct"
    #   confidence: float | None
    #   transformation_note: str | None
    #
    # COLUMN_UNMAPPED.data:
    #   source_column: str
    #   reason: str
    #
    # MAPPING_COMPLETED.data:
    #   mapped: int; unmapped: int; llm_calls: int
    #
    # QUALITY_ISSUE.data:
    #   row_index: int; column: str; value: str | None
    #   issue_type: str; severity: str; suggestion: str
    #
    # QUALITY_COMPLETED.data:
    #   total: int; errors: int; warnings: int
    #
    # ROUTE_DECIDED.data:
    #   frame_name: str; target_table: str; row_count: int
    #
    # ROUTING_COMPLETED.data:
    #   results: list[{table, inserts, updates, errors}]
    #
    # PIPELINE_COMPLETED.data:
    #   total_input_rows: int; total_output_rows: int
    #   tables_written: list[str]; duration_ms: float
    #
    # PIPELINE_FAILED.data:
    #   stage: str; error: str; traceback: str
