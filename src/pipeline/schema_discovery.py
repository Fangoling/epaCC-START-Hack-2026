"""
Stage 2 — AI Schema Discovery.

Sends a sample of the source file to the LLM and returns a SchemaConfig
describing the file's structure, format type, and any anomalies.

Sampling strategy:
  - Large files (>5 000 rows or >300 columns): send column names only.
  - Small files: send 3-4 raw rows.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from src.pipeline.models import FileProfile, SchemaConfig
from src.ai_mapping.prompt_templates import SCHEMA_DISCOVERY_TEMPLATE
from src.ai_mapping.ollama_client import call_structured

if TYPE_CHECKING:
    from src.observability import PipelineRun
    from src.pipeline.mapping_engine import MappingEngine

_LARGE_FILE_ROWS = 5_000
_LARGE_FILE_COLS = 300
_SAMPLE_ROWS = 3


def discover_schema(
    source_path: str | Path,
    profile: FileProfile,
    mapping_engine: "MappingEngine",
    run: "PipelineRun | None" = None,
) -> SchemaConfig:
    """
    Run AI schema discovery on source_path and return a SchemaConfig.
    Raises LLMUnavailableError if the LLM cannot be reached after retries.
    """
    from src.observability.models import EventType

    source_path = Path(source_path)

    # Choose sampling strategy
    if profile.row_count > _LARGE_FILE_ROWS or profile.column_count > _LARGE_FILE_COLS:
        data_context = _columns_only_context(profile)
    else:
        data_context = _sample_rows_context(source_path, profile, _SAMPLE_ROWS)

    sampling_strategy = (
        "columns_only" if (profile.row_count > _LARGE_FILE_ROWS or profile.column_count > _LARGE_FILE_COLS)
        else f"sample_{_SAMPLE_ROWS}_rows"
    )

    if run:
        run.log(
            EventType.OP_STARTED,
            stage="schema_discovery",
            data={
                "op": "schema_discovery",
                "sampling_strategy": sampling_strategy,
                "input_rows": profile.row_count,
                "data_context_preview": data_context[:600],
                "data_context_chars": len(data_context),
            },
        )

    prompt = SCHEMA_DISCOVERY_TEMPLATE.format(
        encoding=profile.encoding,
        delimiter=repr(profile.delimiter),
        row_count=profile.row_count,
        column_count=profile.column_count,
        data_context=data_context,
        sid_examples=", ".join(mapping_engine.get_sample_sids(20)),
        iid_examples=", ".join(mapping_engine.get_sample_iids(20)),
    )

    if run:
        run.log(
            EventType.OP_STARTED,
            stage="schema_discovery",
            data={
                "op": "prompt_assembly",
                "input_rows": profile.row_count,
                "prompt_length": len(prompt),
                "data_context_length": len(data_context),
                "sid_examples_count": len(mapping_engine.get_sample_sids(20)),
                "iid_examples_count": len(mapping_engine.get_sample_iids(20)),
                "template_used": "SCHEMA_DISCOVERY_TEMPLATE",
            },
        )

    config: SchemaConfig = call_structured(prompt, SchemaConfig, run=run, stage="schema_discovery")

    # Post-process: Validate and correct common LLM mistakes for case_id vs patient_id
    config = _validate_id_columns(config, profile, run)
    
    # Post-process: Validate and correct format_type based on EPA structure
    config = _validate_format_type(config, profile, run)

    if run:
        config_data = config.model_dump()
        run.log(
            EventType.SCHEMA_DISCOVERED,
            stage="schema_discovery",
            data={
                **config_data,
                "decision_summary": {
                    "category": config.data_category,
                    "format": config.format_type,
                    "case_id": config.case_id_column,
                    "patient_id": config.patient_id_column,
                    "columns_to_drop_count": len(config.columns_to_drop) if config.columns_to_drop else 0,
                    "columns_to_drop": config.columns_to_drop,
                    "has_epa_config": config.epa is not None,
                    "epa_encoding": config.epa.encoding_type if config.epa else None,
                    "epa_identifier": config.epa.identifier_type if config.epa else None,
                    "anomalies": config.anomalies,
                },
            },
        )

    return config


# ---------------------------------------------------------------------------
# Sampling helpers
# ---------------------------------------------------------------------------

def _columns_only_context(profile: FileProfile) -> str:
    lines = ["Column names only (file too large for row sampling):"]
    lines.append(", ".join(profile.headers_raw))
    return "\n".join(lines)


def _sample_rows_context(
    source_path: Path,
    profile: FileProfile,
    n: int,
) -> str:
    try:
        df = pd.read_csv(
            source_path,
            sep=profile.delimiter,
            encoding=profile.encoding,
            nrows=n + profile.column_count,  # extra buffer for skipped rows
            low_memory=False,
            on_bad_lines="skip",
        )
    except Exception:
        # Fallback: plain column list
        return _columns_only_context(profile)

    df = df.head(n)
    return df.to_string(index=False, max_cols=40)


# ---------------------------------------------------------------------------
# Post-processing validation for ID columns
# ---------------------------------------------------------------------------

# Columns that are DEFINITELY case/encounter IDs (German hospital data patterns)
_CASE_ID_PATTERNS = {
    "patfal", "fallid", "fallnr", "fall_id", "case_id", "caseid",
    "encounter_id", "encounterid", "fallnummer", "e2_i_222", "coe2i222",
}

# Columns that are DEFINITELY patient IDs
_PATIENT_ID_PATTERNS = {
    "pid", "patient_id", "patientid", "patnr", "patientnr", "patientennummer",
}

# Columns that are NOT IDs (common LLM mistakes)
_NOT_ID_PATTERNS = {
    "patgeb": "date_of_birth",      # Geburtsdatum = DOB
    "patadt": "admission_date",     # Aufnahmedatum = admission date
    "patfoe": "type_of_stay",       # Fallart/Aufnahmeart = type of stay
    "geburtsdatum": "date_of_birth",
    "dob": "date_of_birth",
    "date_of_birth": "date_of_birth",
    "aufnahme": "admission_date",
    "admission": "admission_date",
    "entlassung": "discharge_date",
    "discharge": "discharge_date",
}


def _validate_id_columns(
    config: SchemaConfig,
    profile: FileProfile,
    run: "PipelineRun | None" = None,
) -> SchemaConfig:
    """
    Validate and correct common LLM mistakes in case_id_column and patient_id_column.
    
    Common mistakes:
    - PATGEB (date of birth) identified as patient_id
    - PATFAL (case ID) identified as patient_id instead of case_id
    """
    from src.observability.models import EventType
    
    corrections = []
    headers_lower = {h.lower(): h for h in profile.headers_raw}
    
    # Check if patient_id_column is actually NOT an ID
    if config.patient_id_column:
        pid_lower = config.patient_id_column.lower()
        if pid_lower in _NOT_ID_PATTERNS:
            actual_type = _NOT_ID_PATTERNS[pid_lower]
            corrections.append(
                f"patient_id_column '{config.patient_id_column}' is actually {actual_type}, setting to null"
            )
            config.patient_id_column = None
        elif pid_lower in _CASE_ID_PATTERNS:
            # LLM put case ID in patient_id field
            if not config.case_id_column:
                corrections.append(
                    f"patient_id_column '{config.patient_id_column}' is actually a case_id, moving to case_id_column"
                )
                config.case_id_column = config.patient_id_column
                config.patient_id_column = None
    
    # Check if case_id_column is actually NOT an ID
    if config.case_id_column:
        cid_lower = config.case_id_column.lower()
        if cid_lower in _NOT_ID_PATTERNS:
            actual_type = _NOT_ID_PATTERNS[cid_lower]
            corrections.append(
                f"case_id_column '{config.case_id_column}' is actually {actual_type}, setting to null"
            )
            config.case_id_column = None
    
    # If we still don't have a case_id, try to find one in headers
    if not config.case_id_column:
        for pattern in _CASE_ID_PATTERNS:
            if pattern in headers_lower:
                actual_col = headers_lower[pattern]
                corrections.append(
                    f"Auto-detected case_id_column: '{actual_col}' (pattern: {pattern})"
                )
                config.case_id_column = actual_col
                break
    
    # Log corrections
    if corrections and run:
        run.log(
            EventType.OP_STARTED,
            stage="schema_discovery",
            data={
                "op": "id_column_validation",
                "input_rows": 0,
                "corrections": corrections,
                "final_case_id": config.case_id_column,
                "final_patient_id": config.patient_id_column,
            },
        )
    
    return config


def _validate_format_type(
    config: SchemaConfig,
    profile: FileProfile,
    run: "PipelineRun | None" = None,
) -> SchemaConfig:
    """
    Validate and correct format_type based on EPA structure.
    
    Key rule: If epa.sid_column is set (SID codes in a data column, not headers),
    then format_type MUST be "long" because the data needs pivoting.
    """
    from src.observability.models import EventType
    
    corrections = []
    
    # If EPA config exists and has sid_column set, format must be "long"
    if config.epa and config.epa.sid_column:
        if config.format_type != "long":
            corrections.append(
                f"format_type was '{config.format_type}' but epa.sid_column='{config.epa.sid_column}' "
                f"indicates LONG format (SID codes in data column). Correcting to 'long'."
            )
            config.format_type = "long"
    
    # Also check if headers contain SID-like patterns as a column name (not as values)
    # Common long-format indicator columns
    long_format_indicators = {"sid", "sid_value", "item_id", "parameter_code", "assessment_code"}
    headers_lower = {h.lower() for h in profile.headers_raw}
    
    if headers_lower & long_format_indicators:
        # Has SID/value columns - likely long format
        if config.format_type != "long" and config.data_category == "epa_ac":
            found_indicators = headers_lower & long_format_indicators
            corrections.append(
                f"format_type was '{config.format_type}' but found long-format indicator columns: "
                f"{found_indicators}. Correcting to 'long'."
            )
            config.format_type = "long"
    
    # Log corrections
    if corrections and run:
        run.log(
            EventType.OP_STARTED,
            stage="schema_discovery",
            data={
                "op": "format_type_validation",
                "input_rows": 0,
                "corrections": corrections,
                "final_format_type": config.format_type,
                "epa_sid_column": config.epa.sid_column if config.epa else None,
            },
        )
    
    return config
