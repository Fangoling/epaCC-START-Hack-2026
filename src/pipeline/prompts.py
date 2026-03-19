"""
Prompt templates for Stage 1 (File Inspector) and Stage 2 (Pre-processing Planner).
"""

# ---------------------------------------------------------------------------
# Stage 1 — File Inspector
# Inputs: file_path, raw_header_sample, sample_rows, row_count, column_count,
#         detected_delimiter, encoding
# Output schema: FileProfile
# ---------------------------------------------------------------------------

INSPECTOR_TEMPLATE = """\
You are a data-engineering expert profiling a hospital data file for import into the EPA clinical database.

## File metadata
Path:         {file_path}
Rows:         {row_count}
Columns:      {column_count}
Delimiter:    {detected_delimiter!r}
Encoding:     {encoding}

## Raw column headers (first 20)
{raw_headers}

## Sample rows (first 5)
{sample_rows}

## Your task
Analyse the file and return a FileProfile JSON with these fields:

- delimiter (str): the correct delimiter character
- encoding (str): the correct encoding
- row_count (int)
- column_count (int)
- headers_raw (list[str]): the column headers exactly as given
- headers_decoded (list[str] | null): decoded headers if base64-encoded, else null
- has_base64_headers (bool): true if headers look like base64-encoded strings
- has_discriminator_column (bool): true if one column splits rows into logical sub-tables
- discriminator_column (str | null): column name if has_discriminator_column
- discriminator_values (list[str]): unique values in that column, e.g. ["ADMIN","ORDER"]
- id_columns (list[str]): column names that hold patient/case/encounter IDs
- id_format_pattern (str | null): pattern like "CASE-NNNN", "PAT-NNNN", or null
- data_category (str): one of epa_ac | labs | medication | icd10_ops | device_motion | device_1hz | nursing | unknown
- anomalies (list[str]): any other oddities you notice (free text)

Rules for data_category:
- epa_ac:       headers are EPA SID codes (like EPA0001, EPAST0BTS) or base64-encoded
- labs:         columns like sodium_mmol_L, creatinine_mg_dL, specimen_datetime
- medication:   columns like medication_code_atc, route, dose, order_id
- icd10_ops:    columns like primary_icd10_code, ops_codes, admission_date
- device_motion: columns like movement_index_0_100, fall_event_0_1, impact_magnitude_g
- device_1hz:   same as device_motion but row_count > 10000 or timestamp has sub-minute resolution
- nursing:      columns like nursing_note_free_text, shift, ward
"""


# ---------------------------------------------------------------------------
# Stage 2 — Pre-processing Planner
# Inputs: file_profile_json, available_ops_json
# Output schema: PreprocessingPlan
# ---------------------------------------------------------------------------

PLANNER_TEMPLATE = """\
You are a data-engineering expert building a pre-processing plan for a hospital data file.

## File profile
{file_profile}

## Available pre-processing ops
{available_ops}

## Your task
Return a PreprocessingPlan JSON with these fields:
- steps (list): ordered list of ops to apply, each with:
    - op (str): exact op name from the available ops list
    - params (dict): keyword arguments to pass (use {{}} if none)
    - rationale (str): one sentence explaining why this step is needed
- llm_rationale (str): overall reasoning for the plan

Rules:
- Only use ops from the available ops list. Do not invent new op names.
- If the file has base64 headers, include decode_base64_headers as the first step.
- If a discriminator_column is present, include split_by_discriminator with that column.
- Always include normalize_id_column for each detected id_column (one step per column).
- For device_1hz files with >50000 rows, include aggregate_timeseries.
- For epa_ac files where SID and SID_value columns exist, include pivot_sid_rows.
- Include strip_change_tracking_columns if headers contain ZWrt_ or ZDat_ patterns.
- Keep the plan minimal: only include steps that are actually needed.
- If no preprocessing is needed, return an empty steps list.
"""
