"""
Prompt templates for the AI Mapping Agent.
Each template is a function that receives pre-formatted context strings and returns
a complete prompt string ready to send to the LLM.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.ai_mapping.profiler import FileProfile, ColumnProfile

# ---------------------------------------------------------------------------
# Template 1 — Column Mapping
# Ask the LLM to produce a Python dict mapping source CSV columns → target DB columns.
# ---------------------------------------------------------------------------

COLUMN_MAPPING_TEMPLATE = """\
You are a healthcare data integration expert. Your task is to map columns from a \
source CSV file to the target database schema.

=== UNIFICATION RULES ===
{unification_rules}

=== IID / SID MAPPING (SID code → IID code → DDL column) ===
SID codes like "00_01" → IID "E0_I_001" → DDL column "coE0I001" (add "co", remove underscores).
SID codes like "08_02" → IID "E2_I_042" → DDL column "coE2I042".
{iid_sid_mapping}

=== TARGET SCHEMA (table: {target_table}) ===
{target_schema}

=== SOURCE FILE SAMPLE (first 5 rows) ===
{sample_rows}

=== SOURCE COLUMN HEADERS ===
{source_headers}

=== IMPORTANT RULES FOR CASE ANCHOR COLUMNS ===
The following source columns contain CASE-LEVEL metadata stored in a separate \
"tbCaseData" table and are handled automatically by the pipeline. Do NOT map them \
to any column in {target_table}. Return null for them:
- Case/encounter ID columns: FallID, PATFAL, case_id, FallNr, encounter_id
- Patient demographics: PID, patient_id, PATGEB, Geburtsdatum, date_of_birth
- Admission/discharge dates: Aufnahme, Entlassund, Entlassung, admission_date, discharge_date
- Names: Nachname, Vorname, lastname, firstname
- Gender: Geschlecht, sex, gender
- The import table has "coCaseId" as a FK — it is populated automatically; never map to it.

=== TASK ===
For EACH source column header, produce a mapping entry with:
  - "source_column": the exact source header
  - "target_column": the matching DDL column name in {target_table}, or null if no match or \
if it is a case-anchor column (listed above)
  - "transformation_note": optional note (e.g. "parse date", "SID 00_01 → coE0I001")

To resolve SID/IID codes:
- SID format "XX_YY" → look up in IID/SID mapping → strip underscores from IID → prepend "co"
- SAP ABAP names like "EPA0001" often correspond to IID items (EPA = E, first digit group = series)

Return a JSON object with keys "mappings" (array) and "unmapped_columns" (array of strings).

Example output:
{{
  "mappings": [
    {{"source_column": "FallID",   "target_column": null,       "transformation_note": "case anchor — handled separately"}},
    {{"source_column": "00_01",    "target_column": "coE0I001", "transformation_note": "SID 00_01 → IID E0_I_001"}},
    {{"source_column": "EPA0001",  "target_column": "coE0I001", "transformation_note": "SAP EPA0001 → assessment type"}},
    {{"source_column": "BadCol",   "target_column": null,       "transformation_note": null}}
  ],
  "unmapped_columns": ["BadCol"]
}}
"""

# ---------------------------------------------------------------------------
# Template 2 — Transformation Script
# Ask the LLM to generate a standalone Python function that transforms one
# source DataFrame row into a dict ready for DB insertion.
# ---------------------------------------------------------------------------

TRANSFORMATION_SCRIPT_TEMPLATE = """\
You are a healthcare data integration expert. Write a Python function that \
transforms rows from a source CSV into records for the target database table.

=== UNIFICATION RULES ===
{unification_rules}

=== IID / SID MAPPING (IID code → German name → English name) ===
{iid_sid_mapping}

=== TARGET SCHEMA (table: {target_table}) ===
{target_schema}

=== COLUMN MAPPING (source → target, null means skip) ===
{column_mapping}

=== SOURCE FILE SAMPLE (first 5 rows) ===
{sample_rows}

=== TASK ===
Write a Python function with this exact signature:

    def transform_row(row: dict) -> dict:
        ...

Requirements:
- Input: a dict where keys are source column headers.
- Output: a dict where keys are target column names (from the schema above).
- Skip columns mapped to null.
- Apply these transformations based on the unification rules:
  * Null sentinels (NULL, null, Missing, missing, unknow, NaN, nan, N/A, n/a, \
empty string) → Python None
  * case_id: strip "CASE-" prefix and any suffix after "-", convert to int
  * Dates: parse any reasonable format → ISO 8601 string "YYYY-MM-DD"
  * German decimals: replace comma with period before float conversion
  * Gender normalisation: "male"/"männlich"/"M" → "M", "female"/"weiblich"/"F" → "F"
- Return ONLY the function definition as valid Python. No explanation, no markdown fences.
"""

# ---------------------------------------------------------------------------
# Template 3 — Quality Check (simple validation prompt)
# ---------------------------------------------------------------------------

QUALITY_CHECK_TEMPLATE = """\
You are a healthcare data quality analyst.

=== UNIFICATION RULES ===
{unification_rules}

=== SOURCE FILE SAMPLE (first 5 rows) ===
{sample_rows}

=== TASK ===
Identify data quality issues in the sample above. For each issue produce a JSON \
array entry with:
  - "column": the column name
  - "row_index": 0-based row index (0 = first data row)
  - "original_value": the raw value
  - "issue_type": one of MISSING_MANDATORY, NULL_VARIANT, INVALID_FORMAT, \
OUT_OF_RANGE, DUPLICATE_ID, UNKNOWN_CLINIC_ID
  - "severity": WARNING or ERROR
  - "suggestion": how to fix it

Return ONLY a valid JSON array. No explanation, no markdown fences.
"""

# ---------------------------------------------------------------------------
# Template 4 — Universal Schema Discovery
# Sent for EVERY file (all categories) to determine structure.
# The LLM output is validated against SchemaConfig (Pydantic).
# ---------------------------------------------------------------------------

SCHEMA_DISCOVERY_TEMPLATE = """\
You are a clinical data integration expert. Your task is to analyse the \
structure of a clinical data file and return a JSON configuration that \
describes how to parse and normalise it.

=== FILE METADATA ===
Encoding   : {encoding}
Delimiter  : {delimiter}
Row count  : {row_count}
Column count: {column_count}

=== RAW DATA ===
{data_context}

=== KNOWN DATA CATEGORIES ===
- epa_ac       : EPA Assessment (Einschätzungsprotokoll Aktivitäten & Kommunikation)
- labs         : Laboratory results (sodium, creatinine, hemoglobin, flags H/L, units)
- icd10_ops    : ICD-10 / OPS diagnostic and procedure codes
- medication   : Inpatient medication orders (ATC codes, dose, route)
- nursing      : Nursing daily reports (shift, ward, free-text notes)
- device_motion: Wearable / bed sensor data (~1 reading per minute)
- device_1hz   : High-frequency sensor data (1 Hz, >50 000 rows)
- unknown      : Does not match any known category

=== EPA IDENTIFIER CONTEXT ===
Sample SID codes (raw source identifiers): {sid_examples}
Sample IID codes (internal canonical IDs): {iid_examples}

EPA files come in several structural variants:
1. SID as column headers: column names look like "08_02", "00_01"
2. IID as column headers: column names look like "E2_I_042", "E0_I_001"
3. SID in second row: row 0 is metadata/description, row 1 contains SID codes
4. Encoded column names: SAP ABAP names like "EPA0001", or base64-encoded strings
5. Long format: each data row represents ONE assessment item \
(columns: patient_id, item_identifier, value); rows must be pivoted

These are examples only — the actual structure may differ. Describe what you \
observe in free-form text in the epa.identifier_type field.

=== CRITICAL: format_type DETERMINATION ===
- format_type="wide": Each ROW is a complete record. Assessment values are in COLUMNS.
  Example: columns are [case_id, coE0I001, coE0I002, coE0I003, ...] with ONE row per case.
  
- format_type="long": Each ROW is a SINGLE measurement. Must be PIVOTED to wide.
  Example: columns are [case_id, SID, SID_value] with MANY rows per case.
  Key indicator: A column contains SID codes (like "00_01", "00_02") as VALUES, not headers.
  If you see a column named "SID", "item_id", "parameter", "code" containing EPA codes,
  and another column with the measurement value — this is LONG format!
  
IMPORTANT: If epa.sid_column is set (SID codes appear in a data column, not as headers),
then format_type MUST be "long".

=== IMPORTANT: CASE ID vs PATIENT ID DISTINCTION ===
- case_id_column: The ENCOUNTER/VISIT/FALL identifier. Each hospital visit/stay gets a unique case ID.
  Common names: FallID, PATFAL, FallNr, case_id, encounter_id, Fallnummer, E2_I_222
  Pattern: Often numeric IDs like 2300, 4000, 6000 or prefixed like "CASE-2300"
  
- patient_id_column: The PATIENT identifier (same patient across multiple visits).
  Common names: PID, patient_id, PatientID, PatNr, Patientennummer
  Pattern: Often alphanumeric, persistent across encounters
  
DO NOT confuse these with:
- PATGEB / Geburtsdatum / DOB: Date of birth (NOT an ID column!)
- PATADT / Aufnahme: Admission date (NOT an ID column!)
- PATFOE: Type of stay/Aufnahmeart (NOT an ID column!)

=== TASK ===
Return a JSON object conforming to this schema:

{{
  "data_category": "<one of the categories above>",
  "format_type": "wide | long",
  "header_row_index": <0-based integer, usually 0>,
  "patient_id_column": "<column name holding PATIENT ID, or null if not present>",
  "case_id_column": "<column name holding CASE/ENCOUNTER/FALL ID, or null if not present>",
  "columns_to_drop": ["<col1>", ...],
  "record_type_column": "<column name or null>",
  "record_type_values": ["<val1>", ...],
  "epa": {{
    "header_row_index": <int>,
    "identifier_type": "<free-form description of EPA structure>",
    "encoding_type": "sap_epa | base64 | null",
    "sid_column": "<col or null>",
    "value_column": "<col or null>"
  }} or null,
  "anomalies": ["<description of any structural anomaly>"]
}}

Rules:
- Set epa only when data_category == "epa_ac".
- columns_to_drop should include change-tracking columns (ZWrt_*, ZDat_*) and \
any obviously irrelevant metadata columns.
- anomalies should list anything unexpected: shifted headers, mixed encodings, \
duplicate IDs, sparse data, unexpected delimiters, etc.
- Return ONLY valid JSON. No explanation, no markdown fences.
"""
