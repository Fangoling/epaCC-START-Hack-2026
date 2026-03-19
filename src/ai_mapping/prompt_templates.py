"""
Prompt templates for the AI Mapping Agent.
Each template is a function that receives pre-formatted context strings and returns
a complete prompt string ready to send to the LLM.
"""

# ---------------------------------------------------------------------------
# Template 1 — Column Mapping
# Ask the LLM to produce a Python dict mapping source CSV columns → target DB columns.
# ---------------------------------------------------------------------------

COLUMN_MAPPING_TEMPLATE = """\
You are a healthcare data integration expert. Your task is to map columns from a \
source CSV file to the target database schema.

=== UNIFICATION RULES ===
{unification_rules}

=== IID / SID MAPPING (IID code → German name → English name) ===
{iid_sid_mapping}

=== TARGET SCHEMA (table: {target_table}) ===
{target_schema}

=== SOURCE FILE SAMPLE (first 5 rows) ===
{sample_rows}

=== SOURCE COLUMN HEADERS ===
{source_headers}

=== TASK ===
Produce a JSON object that maps each source column header to the corresponding \
target column name in the schema above.

Rules:
- Use null if a source column has no match in the target schema.
- Use the IID/SID mapping to resolve codes like EPA0001, E0_I_001, or coE0I001.
- Apply the unification rules for date formats, null sentinels, and ID formats.
- Return ONLY a valid JSON object. No explanation, no markdown fences.

Example output format:
{{
  "CaseID":   "coE2I222",
  "PATGEB":   "coE2I223",
  "EPA0001":  "coE0I001",
  "BadCol":   null
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
