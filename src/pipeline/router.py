"""
Stage 4 — Target Router.

Writes a normalised DataFrame to the correct target table in the DB.
Table selection is driven by SchemaConfig.data_category.
No heuristic column resolution — columns arrive already renamed to DDL names
by the TransformationEngine.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pandas as pd
import sqlalchemy as sa

from src.pipeline.models import SchemaConfig, RoutingResult
from src.ai_mapping.context_loader import build_sid_to_ddl_column

# SID code → DDL column name (e.g. '08_02' → 'coE2I042'), loaded once at import
_SID_TO_DDL: dict[str, str] = build_sid_to_ddl_column()

if TYPE_CHECKING:
    from src.observability import PipelineRun

# Columns that may hold a case/encounter ID across different file types
_CASE_ID_CANDIDATES = [
    "case_id", "caseid", "fallid", "encounter_id", "coE2I222", "FallID",
    "PATFAL", "FallNr", "FallNr (STRING)", "E2_I_222",
]

# Source column → tbCaseData DDL column (for populating the case anchor table)
_SOURCE_TO_CASE_FIELD: dict[str, str] = {
    # English source names
    "patient_id":     "coPatientId",
    "sex":            "coGender",
    "gender":         "coGender",
    "age_years":      "coAgeYears",
    "age":            "coAgeYears",
    "admission_date": "coE2I223",
    "discharge_date": "coE2I228",
    "lastname":       "coLastname",
    "firstname":      "coFirstname",
    "type_of_stay":   "coTypeOfStay",
    "icd":            "coIcd",
    "drg_name":       "coDrgName",
    "state":          "coState",
    "ward":           "coState",  # ward maps to state/station
    # ICD10 file columns
    "primary_icd10_code":        "coIcd",
    "primary_icd10_description_en": "coDrgName",  # Use ICD description as DRG name if no DRG
    # DDL column names (pass-through when already renamed)
    "coPatientId":    "coPatientId",
    "coGender":       "coGender",
    "coAgeYears":     "coAgeYears",
    "coE2I223":       "coE2I223",
    "coE2I228":       "coE2I228",
    "coLastname":     "coLastname",
    "coFirstname":    "coFirstname",
    "coDateOfBirth":  "coDateOfBirth",
    "coTypeOfStay":   "coTypeOfStay",
    "coIcd":          "coIcd",
    "coDrgName":      "coDrgName",
    "coState":        "coState",
    # SAP / German column names (Data-2)
    "PATGEB":         "coDateOfBirth",
    "PATADT":         "coE2I223",
    "PATFOE":         "coTypeOfStay",
    "PATDOE":         "coState",  # Discharge ward/station
    # German source names (Data-1)
    "Aufnahme":       "coE2I223",
    "Entlassund":     "coE2I228",
    "Station":        "coState",
    "PID":            "coPatientId",
    # Admission/discharge datetime (medication file)
    "admission_datetime": "coE2I223",
    "discharge_datetime": "coE2I228",
}

CATEGORY_TABLE: dict[str, str] = {
    "epa_ac":        "tbImportAcData",
    "labs":          "tbImportLabsData",
    "icd10_ops":     "tbImportIcd10Data",
    "device_motion": "tbImportDeviceMotionData",
    "device_1hz":    "tbImportDevice1HzMotionData",
    "medication":    "tbImportMedicationInpatientData",
    "nursing":       "tbImportNursingDailyReportsData",
    "unknown":       "tbImportUnknownData",
}


class TargetRouter:
    """Routes a normalised DataFrame to its target DB table."""

    def __init__(self, db_path: str, run: "PipelineRun | None" = None) -> None:
        self.db_path = db_path
        self._run = run

    def write(
        self,
        df: pd.DataFrame,
        config: SchemaConfig,
        frame_name: str = "main",
    ) -> RoutingResult:
        """Write df to the target table determined by config.data_category."""
        from src.observability.models import EventType
        from src.ai_mapping.context_loader import load_target_schema

        table = CATEGORY_TABLE.get(config.data_category, "tbImportUnknownData")

        if self._run:
            self._run.log(
                EventType.ROUTE_DECIDED,
                stage="routing",
                data={"frame_name": frame_name, "target_table": table, "row_count": len(df)},
            )

        engine = sa.create_engine(f"sqlite:///{self.db_path}", echo=False)

        _ensure_table(engine, "tbCaseData", pd.DataFrame())
        _ensure_table(engine, table, df)

        insp = sa.inspect(engine)
        table_columns: set[str] = {col["name"] for col in insp.get_columns(table)}

        inserts = updates = errors = 0
        _first_row_logged = False
        _col_accept_methods: dict[str, str] = {}  # col → method used (for summary)
        _col_rejected: list[str] = []  # cols that matched no strategy

        # Find case_id column: prefer config, fall back to candidates
        case_id_col = config.case_id_column
        if not case_id_col:
            col_lower_map = {str(c).lower(): c for c in df.columns}
            for cand in _CASE_ID_CANDIDATES:
                if cand in df.columns:
                    case_id_col = cand
                    break
                # Also try case-insensitive match
                actual = col_lower_map.get(cand.lower())
                if actual:
                    case_id_col = actual
                    break

        if self._run:
            self._run.log(
                EventType.OP_STARTED,
                stage="routing",
                data={
                    "op": "column_filter_setup",
                    "input_rows": 0,
                    "case_id_column": case_id_col,
                    "case_id_source": "config" if config.case_id_column else "candidate_search",
                    "target_table": table,
                    "table_columns": sorted(table_columns),
                    "df_columns": list(df.columns),
                    "df_column_count": len(df.columns),
                    "table_column_count": len(table_columns),
                },
            )

        with engine.begin() as conn:
            for _, row in df.iterrows():
                # Sanitise DataFrame column keys: strip SAP-style " (STRING)" suffixes
                raw_dict = row.where(pd.notna(row), None).to_dict()
                row_dict = {
                    re.sub(r'\s*\(.*?\)\s*$', '', k).strip() or k: v
                    for k, v in raw_dict.items()
                }

                # Upsert case anchor
                case_db_id: int | None = None
                case_int: int | None = None  # The actual case ID value
                raw_case_id = None
                # Sanitise case_id_col name too
                clean_case_id_col = (
                    re.sub(r'\s*\(.*?\)\s*$', '', case_id_col).strip() or case_id_col
                    if case_id_col else None
                )
                if clean_case_id_col and clean_case_id_col in row_dict:
                    raw_case_id = row_dict.get(clean_case_id_col)
                else:
                    # Try all candidates
                    for cand in _CASE_ID_CANDIDATES:
                        raw_case_id = row_dict.get(cand)
                        if raw_case_id is not None:
                            break

                if raw_case_id is not None:
                    case_int = _to_int(raw_case_id)
                    if case_int is not None:
                        case_fields = _extract_case_fields(row_dict)
                        case_db_id = _upsert_case(conn, case_int, case_fields)
                        if not _first_row_logged and self._run:
                            self._run.log(
                                EventType.OP_STARTED,
                                stage="routing",
                                data={
                                    "op": "case_anchor_upsert_sample",
                                    "input_rows": 0,
                                    "raw_case_id": str(raw_case_id),
                                    "case_int": case_int,
                                    "case_db_id": case_db_id,
                                    "case_fields_extracted": {k: str(v) for k, v in case_fields.items()},
                                    "case_fields_count": len(case_fields),
                                },
                            )
                    elif not _first_row_logged and self._run:
                        self._run.log(
                            EventType.OP_STARTED,
                            stage="routing",
                            data={
                                "op": "case_anchor_skip",
                                "input_rows": 0,
                                "raw_case_id": str(raw_case_id),
                                "reason": "could not convert to int",
                            },
                        )
                else:
                    # No direct case ID found - try to look up by patient_id
                    patient_id = row_dict.get("patient_id") or row_dict.get("coPatient_id")
                    if patient_id is not None:
                        case_int = _lookup_case_by_patient(conn, patient_id)
                        if case_int is not None:
                            if not _first_row_logged and self._run:
                                self._run.log(
                                    EventType.OP_STARTED,
                                    stage="routing",
                                    data={
                                        "op": "case_lookup_by_patient",
                                        "input_rows": 0,
                                        "patient_id": str(patient_id),
                                        "case_int": case_int,
                                    },
                                )
                        elif not _first_row_logged and self._run:
                            self._run.log(
                                EventType.OP_STARTED,
                                stage="routing",
                                data={
                                    "op": "case_lookup_failed",
                                    "input_rows": 0,
                                    "patient_id": str(patient_id),
                                    "reason": "no case found for patient_id in tbCaseData",
                                },
                            )
                    elif not _first_row_logged and self._run:
                        self._run.log(
                            EventType.OP_STARTED,
                            stage="routing",
                            data={
                                "op": "case_anchor_skip",
                                "input_rows": 0,
                                "reason": "no case_id or patient_id found in row",
                                "case_id_col": clean_case_id_col,
                                "candidates_checked": _CASE_ID_CANDIDATES,
                            },
                        )

                # 3-strategy column filter: direct → SID→DDL → co-prefix
                # (row_dict keys are already sanitized above)
                filtered: dict = {}
                _case_id_lower_set = {c.lower() for c in _CASE_ID_CANDIDATES}
                for k, v in row_dict.items():
                    if k.lower() in _case_id_lower_set:
                        if not _first_row_logged:
                            _col_accept_methods[k] = "skipped_case_anchor"
                        continue  # handled via case upsert above
                    if k in table_columns:
                        filtered[k] = v
                        if not _first_row_logged:
                            _col_accept_methods[k] = "direct_match"
                    elif k in _SID_TO_DDL and _SID_TO_DDL[k] in table_columns:
                        filtered[_SID_TO_DDL[k]] = v
                        if not _first_row_logged:
                            _col_accept_methods[k] = f"sid_to_ddl → {_SID_TO_DDL[k]}"
                    else:
                        # Check if column already starts with "co" prefix (case-insensitive)
                        if k.lower().startswith("co"):
                            # Already has co-prefix, try direct match with proper casing
                            candidate = "co" + k[2].upper() + k[3:] if len(k) > 3 else k
                            # Also try the original key as-is
                            if k in table_columns:
                                filtered[k] = v
                                if not _first_row_logged:
                                    _col_accept_methods[k] = f"direct_match (already co-prefixed)"
                            elif candidate and candidate in table_columns:
                                filtered[candidate] = v
                                if not _first_row_logged:
                                    _col_accept_methods[k] = f"co_recase → {candidate}"
                            else:
                                if not _first_row_logged:
                                    _col_rejected.append(k)
                                    _col_accept_methods[k] = f"REJECTED (co-prefixed but '{k}' and '{candidate}' not in table)"
                        else:
                            candidate = "co" + k[0].upper() + k[1:] if k else ""
                            if candidate and candidate in table_columns:
                                filtered[candidate] = v
                                if not _first_row_logged:
                                    _col_accept_methods[k] = f"co_prefix → {candidate}"
                            else:
                                if not _first_row_logged:
                                    _col_rejected.append(k)
                                    _col_accept_methods[k] = f"REJECTED (not in table, no SID, co-prefix '{candidate}' not found)"

                # Set coCaseId to the actual case ID value (not internal coId)
                # This links import data to the case by the business key (e.g., 135)
                # case_int is set either from direct case_id or from patient_id lookup
                if case_int is not None and "coCaseId" in table_columns:
                    filtered["coCaseId"] = case_int
                
                # For EPA AC table, also write the case ID to coE2I222
                # (coE2I222 is both a case anchor AND an EPA data field in tbImportAcData)
                if case_int is not None and "coE2I222" in table_columns:
                    filtered["coE2I222"] = case_int

                # Log first row's column mapping decisions
                if not _first_row_logged and self._run:
                    self._run.log(
                        EventType.MAPPING_COMPLETED,
                        stage="routing",
                        data={
                            "op": "first_row_column_filter",
                            "column_decisions": _col_accept_methods,
                            "accepted_columns": list(filtered.keys()),
                            "rejected_columns": _col_rejected,
                            "accepted_count": len(filtered),
                            "rejected_count": len(_col_rejected),
                            "mapped": len(filtered),
                            "unmapped": len(_col_rejected),
                            "llm_calls": 0,
                        },
                    )
                    _first_row_logged = True

                if not filtered:
                    if self._run and errors < 3:  # Log first 3 empty-row errors
                        self._run.log(
                            EventType.WRITE_ERROR,
                            stage="routing",
                            data={
                                "table": table,
                                "error": "no columns matched target table",
                                "row_keys": list(row_dict.keys())[:20],
                                "table_columns_sample": sorted(table_columns)[:20],
                            },
                        )
                    errors += 1
                    continue

                try:
                    # Use sanitized parameter keys to avoid SQL errors from
                    # column names containing spaces, parens, or other special chars
                    safe_keys = {k: re.sub(r'[^a-zA-Z0-9_]', '_', k) for k in filtered}
                    col_list = ', '.join(f'"{k}"' for k in filtered)
                    val_list = ', '.join(f':{safe_keys[k]}' for k in filtered)
                    params = {safe_keys[k]: v for k, v in filtered.items()}
                    
                    # Simple INSERT for all tables
                    # - tbImport* tables: multiple rows per case are allowed (medication orders, device readings, etc.)
                    # - coCaseId is a foreign key reference, not a unique constraint
                    conn.execute(
                        sa.text(
                            f'INSERT INTO "{table}" ({col_list}) VALUES ({val_list})'
                        ),
                        params,
                    )
                    inserts += 1
                except Exception as exc:
                    if self._run:
                        self._run.log(
                            EventType.WRITE_ERROR,
                            stage="routing",
                            data={"table": table, "error": str(exc), "columns": list(filtered.keys())},
                        )
                    errors += 1

        result = RoutingResult(
            table=table,
            frame_name=frame_name,
            row_count=len(df),
            inserts=inserts,
            updates=updates,
            errors=errors,
        )

        if self._run:
            self._run.log(
                EventType.ROUTING_COMPLETED,
                stage="routing",
                data=result.model_dump(),
            )

        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_table(engine: sa.Engine, table: str, frame: pd.DataFrame) -> None:
    import re
    from src.ai_mapping.context_loader import load_target_schema

    ddl = load_target_schema(table)
    col_names: list[str] = []
    if "not found" not in ddl:
        for line in ddl.splitlines():
            stripped = line.strip().rstrip(",")
            if not stripped or stripped.lower().startswith(("constraint", "create", "(")):
                continue
            m = re.match(r"^(\w+)\s+\w+", stripped)
            if m:
                col_names.append(m.group(1))

    if not col_names:
        # Sanitise column names from the frame — strip SAP suffixes like " (STRING)"
        col_names = [re.sub(r'\s*\(.*?\)\s*$', '', c).strip() or c for c in frame.columns]

    expected = set(col_names)
    insp = sa.inspect(engine)

    if table in insp.get_table_names():
        existing = {col["name"] for col in insp.get_columns(table)}
        if expected <= existing:
            return
        with engine.begin() as conn:
            conn.execute(sa.text(f'DROP TABLE "{table}"'))

    col_defs_parts = []
    for c in col_names:
        if c.lower() == "coid":
            col_defs_parts.append(f'"{c}" INTEGER PRIMARY KEY AUTOINCREMENT')
        elif c.lower() == "coe2i222" and table.lower() == "tbcasedata":
            # Case ID must be unique in the case anchor table for upsert to work
            col_defs_parts.append(f'"{c}" TEXT UNIQUE')
        else:
            # coCaseId is NOT unique - multiple rows per case are allowed
            # (e.g., multiple medication orders, device readings per case)
            col_defs_parts.append(f'"{c}" TEXT')

    with engine.begin() as conn:
        conn.execute(sa.text(f'CREATE TABLE "{table}" ({", ".join(col_defs_parts)})'))


def _to_int(val) -> int | None:
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _lookup_case_by_patient(conn: sa.Connection, patient_id) -> int | None:
    """
    Look up the case ID (coE2I222) from tbCaseData using patient_id.
    
    This is used when a data file (e.g., device motion) only has patient_id
    but no direct case/encounter ID. We find the case associated with that patient.
    """
    if patient_id is None:
        return None
    
    try:
        # Try to find a case with this patient ID
        row = conn.execute(
            sa.text('SELECT coE2I222 FROM "tbCaseData" WHERE coPatientId = :pid LIMIT 1'),
            {"pid": str(patient_id)},
        ).fetchone()
        if row and row[0]:
            return _to_int(row[0])
    except Exception:
        pass
    return None


def _extract_case_fields(row_dict: dict, run=None) -> dict:
    """
    Pull demographic / admission fields from a source row for tbCaseData.
    
    Uses a hybrid approach:
    1. First tries quick hardcoded mappings for common patterns (fast, no LLM)
    2. Falls back to LLM-based semantic mapping for unknown columns
    """
    out: dict = {}
    unmapped_cols: list[str] = []
    
    # Build a case-insensitive lookup of the row keys
    row_lower: dict[str, str] = {k.lower(): k for k in row_dict}
    
    # 1. Try hardcoded mappings first (fast path)
    for src_col, ddl_col in _SOURCE_TO_CASE_FIELD.items():
        # Try exact match first, then case-insensitive
        val = row_dict.get(src_col)
        if val is None:
            actual_key = row_lower.get(src_col.lower())
            if actual_key:
                val = row_dict[actual_key]
        if val is not None and str(val).strip():
            if ddl_col not in out:  # Don't overwrite existing mappings
                out[ddl_col] = val
    
    # 2. Find columns that weren't mapped
    mapped_src_cols = set()
    for src_col in _SOURCE_TO_CASE_FIELD:
        if src_col in row_dict or src_col.lower() in row_lower:
            mapped_src_cols.add(src_col.lower())
    
    for col in row_dict:
        if col.lower() not in mapped_src_cols:
            val = row_dict[col]
            if val is not None and str(val).strip():
                unmapped_cols.append(col)
    
    # 3. Use semantic mapper for unmapped columns (if any and LLM available)
    if unmapped_cols:
        try:
            from src.ai_mapping.semantic_mapper import map_source_to_case_fields
            semantic_fields = map_source_to_case_fields(
                {col: row_dict[col] for col in unmapped_cols},
                run=run
            )
            # Merge semantic mappings (don't overwrite existing)
            for ddl_col, val in semantic_fields.items():
                if ddl_col not in out and val is not None:
                    out[ddl_col] = val
        except Exception:
            # LLM not available or error - continue with hardcoded mappings only
            pass
    
    return out


def _upsert_case(conn: sa.Connection, case_int: int, extra_fields: dict) -> int | None:
    """
    Upsert a case row into tbCaseData.
    
    Uses MERGE strategy: only update fields that are currently NULL/empty,
    preserving existing values. This prevents fragmentation when multiple
    files contribute data for the same case.
    """
    # First, ensure the case row exists
    conn.execute(
        sa.text('INSERT OR IGNORE INTO "tbCaseData" (coE2I222) VALUES (:cid)'),
        {"cid": case_int},
    )
    
    if extra_fields:
        # Merge strategy: only update fields that are currently NULL or empty
        # Use COALESCE to keep existing non-null values
        set_parts = []
        for col in extra_fields:
            # COALESCE keeps existing value if not null/empty, otherwise uses new value
            # NULLIF converts empty string to NULL for proper COALESCE behavior
            set_parts.append(
                f'"{col}" = COALESCE(NULLIF("{col}", \'\'), :{col})'
            )
        params = {col: val for col, val in extra_fields.items()}
        params["cid"] = case_int
        conn.execute(
            sa.text(f'UPDATE "tbCaseData" SET {", ".join(set_parts)} WHERE coE2I222 = :cid'),
            params,
        )
    
    row = conn.execute(
        sa.text('SELECT coId FROM "tbCaseData" WHERE coE2I222 = :cid'),
        {"cid": case_int},
    ).fetchone()
    return row[0] if row else None
