"""
Stage 5 — Target Router.

Maps each cleaned DataFrame to the correct target table, then uses
MappingAgent to produce column mappings and transformation scripts,
and finally writes rows via INSERT or UPDATE based on case existence checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from src.pipeline.models import FileProfile, RoutingDecision, RoutingResult
from src.ai_mapping.context_loader import build_sid_to_ddl_column

# SID code → DDL column name (e.g. '08_02' → 'coE2I042'), loaded once at import
_SID_TO_DDL: dict[str, str] = build_sid_to_ddl_column()

if TYPE_CHECKING:
    from src.observability import PipelineRun

# ---------------------------------------------------------------------------
# Category → target table mapping
# ---------------------------------------------------------------------------

_CATEGORY_TABLE: dict[str, str] = {
    "epa_ac":       "tbImportAcData",
    "labs":         "tbImportLabsData",
    "icd10_ops":    "tbImportIcd10Data",
    "device_motion":"tbImportDeviceMotionData",
    "device_1hz":   "tbImportDevice1HzMotionData",
    "medication":   "tbImportMedicationInpatientData",
    "nursing":      "tbImportNursingDailyReportsData",
    "unknown":      "tbImportUnknownData",
}

# Columns that may hold a case/encounter ID across different file types
_CASE_ID_CANDIDATES = [
    "case_id", "caseid", "fallid", "encounter_id", "coE2I222", "FallID",
]

# Source column → tbCaseData DDL column (for populating the case anchor table
# without LLM assistance when OLLAMA_API_KEY is not set).
_SOURCE_TO_CASE_FIELD: dict[str, str] = {
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
}


class TargetRouter:
    """
    Routes each named DataFrame from the pre-processing stage to the
    correct target table and calls MappingAgent for column mapping.
    """

    def __init__(self, db_path: str, run: "PipelineRun | None" = None):
        self.db_path = db_path
        self._run = run

    def route(
        self,
        frames: dict[str, "pd.DataFrame"],
        profile: FileProfile,
    ) -> list[RoutingResult]:
        from src.observability.models import EventType

        results: list[RoutingResult] = []
        decisions = self._decide(frames, profile)

        for decision in decisions:
            frame = frames.get(decision.frame_name)
            if frame is None or frame.empty:
                continue

            if self._run:
                self._run.log(
                    EventType.ROUTE_DECIDED,
                    stage="routing",
                    data={
                        "frame_name": decision.frame_name,
                        "target_table": decision.target_table,
                        "row_count": len(frame),
                    },
                )

            result = self._write_frame(frame, decision)
            results.append(result)

        if self._run:
            self._run.log(
                EventType.ROUTING_COMPLETED,
                stage="routing",
                data={
                    "results": [r.model_dump() for r in results],
                },
            )

        return results

    # -----------------------------------------------------------------------
    # Routing decisions
    # -----------------------------------------------------------------------

    def _decide(
        self,
        frames: dict[str, "pd.DataFrame"],
        profile: FileProfile,
    ) -> list[RoutingDecision]:
        """Map each frame name to a target table."""
        base_table = _CATEGORY_TABLE.get(profile.data_category, "tbImportUnknownData")
        decisions: list[RoutingDecision] = []

        for frame_name in frames:
            # If split_by_discriminator produced ADMIN/ORDER sub-frames, both go
            # to the medication table (they are the same logical entity).
            # Other split frames follow the base category.
            decisions.append(RoutingDecision(
                frame_name=frame_name,
                target_table=base_table,
                confidence=1.0,
            ))

        return decisions

    # -----------------------------------------------------------------------
    # Write one frame
    # -----------------------------------------------------------------------

    def _write_frame(
        self,
        frame: "pd.DataFrame",
        decision: RoutingDecision,
    ) -> RoutingResult:
        """
        Apply column mapping + transformation, then INSERT/UPDATE each row.
        Falls back to a direct INSERT if MappingAgent is unavailable.
        """
        import json
        from src.ai_mapping.agent import MappingAgent

        table = decision.target_table

        # Build mapping agent from DataFrame
        agent = MappingAgent(
            source_path=decision.frame_name,
            target_table=table,
            db_path=self.db_path,
            run=self._run,
            source_df=frame,
        )

        mapping = agent.get_column_mapping()
        if mapping:
            print(f"[router] {decision.frame_name}: column mapping via Ollama ({len(mapping.mappings)} columns mapped)")
        else:
            print(f"[router] {decision.frame_name}: column mapping via fallback heuristic (SID lookup + co-prefix)")
        script_src = agent.get_transformation_script(mapping) if mapping else None

        # Compile transformation function if we got one
        transform_fn = None
        if script_src:
            local_ns: dict = {}
            try:
                import pandas as _pd
                import numpy as _np
                exec(script_src, {"pd": _pd, "np": _np, "__builtins__": {}}, local_ns)  # nosec
                transform_fn = local_ns.get("transform_row")
            except Exception as exc:
                print(f"[router] transform_row compile failed: {exc}")

        inserts = updates = errors = 0

        import sqlalchemy as sa
        engine = sa.create_engine(f"sqlite:///{self.db_path}", echo=False)

        # Ensure tbCaseData anchor table exists before any import table
        _ensure_table(engine, "tbCaseData", pd.DataFrame())
        # Ensure target import table exists with full DDL schema
        _ensure_table(engine, table, frame)

        # Get actual columns present in the target table
        insp = sa.inspect(engine)
        table_columns = {col["name"] for col in insp.get_columns(table)}

        with engine.begin() as conn:
            for _, row in frame.iterrows():
                row_dict = row.where(pd.notna(row), None).to_dict()

                # Apply transformation if available
                if transform_fn:
                    try:
                        row_dict = transform_fn(row_dict)
                    except Exception:
                        pass  # Use original row on transform failure

                # --- Case anchor: parse case_id, upsert into tbCaseData ----
                raw_case_id = None
                for cand in _CASE_ID_CANDIDATES:
                    raw_case_id = row_dict.get(cand)
                    if raw_case_id is not None:
                        break
                case_db_id: int | None = None
                if raw_case_id is not None:
                    case_int = _parse_case_id(str(raw_case_id))
                    if case_int is not None:
                        case_fields = _extract_case_fields(row_dict)
                        case_db_id = _upsert_case(conn, case_int, case_fields)

                # --- Column filter: try direct match, SID lookup, then co-prefix per col --
                filtered: dict = {}
                for k, v in row_dict.items():
                    if k in _CASE_ID_CANDIDATES:
                        continue  # handled via case upsert above
                    if k in table_columns:
                        filtered[k] = v
                    elif k in _SID_TO_DDL and _SID_TO_DDL[k] in table_columns:
                        filtered[_SID_TO_DDL[k]] = v
                    else:
                        candidate = "co" + k[0].upper() + k[1:]
                        if candidate in table_columns:
                            filtered[candidate] = v

                # Inject the resolved FK
                if case_db_id is not None and "coCaseId" in table_columns:
                    filtered["coCaseId"] = case_db_id

                row_dict = filtered
                if not row_dict:
                    errors += 1
                    continue

                # Decide INSERT vs UPDATE (always INSERT when no case anchor)
                action = "INSERT"

                try:
                    conn.execute(sa.text(
                        f"INSERT OR IGNORE INTO \"{table}\" "
                        f"({', '.join(row_dict.keys())}) "
                        f"VALUES ({', '.join(':' + k for k in row_dict.keys())})"
                    ), row_dict)
                    inserts += 1
                except Exception as exc:
                    print(f"[router] Write error for table {table}: {exc}")
                    errors += 1

        return RoutingResult(
            table=table,
            frame_name=decision.frame_name,
            row_count=len(frame),
            inserts=inserts,
            updates=updates,
            errors=errors,
        )


# ---------------------------------------------------------------------------
# Helper: ensure target table exists
# ---------------------------------------------------------------------------

def _ensure_table(engine: "sa.Engine", table: str, frame: "pd.DataFrame") -> None:
    """
    Create the target table if it does not already exist, using the column list
    from CreateImportTables.sql.  If the table already exists but is missing
    expected columns (e.g. was created from a truncated schema), it is dropped
    and recreated so that all DDL columns are present.
    """
    import re
    import sqlalchemy as sa
    from src.ai_mapping.context_loader import load_target_schema

    # Parse expected column names from the DDL
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
        col_names = list(frame.columns)

    expected = set(col_names)

    insp = sa.inspect(engine)
    if table in insp.get_table_names():
        existing = {col["name"] for col in insp.get_columns(table)}
        if expected <= existing:
            return  # Schema is already correct
        # Schema is incomplete — drop so it can be recreated below
        with engine.begin() as conn:
            conn.execute(sa.text(f'DROP TABLE "{table}"'))

    col_defs_parts = []
    for c in col_names:
        if c.lower() == "coid":
            col_defs_parts.append(f'"{c}" INTEGER PRIMARY KEY AUTOINCREMENT')
        else:
            col_defs_parts.append(f'"{c}" TEXT')
    col_defs = ", ".join(col_defs_parts)

    with engine.begin() as conn:
        conn.execute(sa.text(f'CREATE TABLE "{table}" ({col_defs})'))


# ---------------------------------------------------------------------------
# Helper: parse case_id string → integer
# ---------------------------------------------------------------------------

def _parse_case_id(raw: str) -> "int | None":
    """
    Normalise a raw case identifier to a plain integer.
    Handles: "CASE-0198", "CASE-0198-01", "CASE-198", "198", "0198".
    """
    import re
    s = re.sub(r"(?i)^case-?0*", "", str(raw).strip())  # strip CASE-/CASE prefix
    s = re.sub(r"-.*$", "", s)                           # strip trailing -suffix
    try:
        return int(s) if s else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Helper: extract tbCaseData fields from a source row dict
# ---------------------------------------------------------------------------

def _extract_case_fields(row_dict: dict) -> dict:
    """
    Pull demographic / admission fields from a source row that belong in
    tbCaseData, using the _SOURCE_TO_CASE_FIELD mapping.
    Returns a dict of {ddl_column: value} with None values excluded.
    """
    out: dict = {}
    for src_col, ddl_col in _SOURCE_TO_CASE_FIELD.items():
        val = row_dict.get(src_col)
        if val is not None:
            out[ddl_col] = val
    return out


# ---------------------------------------------------------------------------
# Helper: upsert one case into tbCaseData, return its coId PK
# ---------------------------------------------------------------------------

def _upsert_case(conn: "sa.Connection", case_int: int, extra_fields: dict) -> "int | None":
    """
    INSERT OR IGNORE a row into tbCaseData keyed on coE2I222 (the case integer).
    If extra_fields contains columns not yet populated, UPDATE them.
    Returns the coId primary key.
    """
    import sqlalchemy as sa

    # Insert if not present
    conn.execute(
        sa.text('INSERT OR IGNORE INTO "tbCaseData" (coE2I222) VALUES (:cid)'),
        {"cid": case_int},
    )

    # Backfill any extra demographic fields that are currently NULL
    if extra_fields:
        set_parts = [f'"{col}" = COALESCE("{col}", :{col})' for col in extra_fields]
        params = {col: val for col, val in extra_fields.items()}
        params["cid"] = case_int
        conn.execute(
            sa.text(
                f'UPDATE "tbCaseData" SET {", ".join(set_parts)} WHERE coE2I222 = :cid'
            ),
            params,
        )

    row = conn.execute(
        sa.text('SELECT coId FROM "tbCaseData" WHERE coE2I222 = :cid'),
        {"cid": case_int},
    ).fetchone()
    return row[0] if row else None
