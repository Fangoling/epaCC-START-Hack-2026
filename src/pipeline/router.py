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
        from src.ai_mapping.tools import check_case_exists

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

        # Find case ID column for INSERT/UPDATE decision
        case_col = next(
            (c for c in frame.columns if c.lower() in
             [x.lower() for x in _CASE_ID_CANDIDATES]),
            None,
        )

        inserts = updates = errors = 0

        import sqlalchemy as sa
        engine = sa.create_engine(f"sqlite:///{self.db_path}", echo=False)

        # Ensure target table exists (create if missing based on frame columns)
        _ensure_table(engine, table, frame)

        with engine.begin() as conn:
            for _, row in frame.iterrows():
                row_dict = row.where(pd.notna(row), None).to_dict()

                # Apply transformation if available
                if transform_fn:
                    try:
                        row_dict = transform_fn(row_dict)
                    except Exception:
                        pass  # Use original row on transform failure

                # Decide INSERT vs UPDATE
                action = "INSERT"
                if case_col and self.db_path:
                    case_val = row_dict.get(case_col)
                    if case_val is not None:
                        try:
                            lookup = check_case_exists(
                                case_id=int(case_val), db_path=self.db_path
                            )
                            action = lookup.get("action", "INSERT")
                        except Exception:
                            action = "INSERT"

                try:
                    if action == "INSERT":
                        conn.execute(sa.text(
                            f"INSERT OR IGNORE INTO {table} "
                            f"({', '.join(row_dict.keys())}) "
                            f"VALUES ({', '.join(':' + k for k in row_dict.keys())})"
                        ), row_dict)
                        inserts += 1
                    else:
                        # UPDATE using case_col as WHERE key
                        set_clause = ", ".join(
                            f"{k} = :{k}" for k in row_dict if k != case_col
                        )
                        if set_clause:
                            conn.execute(sa.text(
                                f"UPDATE {table} SET {set_clause} "
                                f"WHERE {case_col} = :{case_col}"
                            ), row_dict)
                        updates += 1
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
    Create the target table if it does not already exist.
    Uses TEXT columns for everything — schema mapping is the LLM's job.
    """
    import sqlalchemy as sa

    insp = sa.inspect(engine)
    if table in insp.get_table_names():
        return

    col_defs = ", ".join(f'"{c}" TEXT' for c in frame.columns)
    with engine.begin() as conn:
        conn.execute(sa.text(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})'))
