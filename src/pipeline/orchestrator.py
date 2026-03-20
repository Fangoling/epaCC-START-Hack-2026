"""
Pipeline Orchestrator — 4-stage pipeline.

  Stage 1: preflight()        → FileProfile        (deterministic)
  Stage 2: discover_schema()  → SchemaConfig        (AI)
  Stage 3: transform()        → normalised DataFrame (deterministic)
  Stage 4: router.write()     → RoutingResult       (DB write)
"""

from __future__ import annotations

import time
from pathlib import Path


_IID_SID_CSV = Path(__file__).parent.parent.parent / "IID-SID-ITEM.csv"


def _safe_sample_values(df, max_cols: int = 15) -> dict:
    """
    Safely extract sample values from a DataFrame, handling duplicate column names.
    When df[col] returns a DataFrame (due to duplicate names), take the first column.
    """
    import pandas as pd
    result = {}
    seen_cols = set()
    for i, col in enumerate(df.columns):
        if i >= max_cols:
            break
        # Skip duplicate column names (already processed)
        if col in seen_cols:
            continue
        seen_cols.add(col)
        try:
            series = df.iloc[:, i]  # Use positional indexing to get single column
            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            result[col] = series.dropna().head(2).tolist()
        except Exception:
            result[col] = []
    return result


class Pipeline:
    """
    Chains the four pipeline stages and exposes run() / run_all().
    MappingEngine is loaded once at construction time.
    """

    def __init__(
        self,
        db_path: str | Path = "output/health_data.db",
        log_dir: str | Path = "logs",
        iid_sid_csv: str | Path | None = None,
    ) -> None:
        from src.pipeline.mapping_engine import MappingEngine

        self.db_path = str(Path(db_path))
        self.log_dir = Path(log_dir)

        csv_path = Path(iid_sid_csv) if iid_sid_csv else _IID_SID_CSV
        self.mapping_engine = MappingEngine(csv_path)

        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def run(self, source_path: str | Path) -> dict:
        import pandas as pd
        from src.observability import PipelineRun
        from src.pipeline.inspector import preflight
        from src.pipeline.schema_discovery import discover_schema
        from src.pipeline.transformation_engine import transform
        from src.pipeline.router import TargetRouter

        source_path = Path(source_path)

        with PipelineRun(source_file=str(source_path), log_dir=str(self.log_dir)) as run:
            from src.observability.models import EventType

            # Stage 1 — Preflight
            t0 = time.monotonic()
            profile = preflight(source_path, run=run)
            run.log(EventType.OP_COMPLETED, stage="preflight", duration_ms=(time.monotonic() - t0) * 1000,
                    data={"op": "preflight", "rows": profile.row_count, "columns": profile.column_count,
                          "encoding": profile.encoding, "delimiter": profile.delimiter})

            # Stage 2 — AI schema discovery
            t0 = time.monotonic()
            config = discover_schema(source_path, profile, self.mapping_engine, run=run)
            run.log(EventType.OP_COMPLETED, stage="schema_discovery", duration_ms=(time.monotonic() - t0) * 1000,
                    data={"op": "schema_discovery", "category": config.data_category,
                          "format_type": config.format_type, "case_id_column": config.case_id_column})

            # Stage 3 — Load raw + transform
            t0 = time.monotonic()
            df_raw = pd.read_csv(
                source_path,
                sep=profile.delimiter,
                encoding=profile.encoding,
                low_memory=False,
                on_bad_lines="skip",
            )
            run.log(EventType.OP_STARTED, stage="load_csv",
                    data={"op": "load_csv", "rows": len(df_raw), "columns": len(df_raw.columns),
                          "input_rows": len(df_raw),
                          "column_names": list(df_raw.columns),
                          "dtypes": {col: str(dtype) for col, dtype in df_raw.dtypes.items()},
                          "null_counts": {col: int(df_raw[col].isna().sum()) for col in df_raw.columns},
                          "sample_values": _safe_sample_values(df_raw, 20)})

            df_norm, unmapped = transform(df_raw, config, self.mapping_engine, run=run)
            run.log(EventType.OP_COMPLETED, stage="transform", duration_ms=(time.monotonic() - t0) * 1000,
                    data={"op": "transform", "input_rows": len(df_raw), "output_rows": len(df_norm),
                          "unmapped_count": len(unmapped)})

            if unmapped:
                run.log(EventType.COLUMN_UNMAPPED, stage="transform",
                        data={"source_column": f"({len(unmapped)} total)", "reason": "no DDL mapping found",
                              "unmapped_columns": unmapped})

            # Log final DataFrame state after transformation
            run.log(EventType.OP_COMPLETED, stage="post_transform_summary",
                    duration_ms=0,
                    data={"op": "post_transform_summary",
                          "input_rows": len(df_raw), "output_rows": len(df_norm),
                          "final_columns": list(df_norm.columns),
                          "mapped_columns": [c for c in df_norm.columns if c not in unmapped],
                          "unmapped_columns": unmapped,
                          "mapped_count": len(df_norm.columns) - len(unmapped),
                          "unmapped_count": len(unmapped),
                          "sample_values": _safe_sample_values(df_norm, 15)})

            # Stage 4 — Write to DB
            router = TargetRouter(db_path=self.db_path, run=run)
            result = router.write(df_norm, config, frame_name=source_path.name)

            summary = run.summary()

        return {
            "source_file": str(source_path),
            "profile": profile.model_dump(),
            "config": config.model_dump(),
            "unmapped_columns": unmapped,
            "routing_result": result.model_dump(),
            "run_summary": summary,
        }

    def run_all(
        self,
        directory: str | Path,
        glob: str = "*.csv",
        stop_on_error: bool = False,
    ) -> list[dict]:
        directory = Path(directory)
        files = sorted(directory.glob(glob))
        results: list[dict] = []

        for path in files:
            try:
                results.append(self.run(path))
            except Exception as exc:
                print(f"[Pipeline] ERROR processing {path.name}: {exc}")
                results.append({"source_file": str(path), "error": str(exc)})
                if stop_on_error:
                    raise

        return results
