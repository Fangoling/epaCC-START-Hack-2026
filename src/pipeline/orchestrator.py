"""
Pipeline Orchestrator — wires all 5 stages together.

Usage:
    pipeline = Pipeline(db_path="output/health_data.db", log_dir="logs")
    results  = pipeline.run("data/synth_labs.csv")
    # or batch:
    results  = pipeline.run_all("data/")
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class Pipeline:
    """
    Top-level orchestrator that chains:
      Stage 1: FileInspector   → FileProfile
      Stage 2: Planner         → PreprocessingPlan
      Stage 3: PreprocessorRegistry.execute_plan → dict[str, DataFrame]
      Stage 4: (mapping handled inside TargetRouter per frame)
      Stage 5: TargetRouter    → list[RoutingResult]
    """

    def __init__(
        self,
        db_path: str | Path = "output/health_data.db",
        log_dir: str | Path = "logs",
    ):
        from src.pipeline.preprocessors import PreprocessorRegistry

        self.db_path = str(Path(db_path))
        self.log_dir = Path(log_dir)
        self.registry = PreprocessorRegistry()

        # Ensure output dirs exist
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Single file
    # ------------------------------------------------------------------

    def run(self, source_path: str | Path) -> dict:
        """
        Process one source file through the full pipeline.
        Returns a summary dict with profile, plan, frames, and routing results.
        """
        from src.observability import PipelineRun
        from src.pipeline.inspector import inspect_file
        from src.pipeline.planner import generate_plan
        from src.pipeline.router import TargetRouter

        source_path = Path(source_path)

        with PipelineRun(source_file=str(source_path), log_dir=str(self.log_dir)) as run:
            # Stage 1 — Inspect
            profile = inspect_file(source_path, run=run)

            # Stage 2 — Plan
            plan = generate_plan(profile, self.registry, run=run)

            # Stage 3 — Preprocess
            import pandas as pd
            df = pd.read_csv(
                source_path,
                sep=profile.delimiter,
                encoding=profile.encoding,
                low_memory=False,
                on_bad_lines="skip",
            )
            frames = self.registry.execute_plan(df, plan, run=run)

            # Stage 4+5 — Map + Route
            router = TargetRouter(db_path=self.db_path, run=run)
            results = router.route(frames, profile)

            summary = run.summary()

        return {
            "source_file": str(source_path),
            "profile": profile.model_dump(),
            "plan": plan.model_dump(),
            "frames": {k: len(v) for k, v in frames.items()},
            "routing_results": [r.model_dump() for r in results],
            "run_summary": summary,
        }

    # ------------------------------------------------------------------
    # Batch
    # ------------------------------------------------------------------

    def run_all(
        self,
        directory: str | Path,
        glob: str = "*.csv",
        stop_on_error: bool = False,
    ) -> list[dict]:
        """
        Process every file matching `glob` in `directory`.
        Returns a list of per-file summary dicts (errors are captured inline).
        """
        directory = Path(directory)
        files = sorted(directory.glob(glob))
        results: list[dict] = []

        for path in files:
            try:
                result = self.run(path)
                results.append(result)
            except Exception as exc:
                print(f"[Pipeline] ERROR processing {path.name}: {exc}")
                results.append({
                    "source_file": str(path),
                    "error": str(exc),
                })
                if stop_on_error:
                    raise

        return results
