"""
PipelineRun — context manager that collects PipelineEvents and writes them to JSONL.

Usage:
    with PipelineRun(source_file="synth_labs.csv", log_dir="logs/") as run:
        profile = inspect_file(path, run=run)
        plan    = generate_plan(profile, run=run)
        frames  = execute_plan(df, plan, run=run)
        mapping = agent.get_column_mapping(run=run)
        ...
    # → logs/2026-03-19T14:05:22_abc12345_synth_labs.jsonl

Each stage calls run.log(...) or uses the run.op() context manager.
"""

from __future__ import annotations

import json
import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from .models import EventType, PipelineEvent


_DEFAULT_LOG_DIR = Path("logs")


class PipelineRun:
    """
    Collects structured events for one file's processing run.
    Thread-safe for sequential pipelines; not designed for concurrent stage execution.
    """

    def __init__(self, source_file: str, log_dir: str | Path = _DEFAULT_LOG_DIR):
        self.run_id = uuid.uuid4().hex[:8]
        self.source_file = source_file
        self.log_dir = Path(log_dir)
        self._events: list[PipelineEvent] = []
        self._start_ms: float = 0.0

    # ── Context manager ──────────────────────────────────────────────────────

    def __enter__(self) -> "PipelineRun":
        self._start_ms = time.monotonic() * 1000
        self.log(EventType.PIPELINE_STARTED, stage="pipeline", data={})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        elapsed = time.monotonic() * 1000 - self._start_ms
        if exc_type is None:
            self.log(
                EventType.PIPELINE_COMPLETED,
                stage="pipeline",
                duration_ms=elapsed,
                data={"duration_ms": elapsed, "event_count": len(self._events)},
            )
        else:
            self.log(
                EventType.PIPELINE_FAILED,
                stage="pipeline",
                duration_ms=elapsed,
                data={
                    "error": str(exc_val),
                    "traceback": traceback.format_exc(),
                },
            )
        self._flush()
        return False  # re-raise exceptions

    # ── Core logging ─────────────────────────────────────────────────────────

    def log(
        self,
        event_type: EventType,
        stage: str,
        data: dict[str, Any],
        duration_ms: float | None = None,
    ) -> PipelineEvent:
        event = PipelineEvent(
            run_id=self.run_id,
            event_type=event_type,
            source_file=self.source_file,
            stage=stage,
            duration_ms=duration_ms,
            data=data,
        )
        self._events.append(event)
        self._print(event)
        return event

    # ── Convenience: timed op context manager ────────────────────────────────

    @contextmanager
    def op(
        self,
        op_name: str,
        params: dict[str, Any],
        input_rows: int,
    ) -> Generator[None, None, None]:
        """
        Context manager for a single pre-processing op.
        Automatically logs OP_STARTED, OP_COMPLETED, or OP_FAILED.

        Example:
            with run.op("split_by_discriminator", {"column": "record_type"}, input_rows=len(df)):
                frames = split_by_discriminator(df, column="record_type")
            # caller must call run.log(OP_COMPLETED, ...) with output_rows after
        """
        stage = f"op:{op_name}"
        self.log(
            EventType.OP_STARTED,
            stage=stage,
            data={"op": op_name, "params": params, "input_rows": input_rows},
        )
        t0 = time.monotonic() * 1000
        try:
            yield
        except Exception as exc:
            elapsed = time.monotonic() * 1000 - t0
            self.log(
                EventType.OP_FAILED,
                stage=stage,
                duration_ms=elapsed,
                data={"op": op_name, "params": params, "input_rows": input_rows, "error": str(exc)},
            )
            raise
        # OP_COMPLETED is emitted by the caller (it has access to output_rows/frames)

    def op_completed(
        self,
        op_name: str,
        params: dict[str, Any],
        input_rows: int,
        output_rows: int,
        output_frames: list[str],
        duration_ms: float,
    ) -> None:
        self.log(
            EventType.OP_COMPLETED,
            stage=f"op:{op_name}",
            duration_ms=duration_ms,
            data={
                "op": op_name,
                "params": params,
                "input_rows": input_rows,
                "output_rows": output_rows,
                "row_delta": output_rows - input_rows,
                "output_frames": output_frames,
            },
        )

    # ── Column mapping helpers ────────────────────────────────────────────────

    def column_resolved(
        self,
        source_column: str,
        target_column: str,
        method: str,
        confidence: float | None = None,
        transformation_note: str | None = None,
    ) -> None:
        self.log(
            EventType.COLUMN_RESOLVED,
            stage="mapping",
            data={
                "source_column": source_column,
                "target_column": target_column,
                "method": method,
                "confidence": confidence,
                "transformation_note": transformation_note,
            },
        )

    def column_unmapped(self, source_column: str, reason: str) -> None:
        self.log(
            EventType.COLUMN_UNMAPPED,
            stage="mapping",
            data={"source_column": source_column, "reason": reason},
        )

    # ── Query / export ───────────────────────────────────────────────────────

    def events_of_type(self, event_type: EventType) -> list[PipelineEvent]:
        return [e for e in self._events if e.event_type == event_type]

    def summary(self) -> dict[str, Any]:
        """Return a human-readable summary dict — useful for tests and CLI output."""
        ops = self.events_of_type(EventType.OP_COMPLETED)
        resolved = self.events_of_type(EventType.COLUMN_RESOLVED)
        unmapped = self.events_of_type(EventType.COLUMN_UNMAPPED)
        quality = self.events_of_type(EventType.QUALITY_ISSUE)
        routing = self.events_of_type(EventType.ROUTING_COMPLETED)

        method_counts: dict[str, int] = {}
        for e in resolved:
            m = e.data.get("method", "unknown")
            method_counts[m] = method_counts.get(m, 0) + 1

        return {
            "run_id": self.run_id,
            "source_file": self.source_file,
            "total_events": len(self._events),
            "ops_executed": [e.data.get("op") for e in ops],
            "row_deltas": {
                e.data.get("op"): e.data.get("row_delta") for e in ops
            },
            "columns_mapped": len(resolved),
            "columns_unmapped": len(unmapped),
            "mapping_methods": method_counts,
            "quality_issues": len(quality),
            "quality_errors": sum(
                1 for e in quality if e.data.get("severity") == "error"
            ),
            "tables_written": [
                r.data.get("results") for r in routing
            ],
        }

    # ── Internal ─────────────────────────────────────────────────────────────

    def _flush(self) -> None:
        """Write all events to a JSONL file."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        stem = Path(self.source_file).stem.replace(" ", "_")
        path = self.log_dir / f"{ts}_{self.run_id}_{stem}.jsonl"

        with open(path, "w", encoding="utf-8") as f:
            for event in self._events:
                f.write(event.model_dump_json() + "\n")

        print(f"[observability] Run log → {path}  ({len(self._events)} events)")

    def _print(self, event: PipelineEvent) -> None:
        """Minimal structured stdout line for live monitoring."""
        ts = event.timestamp.strftime("%H:%M:%S")
        dur = f" {event.duration_ms:.0f}ms" if event.duration_ms is not None else ""
        # Summarise the data payload in one short line
        summary = _short_summary(event)
        print(f"[{ts}] {event.run_id} | {event.event_type.value:<30} | {event.stage:<35} |{dur} {summary}")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _short_summary(event: PipelineEvent) -> str:
    d = event.data
    et = event.event_type
    if et == EventType.FILE_INSPECTED:
        p = d.get("profile", {})
        return f"delimiter={p.get('delimiter')!r} anomalies={d.get('anomalies', [])}"
    if et == EventType.PLAN_GENERATED:
        steps = d.get("plan", {}).get("steps", [])
        ops = [s.get("op") for s in steps]
        return f"steps={ops}"
    if et in (EventType.OP_STARTED, EventType.OP_COMPLETED, EventType.OP_FAILED):
        delta = d.get("row_delta")
        delta_str = f" Δrows={delta:+d}" if delta is not None else ""
        return f"op={d.get('op')!r} in={d.get('input_rows')} out={d.get('output_rows')}{delta_str}"
    if et == EventType.COLUMN_RESOLVED:
        return f"{d.get('source_column')!r} → {d.get('target_column')!r} via {d.get('method')}"
    if et == EventType.COLUMN_UNMAPPED:
        return f"{d.get('source_column')!r}: {d.get('reason')}"
    if et == EventType.QUALITY_ISSUE:
        return f"[{d.get('severity')}] row={d.get('row_index')} col={d.get('column')!r} {d.get('issue_type')}"
    if et == EventType.ROUTING_COMPLETED:
        return str(d.get("results", []))
    if et == EventType.PIPELINE_FAILED:
        return f"ERROR: {d.get('error')}"
    return json.dumps(d, default=str)[:120]
