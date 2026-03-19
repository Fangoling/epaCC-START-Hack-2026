"""
Stage 2 — Pre-processing Planner.

Given a FileProfile and the registered op list, calls the LLM to produce a
PreprocessingPlan. Falls back to a heuristic plan when the LLM is unavailable.

Unknown ops requested by the LLM trigger a code-generation fallback:
  - A second LLM call writes the function body
  - exec() is called in a restricted sandbox (pandas/numpy/re only)
  - The generated function is registered for this run
"""

from __future__ import annotations

import json
import textwrap
from typing import TYPE_CHECKING

from src.ai_mapping.ollama_client import call_structured, call_ollama
from src.observability.models import EventType
from src.pipeline.models import FileProfile, PreprocessingPlan, PreprocessingStep
from src.pipeline.prompts import PLANNER_TEMPLATE

if TYPE_CHECKING:
    from src.observability import PipelineRun
    from src.pipeline.preprocessors import PreprocessorRegistry

# Globals allowed inside exec'd op code
_EXEC_GLOBALS: dict = {"__builtins__": {}}
try:
    import pandas as pd
    import numpy as np
    import re
    _EXEC_GLOBALS.update({"pd": pd, "np": np, "re": re})
except ImportError:
    pass


def generate_plan(
    profile: FileProfile,
    registry: "PreprocessorRegistry",
    run: "PipelineRun | None" = None,
) -> PreprocessingPlan:
    """
    Produce a PreprocessingPlan from a FileProfile.
    Tries LLM first; falls back to heuristic plan if LLM unavailable.
    """
    available_ops = json.dumps(registry.list_ops(), indent=2)
    profile_json = profile.model_dump_json(indent=2)

    prompt = PLANNER_TEMPLATE.format(
        file_profile=profile_json,
        available_ops=available_ops,
    )

    plan = call_structured(prompt, PreprocessingPlan)

    if plan is None:
        plan = _heuristic_plan(profile)
        if run:
            run.log(
                EventType.PLAN_GENERATED,
                stage="planning",
                data={
                    "plan": plan.model_dump(),
                    "llm_rationale": "LLM unavailable — heuristic plan used",
                    "source": "heuristic",
                },
            )
        return plan

    # Validate: reject unknown ops or attempt code generation
    plan = _validate_and_patch(plan, profile, registry, run)

    if run:
        run.log(
            EventType.PLAN_GENERATED,
            stage="planning",
            data={
                "plan": plan.model_dump(),
                "llm_rationale": plan.llm_rationale,
                "source": "llm",
            },
        )

    return plan


# ---------------------------------------------------------------------------
# Heuristic fallback plan
# ---------------------------------------------------------------------------

def _heuristic_plan(profile: FileProfile) -> PreprocessingPlan:
    """Build a safe minimal plan from the FileProfile without any LLM call."""
    steps: list[PreprocessingStep] = []

    if profile.has_base64_headers:
        steps.append(PreprocessingStep(
            op="decode_base64_headers",
            params={},
            rationale="Headers are base64-encoded — decode before any further processing",
        ))

    if profile.has_discriminator_column and profile.discriminator_column:
        steps.append(PreprocessingStep(
            op="split_by_discriminator",
            params={"column": profile.discriminator_column,
                    "values": profile.discriminator_values or None},
            rationale=f"Column {profile.discriminator_column!r} splits rows into logical sub-tables",
        ))

    # Detect SID long-format (epaAC-Data-1..4)
    headers_lower = {h.lower() for h in profile.headers_raw}
    if "sid" in headers_lower and "sid_value" in headers_lower:
        sid_col = next(h for h in profile.headers_raw if h.lower() == "sid")
        val_col = next(h for h in profile.headers_raw if h.lower() == "sid_value")
        group_cols = [h for h in profile.headers_raw
                      if h.lower() not in ("sid", "sid_value")]
        steps.append(PreprocessingStep(
            op="pivot_sid_rows",
            params={"sid_col": sid_col, "value_col": val_col,
                    "group_cols": group_cols or None},
            rationale="Long-format SID rows need to be pivoted to wide format for the target schema",
        ))

    # Strip SAP change-tracking columns
    if any(h.startswith(("ZWrt_", "ZDat_")) for h in profile.headers_raw):
        steps.append(PreprocessingStep(
            op="strip_change_tracking_columns",
            params={},
            rationale="Remove SAP ZWrt_/ZDat_ change-tracking columns not in target schema",
        ))

    for id_col in profile.id_columns:
        steps.append(PreprocessingStep(
            op="normalize_id_column",
            params={"column": id_col},
            rationale=f"Normalise ID column {id_col!r} to plain integer",
        ))

    if profile.data_category == "device_1hz" and profile.row_count > 50_000:
        time_col = next(
            (h for h in profile.headers_raw if "time" in h.lower() or "timestamp" in h.lower()),
            "timestamp",
        )
        patient_col = profile.id_columns[0] if profile.id_columns else "patient_id"
        steps.append(PreprocessingStep(
            op="aggregate_timeseries",
            params={"patient_col": patient_col, "time_col": time_col, "interval": "1h"},
            rationale="High-frequency 1Hz data needs downsampling to hourly resolution",
        ))

    return PreprocessingPlan(
        steps=steps,
        llm_rationale="Heuristic plan — LLM was not available",
    )


# ---------------------------------------------------------------------------
# Validation + code-gen for unknown ops
# ---------------------------------------------------------------------------

def _validate_and_patch(
    plan: PreprocessingPlan,
    profile: FileProfile,
    registry: "PreprocessorRegistry",
    run: "PipelineRun | None",
) -> PreprocessingPlan:
    """
    Check that every step.op exists in registry.
    For missing ops: attempt LLM code generation + exec in a restricted sandbox.
    Steps that can't be resolved are removed from the plan.
    """
    valid_steps: list[PreprocessingStep] = []

    for step in plan.steps:
        if registry.get(step.op) is not None:
            valid_steps.append(step)
            continue

        # Unknown op — try code generation
        print(f"[planner] Op {step.op!r} not in registry — attempting code generation")
        fn = _codegen_op(step.op, step.rationale, run)
        if fn is not None:
            registry.register(step.op, fn)
            valid_steps.append(step)
        else:
            print(f"[planner] Skipping unresolvable op {step.op!r}")

    return plan.model_copy(update={"steps": valid_steps})


def _codegen_op(op_name: str, rationale: str, run: "PipelineRun | None") -> object | None:
    """
    Ask the LLM to write a Python function for an unknown op.
    The function must have signature: (df, **kwargs) -> DataFrame | dict[str, DataFrame]
    Exec'd in a restricted sandbox; returns the callable or None on failure.
    """
    prompt = textwrap.dedent(f"""
        Write a Python function named `{op_name}` for use as a pandas DataFrame pre-processing op.

        Purpose: {rationale}

        Requirements:
        - Signature: def {op_name}(df, **kwargs) -> "pd.DataFrame":
        - First argument is always a pandas DataFrame named `df`
        - Return either a DataFrame or a dict[str, DataFrame]
        - Use only: pd (pandas), np (numpy), re
        - No imports — pd, np, re are already available
        - Include a one-line docstring
        - Return ONLY the function code, no explanation, no markdown fences
    """)

    code = call_ollama(prompt)
    if code is None:
        return None

    # Strip markdown fences if present
    import re as _re
    code = _re.sub(r"```(?:python)?\s*", "", code).strip()

    local_ns: dict = {}
    try:
        exec(code, _EXEC_GLOBALS, local_ns)   # nosec — restricted globals
    except Exception as exc:
        print(f"[planner] Code generation exec failed for {op_name!r}: {exc}")
        return None

    fn = local_ns.get(op_name)
    if fn is None or not callable(fn):
        print(f"[planner] Code generation did not produce callable {op_name!r}")
        return None

    if run:
        from src.observability.models import EventType
        run.log(
            EventType.PLAN_GENERATED,
            stage=f"codegen:{op_name}",
            data={"op": op_name, "generated_code": code, "rationale": rationale},
        )

    return fn
