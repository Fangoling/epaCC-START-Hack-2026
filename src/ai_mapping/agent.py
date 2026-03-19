"""
AI Mapping Agent — assembles context, fills prompt templates, calls Ollama,
and invokes DB lookup tools to decide INSERT vs UPDATE.
"""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from src.observability import PipelineRun

from src.ai_mapping.context_loader import (
    load_iid_sid_mapping,
    load_source_sample,
    load_source_sample_from_df,
    load_target_schema,
    load_unification_rules,
)
from src.ai_mapping.models import (
    CaseLookupResult,
    ColumnMappingResult,
    QualityReport,
)
from src.ai_mapping.ollama_client import call_ollama, call_structured
from src.ai_mapping.prompt_templates import (
    COLUMN_MAPPING_TEMPLATE,
    QUALITY_CHECK_TEMPLATE,
    TRANSFORMATION_SCRIPT_TEMPLATE,
)
from src.ai_mapping.tools import check_case_exists, lookup_iid_for_column


class MappingAgent:
    """
    Orchestrates context loading + prompt assembly + LLM call for one source file.

    Usage:
        agent = MappingAgent(
            source_path="data/synth_labs.csv",
            target_table="tbImportLabsData",
            db_path="output/health_data.db",   # optional, enables DB tools
        )
        mapping = agent.get_column_mapping()       # → ColumnMappingResult
        issues  = agent.get_quality_issues()       # → QualityReport
        script  = agent.get_transformation_script(mapping)
        action  = agent.check_case(case_id=135)    # → CaseLookupResult
    """

    def __init__(
        self,
        source_path: str | Path,
        target_table: str,
        db_path: str | Path | None = None,
        run: "PipelineRun | None" = None,
        source_df: "pd.DataFrame | None" = None,
    ):
        self.source_path = Path(source_path)
        self.target_table = target_table
        self.db_path = str(db_path) if db_path else None
        self._run = run

        # Load context once — reused across all calls
        self._rules = load_unification_rules()
        self._iid_sid = load_iid_sid_mapping()
        self._schema = load_target_schema(target_table)
        if source_df is not None:
            self._headers, self._sample = load_source_sample_from_df(source_df)
        else:
            self._headers, self._sample = load_source_sample(source_path)

    # ------------------------------------------------------------------
    # LLM calls (structured output via instructor + Pydantic)
    # ------------------------------------------------------------------

    def get_column_mapping(self) -> ColumnMappingResult | None:
        """
        Ask the LLM to map source columns → target columns.
        Returns a validated ColumnMappingResult or None if LLM unavailable.
        """
        from src.observability.models import EventType

        prompt = COLUMN_MAPPING_TEMPLATE.format(
            unification_rules=self._rules,
            iid_sid_mapping=self._iid_sid,
            target_table=self.target_table,
            target_schema=self._schema,
            sample_rows=self._sample,
            source_headers=self._headers,
        )
        t0 = time.monotonic() * 1000
        result = call_structured(prompt, ColumnMappingResult)
        elapsed = time.monotonic() * 1000 - t0

        if result is None:
            print(f"[MappingAgent] Column mapping failed for {self.source_path.name}")
            return None

        if self._run:
            for m in result.mappings:
                self._run.column_resolved(
                    source_column=m.source_column,
                    target_column=m.target_column or "",
                    method="llm",
                    transformation_note=m.transformation_note,
                )
            for col in result.unmapped_columns:
                self._run.column_unmapped(col, reason="llm_returned_unmapped")
            self._run.log(
                EventType.MAPPING_COMPLETED,
                stage="mapping",
                duration_ms=elapsed,
                data={
                    "mapped": len(result.mappings),
                    "unmapped": len(result.unmapped_columns),
                    "llm_calls": 1,
                },
            )
        return result

    def get_quality_issues(self) -> QualityReport | None:
        """
        Ask the LLM to identify data quality issues in the sample rows.
        Returns a validated QualityReport or None if LLM unavailable.
        """
        from src.observability.models import EventType

        prompt = QUALITY_CHECK_TEMPLATE.format(
            unification_rules=self._rules,
            sample_rows=self._sample,
        )
        t0 = time.monotonic() * 1000
        result = call_structured(prompt, QualityReport)
        elapsed = time.monotonic() * 1000 - t0

        if result is None:
            print(f"[MappingAgent] Quality check failed for {self.source_path.name}")
            return None

        if self._run:
            for issue in result.issues:
                self._run.log(
                    EventType.QUALITY_ISSUE,
                    stage="quality",
                    data={
                        "row_index": issue.row_index,
                        "column": issue.column,
                        "value": issue.original_value,
                        "issue_type": issue.issue_type,
                        "severity": issue.severity,
                        "suggestion": issue.suggestion,
                    },
                )
            self._run.log(
                EventType.QUALITY_COMPLETED,
                stage="quality",
                duration_ms=elapsed,
                data={
                    "total": result.total,
                    "errors": len(result.errors),
                    "warnings": len(result.warnings),
                },
            )
        return result

    def get_transformation_script(
        self, mapping: ColumnMappingResult
    ) -> str | None:
        """
        Ask the LLM to write a transform_row() Python function.
        Returns the function source as a string, or None on failure.
        """
        import json

        prompt = TRANSFORMATION_SCRIPT_TEMPLATE.format(
            unification_rules=self._rules,
            iid_sid_mapping=self._iid_sid,
            target_table=self.target_table,
            target_schema=self._schema,
            column_mapping=json.dumps(mapping.to_dict(), indent=2),
            sample_rows=self._sample,
        )
        raw = call_ollama(prompt)
        if raw is None:
            print(f"[MappingAgent] Transformation script failed for {self.source_path.name}")
            return None
        # Strip markdown fences if the model wrapped code in ```python ... ```
        return re.sub(r"```(?:python)?\s*", "", raw).strip()

    # ------------------------------------------------------------------
    # DB lookup tools
    # ------------------------------------------------------------------

    def check_case(self, case_id: int) -> CaseLookupResult | None:
        """
        Check whether case_id already exists in tbCaseData.
        Requires db_path to be set at construction time.
        Returns a CaseLookupResult with action=INSERT or UPDATE.
        """
        if not self.db_path:
            print("[MappingAgent] db_path not set — cannot check case existence.")
            return None

        raw = check_case_exists(case_id=case_id, db_path=self.db_path)
        return CaseLookupResult(
            case_id=case_id,
            exists=raw["exists"],
            action=raw["action"],
            existing_row_id=raw.get("row_id"),
            existing_data=raw.get("existing_data"),
        )

    def resolve_column_iid(self, column_name: str) -> str | None:
        """
        Look up the IID code for a column name using the IID-SID mapping tool.
        Useful when the LLM mapping result needs manual verification.
        """
        return lookup_iid_for_column(column_name=column_name)

    # ------------------------------------------------------------------
    # High-level pipeline: run all steps for one file
    # ------------------------------------------------------------------

    def run(self, case_ids: list[int] | None = None) -> dict:
        """
        Run the full mapping pipeline:
          1. Column mapping
          2. Quality issues
          3. Transformation script
          4. (optional) Case existence check for each case_id

        Returns a summary dict with all results.
        """
        print(f"[MappingAgent] Running pipeline for {self.source_path.name} → {self.target_table}")

        mapping = self.get_column_mapping()
        quality = self.get_quality_issues()
        script = self.get_transformation_script(mapping) if mapping else None

        case_actions: dict[int, str] = {}
        if case_ids and self.db_path:
            for cid in case_ids:
                result = self.check_case(cid)
                if result:
                    case_actions[cid] = result.action

        return {
            "source_file": self.source_path.name,
            "target_table": self.target_table,
            "column_mapping": mapping,
            "quality_report": quality,
            "transformation_script": script,
            "case_actions": case_actions,
        }

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------

    def describe_context(self) -> dict:
        return {
            "source_file": self.source_path.name,
            "target_table": self.target_table,
            "source_headers": self._headers,
            "schema_preview": self._schema[:300] + "...",
            "iid_sid_rows": self._iid_sid.count("\n"),
            "rules_chars": len(self._rules),
            "db_path": self.db_path,
        }
