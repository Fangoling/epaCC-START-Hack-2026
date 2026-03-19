"""
AI Mapping Agent — assembles context, fills prompt templates, calls Ollama.
"""

from __future__ import annotations

from pathlib import Path

from src.ai_mapping.context_loader import (
    load_iid_sid_mapping,
    load_source_sample,
    load_target_schema,
    load_unification_rules,
)
from src.ai_mapping.ollama_client import call_ollama_json
from src.ai_mapping.prompt_templates import (
    COLUMN_MAPPING_TEMPLATE,
    QUALITY_CHECK_TEMPLATE,
    TRANSFORMATION_SCRIPT_TEMPLATE,
)


class MappingAgent:
    """
    Orchestrates context loading + prompt assembly + LLM call for one source file.

    Usage:
        agent = MappingAgent(source_path="data/synth_labs.csv",
                             target_table="tbImportLabsData")
        column_map = agent.get_column_mapping()
        script     = agent.get_transformation_script(column_map)
        issues     = agent.get_quality_issues()
    """

    def __init__(self, source_path: str | Path, target_table: str):
        self.source_path = Path(source_path)
        self.target_table = target_table

        # Load context once — reused across all calls
        self._rules = load_unification_rules()
        self._iid_sid = load_iid_sid_mapping()
        self._schema = load_target_schema(target_table)
        self._headers, self._sample = load_source_sample(source_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_column_mapping(self) -> dict[str, str | None] | None:
        """
        Ask the LLM to map source columns → target columns.
        Returns a dict or None if the LLM is unavailable.
        """
        prompt = COLUMN_MAPPING_TEMPLATE.format(
            unification_rules=self._rules,
            iid_sid_mapping=self._iid_sid,
            target_table=self.target_table,
            target_schema=self._schema,
            sample_rows=self._sample,
            source_headers=self._headers,
        )
        result = call_ollama_json(prompt)
        if result is None:
            print(f"[MappingAgent] Column mapping failed for {self.source_path.name}")
        return result

    def get_transformation_script(
        self, column_mapping: dict[str, str | None]
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
            column_mapping=json.dumps(column_mapping, indent=2),
            sample_rows=self._sample,
        )
        # This returns raw Python code, not JSON — use call_ollama directly
        from src.ai_mapping.ollama_client import call_ollama

        raw = call_ollama(prompt)
        if raw is None:
            print(f"[MappingAgent] Transformation script failed for {self.source_path.name}")
            return None
        # Strip markdown fences if the model wrapped code in ```python ... ```
        import re
        raw = re.sub(r"```(?:python)?\s*", "", raw).strip()
        return raw

    def get_quality_issues(self) -> list[dict] | None:
        """
        Ask the LLM to identify data quality issues in the sample rows.
        Returns a list of issue dicts or None on failure.
        """
        prompt = QUALITY_CHECK_TEMPLATE.format(
            unification_rules=self._rules,
            sample_rows=self._sample,
        )
        result = call_ollama_json(prompt)
        if not isinstance(result, list):
            return None
        return result

    # ------------------------------------------------------------------
    # Context inspection helpers (useful for debugging / dashboard)
    # ------------------------------------------------------------------

    def describe_context(self) -> dict:
        """Return a summary of loaded context for logging/debugging."""
        return {
            "source_file": self.source_path.name,
            "target_table": self.target_table,
            "source_headers": self._headers,
            "schema_preview": self._schema[:300] + "...",
            "iid_sid_rows": self._iid_sid.count("\n"),
            "rules_chars": len(self._rules),
        }
