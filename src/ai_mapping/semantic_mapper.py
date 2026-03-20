"""
Semantic Column Mapper - Uses LLM to dynamically map source columns to DDL columns.

This replaces hardcoded column mappings with intelligent semantic matching.
Results are cached to avoid repeated LLM calls for the same column names.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Any, TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from src.observability import PipelineRun

logger = logging.getLogger(__name__)


# Cache file for storing learned mappings
_CACHE_FILE = Path(__file__).parent.parent.parent / "output" / "column_mapping_cache.json"


class ColumnMapping(BaseModel):
    """Response model for column mapping."""
    mappings: dict[str, str | None] = Field(
        description="Dictionary mapping source column names to DDL column names. Use null if no match."
    )
    confidence: dict[str, float] = Field(
        default_factory=dict,
        description="Confidence score (0-1) for each mapping"
    )


class SemanticColumnMapper:
    """
    Maps source CSV columns to DDL columns using semantic similarity via LLM.
    
    Features:
    - Caches mappings to avoid repeated LLM calls
    - Batch processes columns for efficiency
    - Falls back to exact/fuzzy matching for common patterns
    """
    
    # Quick mappings that don't need LLM (exact matches and common aliases)
    _QUICK_MAPPINGS: dict[str, str] = {
        # Case ID related
        "case_id": "coE2I222",
        "caseid": "coE2I222",
        "fallid": "coE2I222",
        "fall_id": "coE2I222",
        "encounter_id": "coE2I222",
        "patfal": "coE2I222",
        "fallnr": "coE2I222",
        # Patient ID
        "patient_id": "coPatientId",
        "patientid": "coPatientId",
        "pid": "coPatientId",
        # Demographics
        "sex": "coGender",
        "gender": "coGender",
        "age": "coAgeYears",
        "age_years": "coAgeYears",
        "date_of_birth": "coDateOfBirth",
        "dob": "coDateOfBirth",
        "patgeb": "coDateOfBirth",
        "lastname": "coLastname",
        "last_name": "coLastname",
        "firstname": "coFirstname",
        "first_name": "coFirstname",
        # Dates
        "admission_date": "coE2I223",
        "admission_datetime": "coE2I223",
        "aufnahme": "coE2I223",
        "patadt": "coE2I223",
        "discharge_date": "coE2I228",
        "discharge_datetime": "coE2I228",
        "entlassund": "coE2I228",
        # Location/Ward
        "ward": "coState",
        "station": "coState",
        "state": "coState",
        "patdoe": "coState",
        "patfoe": "coTypeOfStay",
        "type_of_stay": "coTypeOfStay",
        # Clinical
        "icd": "coIcd",
        "icd_code": "coIcd",
        "primary_icd10_code": "coIcd",
        "drg": "coDrgName",
        "drg_name": "coDrgName",
        "primary_icd10_description_en": "coDrgName",
    }
    
    # Category keywords for grouping DDL columns in prompts (immutable constant)
    _CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
        "identifiers": ("id", "e2i222"),
        "demographics": ("gender", "age", "name", "birth"),
        "dates": ("date", "e2i223", "e2i228"),
        "clinical": ("icd", "drg", "state", "stay", "type"),
    }

    def __init__(self) -> None:
        self._cache: dict[str, str | None] = {}
        self._cache_lock = threading.Lock()
        self._load_cache()
    
    def _load_cache(self) -> None:
        """Load cached mappings from disk."""
        if _CACHE_FILE.exists():
            try:
                with open(_CACHE_FILE) as f:
                    self._cache = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._cache = {}

    def _save_cache(self, snapshot: dict[str, str | None] | None = None) -> None:
        """Save cached mappings to disk.

        Args:
            snapshot: Pre-copied cache dict to write. If None, copies self._cache
                      (caller must NOT hold _cache_lock when snapshot is None).
        """
        if snapshot is None:
            snapshot = dict(self._cache)
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_FILE, "w") as f:
            json.dump(snapshot, f, indent=2)
    
    def _cache_key(self, source_col: str, ddl_columns: list[str]) -> str:
        """Generate a cache key for a source column + available DDL columns."""
        ddl_hash = hashlib.sha256(",".join(sorted(ddl_columns)).encode()).hexdigest()[:8]
        return f"{source_col.lower()}:{ddl_hash}"
    
    def map_column(
        self,
        source_col: str,
        ddl_columns: list[str],
        sample_values: list[str] | None = None,
        run: "PipelineRun | None" = None,
    ) -> str | None:
        """
        Map a single source column to a DDL column.
        
        Args:
            source_col: The source column name to map
            ddl_columns: Available DDL column names in the target table
            sample_values: Optional sample values from the column (for context)
            run: Optional pipeline run for logging
            
        Returns:
            The matched DDL column name, or None if no match
        """
        # 1. Try quick exact/alias mapping
        quick_match = self._QUICK_MAPPINGS.get(source_col.lower())
        if quick_match and quick_match in ddl_columns:
            return quick_match
        
        # 2. Check cache
        cache_key = self._cache_key(source_col, ddl_columns)
        with self._cache_lock:
            if cache_key in self._cache:
                cached = self._cache[cache_key]
                # Verify cached value is still valid
                if cached is None or cached in ddl_columns:
                    return cached

        # 3. Use LLM for semantic matching
        try:
            result = self._llm_map_columns([source_col], ddl_columns,
                                           {source_col: sample_values} if sample_values else None,
                                           run)
            mapped = result.get(source_col)

            # Cache the result — copy under lock, write outside lock
            with self._cache_lock:
                self._cache[cache_key] = mapped
                snapshot = dict(self._cache)
            self._save_cache(snapshot)

            return mapped
        except Exception as e:
            if run:
                from src.observability.models import EventType
                run.log(EventType.OP_FAILED, stage="semantic_mapping",
                       data={"error": str(e), "source_col": source_col})
            return None
    
    def map_columns_batch(
        self,
        source_cols: list[str],
        ddl_columns: list[str],
        sample_values: dict[str, list[str]] | None = None,
        run: "PipelineRun | None" = None,
    ) -> dict[str, str | None]:
        """
        Map multiple source columns to DDL columns in a single LLM call.
        
        This is more efficient than calling map_column() for each column.
        """
        result: dict[str, str | None] = {}
        cols_to_query: list[str] = []
        
        for col in source_cols:
            # 1. Try quick exact/alias mapping
            quick_match = self._QUICK_MAPPINGS.get(col.lower())
            if quick_match and quick_match in ddl_columns:
                result[col] = quick_match
                continue
            
            # 2. Check cache
            cache_key = self._cache_key(col, ddl_columns)
            with self._cache_lock:
                if cache_key in self._cache:
                    cached = self._cache[cache_key]
                    if cached is None or cached in ddl_columns:
                        result[col] = cached
                        continue

            # Need LLM for this column
            cols_to_query.append(col)

        # 3. Batch LLM call for uncached columns
        if cols_to_query:
            try:
                llm_results = self._llm_map_columns(
                    cols_to_query,
                    ddl_columns,
                    {k: v for k, v in (sample_values or {}).items() if k in cols_to_query},
                    run
                )

                with self._cache_lock:
                    for col in cols_to_query:
                        mapped = llm_results.get(col)
                        result[col] = mapped
                        cache_key = self._cache_key(col, ddl_columns)
                        self._cache[cache_key] = mapped
                    snapshot = dict(self._cache)
                self._save_cache(snapshot)
            except Exception as e:
                if run:
                    from src.observability.models import EventType
                    run.log(EventType.OP_FAILED, stage="semantic_mapping",
                           data={"error": str(e), "cols_to_query": cols_to_query})
                # Mark all as unmapped
                for col in cols_to_query:
                    result[col] = None
        
        return result
    
    def _llm_map_columns(
        self,
        source_cols: list[str],
        ddl_columns: list[str],
        sample_values: dict[str, list[str]] | None = None,
        run: "PipelineRun | None" = None,
    ) -> dict[str, str | None]:
        """Call LLM to map source columns to DDL columns."""
        from src.ai_mapping.ollama_client import call_structured
        
        # Build the prompt
        prompt = self._build_mapping_prompt(source_cols, ddl_columns, sample_values)
        
        # Call LLM
        response = call_structured(
            prompt=prompt,
            response_model=ColumnMapping,
            run=run,
            stage="semantic_column_mapping",
        )
        
        # Validate and return
        result: dict[str, str | None] = {}
        for col in source_cols:
            mapped = response.mappings.get(col)
            # Ensure mapped column exists in DDL
            if mapped and mapped in ddl_columns:
                result[col] = mapped
            else:
                result[col] = None
        
        return result
    
    def _build_mapping_prompt(
        self,
        source_cols: list[str],
        ddl_columns: list[str],
        sample_values: dict[str, list[str]] | None = None,
    ) -> str:
        """Build the prompt for column mapping."""
        
        # Group DDL columns by category for better context
        seen: set[str] = set()
        ddl_by_category: dict[str, list[str]] = {}
        for cat_name, keywords in self._CATEGORY_KEYWORDS.items():
            matched = [c for c in ddl_columns if any(x in c.lower() for x in keywords)]
            ddl_by_category[cat_name] = matched
            seen.update(matched)
        ddl_by_category["other"] = [c for c in ddl_columns if c not in seen]
        
        prompt = f"""You are a healthcare data integration expert. Map source CSV columns to target database (DDL) columns.

## Target DDL Columns (tbCaseData table):
- Identifiers: {', '.join(ddl_by_category['identifiers']) or 'none'}
- Demographics: {', '.join(ddl_by_category['demographics']) or 'none'}  
- Dates: {', '.join(ddl_by_category['dates']) or 'none'}
- Clinical: {', '.join(ddl_by_category['clinical']) or 'none'}
- Other: {', '.join(ddl_by_category['other'][:20]) or 'none'}

## DDL Column Naming Convention:
- coE2I222 = Case/Encounter ID
- coE2I223 = Admission date
- coE2I228 = Discharge date
- coPatientId = Patient identifier
- coGender = Sex/Gender
- coAgeYears = Age in years
- coDateOfBirth = Date of birth
- coLastname, coFirstname = Patient name
- coIcd = ICD diagnosis code
- coDrgName = DRG or diagnosis description
- coState = Ward/Station/Location
- coTypeOfStay = Type of stay/care setting

## Source Columns to Map:
"""
        for col in source_cols:
            prompt += f"\n- {col}"
            if sample_values and col in sample_values:
                samples = sample_values[col][:3]
                prompt += f" (samples: {samples})"
        
        prompt += """

## Instructions:
1. For each source column, find the BEST matching DDL column based on semantic meaning
2. Consider column names in multiple languages (English, German)
3. Use sample values to infer the data type and meaning
4. If no good match exists, map to null
5. Only map to columns that exist in the DDL list above

Return a JSON mapping of source column names to DDL column names (or null if no match).
"""
        
        return prompt


# Global singleton instance (thread-safe)
_mapper: SemanticColumnMapper | None = None
_mapper_lock = threading.Lock()


def get_semantic_mapper() -> SemanticColumnMapper:
    """Get the global semantic mapper instance (thread-safe)."""
    global _mapper
    if _mapper is None:
        with _mapper_lock:
            if _mapper is None:
                _mapper = SemanticColumnMapper()
    return _mapper


def map_source_to_case_fields(
    row_dict: dict[str, Any],
    run: "PipelineRun | None" = None,
) -> dict[str, Any]:
    """
    Extract case fields from a source row using semantic mapping.
    
    This replaces the hardcoded _SOURCE_TO_CASE_FIELD dictionary.
    
    Args:
        row_dict: Dictionary of source column names to values
        run: Optional pipeline run for logging
        
    Returns:
        Dictionary of DDL column names to values for tbCaseData
    """
    # DDL columns available in tbCaseData
    CASE_DDL_COLUMNS = [
        "coE2I222", "coPatientId", "coE2I223", "coE2I228",
        "coLastname", "coFirstname", "coGender", "coDateOfBirth",
        "coAgeYears", "coTypeOfStay", "coIcd", "coDrgName",
        "coRecliningType", "coState"
    ]
    
    mapper = get_semantic_mapper()
    
    # Get source columns that have values
    source_cols = [k for k, v in row_dict.items() if v is not None and str(v).strip()]
    
    # Build sample values for context
    sample_values = {k: [str(row_dict[k])] for k in source_cols}
    
    # Map all columns in one batch
    mappings = mapper.map_columns_batch(
        source_cols=source_cols,
        ddl_columns=CASE_DDL_COLUMNS,
        sample_values=sample_values,
        run=run,
    )
    
    # Build result with mapped values
    result: dict[str, Any] = {}
    for src_col, ddl_col in mappings.items():
        if ddl_col and src_col in row_dict:
            val = row_dict[src_col]
            if val is not None and str(val).strip():
                # Don't overwrite if already set (first value wins)
                if ddl_col not in result:
                    result[ddl_col] = val
    
    return result
