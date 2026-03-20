"""
PDF to CSV Converter for Nursing Data.

Converts PDF nursing reports to structured CSV format using LLM extraction.
The PDF is first converted to markdown, then the LLM extracts structured
nursing report records that match the target database schema.

Target table: tbImportNursingDailyReportsData
Expected columns: case_id, report_date, shift, ward, nursing_note_free_text
"""

from __future__ import annotations

import csv
import io
import os
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from src.observability import PipelineRun


# ---------------------------------------------------------------------------
# Pydantic models for LLM output validation
# ---------------------------------------------------------------------------

class NursingReportEntry(BaseModel):
    """A single nursing report entry extracted from the PDF."""
    case_id: str = Field(description="Patient case identifier (e.g., CASE-001)")
    patient_id: str | None = Field(default=None, description="Patient ID if available")
    ward: str = Field(description="Hospital ward/department name")
    report_date: str = Field(description="Date of the report in YYYY-MM-DD format")
    shift: str = Field(description="Shift type: Early shift, Late shift, or Night shift")
    nursing_note_free_text: str = Field(description="Full nursing note text for this entry")


class NursingReportExtractionResult(BaseModel):
    """Collection of nursing report entries extracted from a PDF."""
    entries: list[NursingReportEntry] = Field(
        description="List of nursing report entries extracted from the document"
    )
    extraction_notes: str | None = Field(
        default=None,
        description="Any notes about the extraction process or data quality issues"
    )


# ---------------------------------------------------------------------------
# Prompt template for nursing data extraction
# ---------------------------------------------------------------------------

NURSING_EXTRACTION_PROMPT = """You are a medical data extraction specialist. Extract structured nursing report data from the following markdown document.

## Target Schema
Each nursing report entry should contain:
- case_id: Patient case identifier (format like CASE-001, CASE-002, etc.)
- patient_id: Patient ID if available (format like PAT-1234)
- ward: Hospital ward/department (e.g., Pulmonology, Geriatrics, Internal Medicine, Surgery)
- report_date: Date of the report in YYYY-MM-DD format
- shift: One of "Early shift" (Frühdienst), "Late shift" (Spätdienst), or "Night shift" (Nachtdienst)
- nursing_note_free_text: The complete nursing note text including observations, interventions, and evaluations

## Instructions
1. Identify each distinct nursing report entry in the document
2. Extract all fields accurately - preserve the original nursing note text
3. Normalize shift names to English: "Frühdienst" -> "Early shift", "Spätdienst" -> "Late shift", "Nachtdienst" -> "Night shift"
4. If a field is not clearly present, make a reasonable inference or leave it empty
5. Ensure dates are in YYYY-MM-DD format

## Source Document (Markdown)
{markdown_content}

## Output
Extract all nursing report entries from the document above.
"""


# ---------------------------------------------------------------------------
# PDF to CSV Converter
# ---------------------------------------------------------------------------

class PDFToCSVConverter:
    """
    Converts PDF nursing reports to CSV format using LLM extraction.
    
    Pipeline:
    1. PDF → Markdown (via PyMuPDFParser)
    2. Markdown → Structured Data (via LLM with instructor validation)
    3. Structured Data → CSV
    """
    
    def __init__(self):
        # Lazy import to avoid circular dependencies
        pass
    
    def convert(
        self,
        pdf_path: str | Path,
        output_csv_path: str | Path | None = None,
        run: "PipelineRun | None" = None,
    ) -> tuple[list[dict], str]:
        """
        Convert a PDF nursing report to structured CSV data.
        
        Args:
            pdf_path: Path to the PDF file
            output_csv_path: Optional path to write CSV output
            run: Optional PipelineRun for observability logging
            
        Returns:
            Tuple of (list of record dicts, CSV string content)
        """
        from src.data_ingestion.pdf_parser import PyMuPDFParser
        from src.ai_mapping.ollama_client import call_structured, LLMUnavailableError
        
        pdf_path = Path(pdf_path)
        
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        # Stage 1: PDF → Markdown
        if run:
            self._log_event(run, "PDF_TO_MARKDOWN_STARTED", {"pdf_path": str(pdf_path)})
        
        parser = PyMuPDFParser()
        markdown_content = parser.parse_to_markdown(str(pdf_path))
        
        if run:
            self._log_event(run, "PDF_TO_MARKDOWN_COMPLETED", {
                "markdown_length": len(markdown_content),
                "preview": markdown_content[:500]
            })
        
        # Stage 2: Markdown → Structured Data (LLM extraction)
        prompt = NURSING_EXTRACTION_PROMPT.format(markdown_content=markdown_content)
        
        try:
            result = call_structured(
                prompt=prompt,
                response_model=NursingReportExtractionResult,
                run=run,
                stage="pdf_extraction",
            )
        except LLMUnavailableError as e:
            raise RuntimeError(f"LLM extraction failed: {e}") from e
        
        # Convert to list of dicts
        records = [entry.model_dump() for entry in result.entries]
        
        if run:
            self._log_event(run, "LLM_EXTRACTION_COMPLETED", {
                "records_extracted": len(records),
                "extraction_notes": result.extraction_notes
            })
        
        # Stage 3: Convert to CSV string
        csv_content = self._records_to_csv(records)
        
        # Optionally write to file
        if output_csv_path:
            output_csv_path = Path(output_csv_path)
            output_csv_path.parent.mkdir(parents=True, exist_ok=True)
            output_csv_path.write_text(csv_content, encoding="utf-8")
            if run:
                self._log_event(run, "CSV_WRITTEN", {"output_path": str(output_csv_path)})
        
        return records, csv_content
    
    def convert_markdown(
        self,
        markdown_content: str,
        output_csv_path: str | Path | None = None,
        run: "PipelineRun | None" = None,
    ) -> tuple[list[dict], str]:
        """
        Convert markdown content (already extracted from PDF) to structured CSV.
        
        Useful when you already have markdown and don't need PDF parsing.
        """
        from src.ai_mapping.ollama_client import call_structured, LLMUnavailableError
        
        prompt = NURSING_EXTRACTION_PROMPT.format(markdown_content=markdown_content)
        
        try:
            result = call_structured(
                prompt=prompt,
                response_model=NursingReportExtractionResult,
                run=run,
                stage="markdown_extraction",
            )
        except LLMUnavailableError as e:
            raise RuntimeError(f"LLM extraction failed: {e}") from e
        
        records = [entry.model_dump() for entry in result.entries]
        csv_content = self._records_to_csv(records)
        
        if output_csv_path:
            output_csv_path = Path(output_csv_path)
            output_csv_path.parent.mkdir(parents=True, exist_ok=True)
            output_csv_path.write_text(csv_content, encoding="utf-8")
        
        return records, csv_content
    
    def _records_to_csv(self, records: list[dict]) -> str:
        """Convert list of record dicts to CSV string."""
        if not records:
            return ""
        
        output = io.StringIO()
        fieldnames = ["case_id", "patient_id", "ward", "report_date", "shift", "nursing_note_free_text"]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
        
        return output.getvalue()
    
    def _log_event(self, run: "PipelineRun", event_name: str, data: dict) -> None:
        """Log an event to the pipeline run."""
        try:
            from src.observability.models import EventType
            # Use a generic event type for custom events
            run.log(EventType.OP_STARTED, stage="pdf_converter", data={"event": event_name, **data})
        except Exception:
            pass  # Logging should not break the pipeline


# ---------------------------------------------------------------------------
# CLI for standalone usage
# ---------------------------------------------------------------------------

def main():
    import argparse
    
    arg_parser = argparse.ArgumentParser(
        description="Convert PDF nursing reports to CSV format using LLM extraction"
    )
    arg_parser.add_argument("pdf_path", type=str, help="Path to the PDF file")
    arg_parser.add_argument(
        "-o", "--output",
        type=str,
        help="Output CSV file path (default: stdout)",
        default=None
    )
    
    args = arg_parser.parse_args()
    
    converter = PDFToCSVConverter()
    
    try:
        records, csv_content = converter.convert(args.pdf_path, args.output)
        
        if args.output:
            print(f"Success! Extracted {len(records)} nursing reports to {args.output}")
        else:
            print(csv_content)
            
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
