"""
Data Ingestion Pipeline - Routes files through appropriate processing paths.

Handles CSV, PDF, and Markdown files, converting them to a unified format
for downstream processing by the main Pipeline orchestrator.

For PDFs (especially nursing data), uses LLM extraction to convert
unstructured content to structured CSV format.
"""

import os
import argparse
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.observability import PipelineRun


class DataIngestionPipeline:
    """
    Coordinates the ingestion of CSV, PDF, and Markdown files, processing them
    through their respective paths to generate a unified output for the
    main Pipeline orchestrator.
    
    Processing paths:
    1. CSV files → Pass through directly (structured data)
    2. PDF files → Convert to CSV via LLM extraction (unstructured → structured)
    3. Markdown files → Convert to CSV via LLM extraction (unstructured → structured)
    """
    
    def __init__(self):
        # Lazy imports to avoid circular dependencies
        pass
    
    def process_file(
        self,
        file_path: str | Path,
        output_dir: str | Path | None = None,
        run: "PipelineRun | None" = None,
    ) -> dict:
        """
        Determines the file type and routes it through the correct processing path.
        
        Args:
            file_path: Path to the input file (CSV, PDF, or MD)
            output_dir: Directory for intermediate outputs (e.g., converted CSVs)
            run: Optional PipelineRun for observability logging
            
        Returns:
            dict with:
              - source_type: "csv", "pdf", or "markdown"
              - csv_path: Path to CSV file (original or converted)
              - content: Parsed data (list of dicts for CSV, or records for PDF/MD)
              - original_path: Original input file path
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File '{file_path}' not found.")
        
        ext = file_path.suffix.lower()
        output_dir = Path(output_dir) if output_dir else Path(tempfile.gettempdir())
        
        # 1. Structured Data Path (CSV) - pass through
        if ext == '.csv':
            print(f"[DataIngestion] Processing CSV file: {file_path}")
            return self._process_csv(file_path, run)
        
        # 2. Unstructured Data Path (PDF) - convert to CSV via LLM
        elif ext == '.pdf':
            print(f"[DataIngestion] Processing PDF file: {file_path}")
            return self._process_pdf(file_path, output_dir, run)
        
        # 3. Unstructured Data Path (Markdown) - convert to CSV via LLM
        elif ext == '.md':
            print(f"[DataIngestion] Processing Markdown file: {file_path}")
            return self._process_markdown(file_path, output_dir, run)
        
        else:
            raise ValueError(f"Unsupported file type '{ext}' for file {file_path}")
    
    def _process_csv(
        self,
        file_path: Path,
        run: "PipelineRun | None" = None,
    ) -> dict:
        """Process CSV file - read and return as structured data."""
        from src.data_ingestion.csv_reader import CSVReader
        
        reader = CSVReader()
        csv_data = reader.parse_to_dict_list(str(file_path))
        
        return {
            "source_type": "csv",
            "csv_path": str(file_path),
            "content": csv_data,
            "original_path": str(file_path),
            "records_count": len(csv_data) if csv_data else 0,
        }
    
    def _process_pdf(
        self,
        file_path: Path,
        output_dir: Path,
        run: "PipelineRun | None" = None,
    ) -> dict:
        """
        Process PDF file - convert to CSV via LLM extraction.
        
        The converted CSV is saved to output_dir and can be processed
        by the main Pipeline orchestrator.
        """
        from src.data_ingestion.pdf_to_csv_converter import PDFToCSVConverter
        
        converter = PDFToCSVConverter()
        
        # Generate output CSV path
        csv_filename = file_path.stem + "_converted.csv"
        csv_path = output_dir / csv_filename
        
        # Convert PDF to CSV
        records, csv_content = converter.convert(
            pdf_path=file_path,
            output_csv_path=csv_path,
            run=run,
        )
        
        print(f"[DataIngestion] Converted PDF to CSV: {csv_path} ({len(records)} records)")
        
        return {
            "source_type": "pdf",
            "csv_path": str(csv_path),
            "content": records,
            "original_path": str(file_path),
            "records_count": len(records),
        }
    
    def _process_markdown(
        self,
        file_path: Path,
        output_dir: Path,
        run: "PipelineRun | None" = None,
    ) -> dict:
        """
        Process Markdown file - convert to CSV via LLM extraction.
        
        Assumes the markdown contains nursing report data that can be
        extracted to structured format.
        """
        from src.data_ingestion.pdf_to_csv_converter import PDFToCSVConverter
        
        converter = PDFToCSVConverter()
        
        # Read markdown content
        markdown_content = file_path.read_text(encoding="utf-8")
        
        # Generate output CSV path
        csv_filename = file_path.stem + "_converted.csv"
        csv_path = output_dir / csv_filename
        
        # Convert markdown to CSV
        records, csv_content = converter.convert_markdown(
            markdown_content=markdown_content,
            output_csv_path=csv_path,
            run=run,
        )
        
        print(f"[DataIngestion] Converted Markdown to CSV: {csv_path} ({len(records)} records)")
        
        return {
            "source_type": "markdown",
            "csv_path": str(csv_path),
            "content": records,
            "original_path": str(file_path),
            "records_count": len(records),
        }
    
    def process_to_pipeline(
        self,
        file_path: str | Path,
        output_dir: str | Path | None = None,
        run: "PipelineRun | None" = None,
    ) -> Path:
        """
        Process a file and return the path to a CSV that can be fed
        directly to the main Pipeline.run() method.
        
        This is a convenience method that abstracts away the conversion
        process - you get back a CSV path regardless of input type.
        """
        result = self.process_file(file_path, output_dir, run)
        return Path(result["csv_path"])


# ---------------------------------------------------------------------------
# Integration with main Pipeline
# ---------------------------------------------------------------------------

def run_full_pipeline(
    file_path: str | Path,
    db_path: str | Path | None = None,
    log_dir: str | Path = "logs",
    output_dir: str | Path = "output/converted",
) -> dict:
    """
    Run the complete pipeline: ingestion + transformation + database insertion.

    This function bridges DataIngestionPipeline and the main Pipeline,
    handling any file type (CSV, PDF, MD) seamlessly.

    Args:
        file_path: Input file (CSV, PDF, or Markdown)
        db_path: SQLAlchemy DB URL or file path (None = MSSQL default)
        log_dir: Directory for pipeline logs
        output_dir: Directory for converted CSV files

    Returns:
        Pipeline result dictionary with routing results and metadata
    """
    from src.pipeline.orchestrator import Pipeline
    
    file_path = Path(file_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Stage 1: Ingestion - convert to CSV if needed
    ingestion = DataIngestionPipeline()
    ingestion_result = ingestion.process_file(file_path, output_dir)
    
    csv_path = Path(ingestion_result["csv_path"])
    
    # Stage 2-4: Run main pipeline on the CSV
    pipeline = Pipeline(db_path=db_path, log_dir=log_dir)
    result = pipeline.run(csv_path)
    
    # Combine results
    result["ingestion"] = {
        "source_type": ingestion_result["source_type"],
        "original_path": ingestion_result["original_path"],
        "converted_csv_path": ingestion_result["csv_path"],
        "records_extracted": ingestion_result["records_count"],
    }
    
    return result


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the full Data Ingestion Pipeline with optional database insertion"
    )
    parser.add_argument(
        "file_path",
        type=str,
        help="Path to the CSV, PDF, or MD file to process"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="SQLAlchemy DB URL (default: MSSQL on localhost:1433/CaseDB)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output/converted",
        help="Directory for converted CSV files"
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Only run ingestion (convert to CSV), don't run full pipeline"
    )
    
    args = parser.parse_args()
    
    try:
        if args.ingest_only:
            # Just convert to CSV
            ingestion = DataIngestionPipeline()
            result = ingestion.process_file(args.file_path, args.output_dir)
            print(f"\nIngestion complete!")
            print(f"  Source type: {result['source_type']}")
            print(f"  Records: {result['records_count']}")
            print(f"  CSV path: {result['csv_path']}")
        else:
            # Run full pipeline
            result = run_full_pipeline(
                args.file_path,
                db_path=args.db_path,
                output_dir=args.output_dir,
            )
            print(f"\nPipeline complete!")
            print(f"  Source: {result.get('ingestion', {}).get('original_path')}")
            print(f"  Records inserted: {result.get('routing_result', {}).get('rows_written', 0)}")
            
    except Exception as e:
        print(f"Pipeline Execution Failed: {e}")
        raise
