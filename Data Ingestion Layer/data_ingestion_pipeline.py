import os
import argparse
import json

from csv_reader import CSVReader
from pdf_parser import PyMuPDFParser
from unstructured_data_parser import UnstructuredDataParser

class DataIngestionPipeline:
    """
    Coordinates the ingestion of CSV, PDF, and Markdown files, processing them
    through their respective paths to generate a unified 'Data' output for
    the AI Mapping Agent.
    """
    def __init__(self):
        self.csv_reader = CSVReader()
        self.pdf_parser = PyMuPDFParser()
        self.unstructured_parser = UnstructuredDataParser()

    def process_file(self, file_path: str) -> dict:
        """
        Determines the file type and routes it through the correct processing path.
        Returns a unified 'Data' dictionary ready for downstream routing.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_path}' not found.")
            
        _, ext = os.path.splitext(file_path.lower())
        
        # 1. Structured Data Path (CSV)
        if ext == '.csv':
            print(f"[Pipeline] Processing CSV file: {file_path}")
            # The CSV Reader outputs processed 'Data' (list of dicts)
            csv_data = self.csv_reader.parse_to_dict_list(file_path)
            # Package it in our unified format
            return {
                "source_type": "csv",
                "content": csv_data
            }
            
        # 2. Unstructured Data Path (PDF)
        elif ext == '.pdf':
            print(f"[Pipeline] Processing PDF file: {file_path}")
            # Process 1: The PDF Parser converts the document into "Markdown" format.
            markdown_content = self.pdf_parser.parse_to_markdown(file_path)
            
            # Process 2: The generated Markdown is passed into the Unstructured Data Parser.
            # Final Output: The Unstructured Data Parser outputs processed "Data".
            return self.unstructured_parser.parse_markdown_string(markdown_content)
            
        # 3. Unstructured Data Path (Direct Markdown Files)
        elif ext == '.md':
            print(f"[Pipeline] Processing Markdown file: {file_path}")
            # Process: The file bypasses the PDF Parser and is ingested directly.
            # Output: The Unstructured Data Parser outputs processed "Data".
            return self.unstructured_parser.parse_markdown_file(file_path)
            
        else:
            raise ValueError(f"Unsupported file type '{ext}' for file {file_path}")

class AIMappingAgent:
    """
    The external component receiving the consolidated "Data" streams 
    from the Data Ingestion Layer.
    """
    def __init__(self):
        pass

    def process(self, unified_data: dict):
        """
        Receives the unified data from the ingestion layer and performs
        mapping/processing depending on the source type.
        """
        source_type = unified_data.get("source_type")
        content = unified_data.get("content")
        
        print("\n--- [AI Mapping Agent] Processing Data ---")
        
        if source_type == "csv":
            print("Received Structured Data (CSV).")
            # TODO: Add logic to process/map CSV dictionary lists to target ontology
            print(f"Number of rows to map: {len(content)}")
            print("[Placeholder] Mapping tabular data...\n")
            
        elif source_type == "markdown":
            print("Received Unstructured Data (Markdown).")
            # TODO: Add logic to use LLM to extract entities/relationships from markdown text
            print(f"Text length to process: {len(content)} characters")
            print("[Placeholder] Invoking LLM for NLP mapping...\n")
            
        else:
            print(f"Unknown data type: {source_type}")

# ==========================================
# Example Usage (Downstream Routing)
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full Data Ingestion Pipeline")
    parser.add_argument("file_path", type=str, help="Path to the CSV, PDF, or MD file to process.")
    
    args = parser.parse_args()
    
    pipeline = DataIngestionPipeline()
    ai_agent = AIMappingAgent()
    
    try:
        # 1. The pipeline processes the raw input into unified 'Data'
        unified_data_output = pipeline.process_file(args.file_path)
        
        # 2. The combined data stream is routed upwards to the AI Mapping Agent
        ai_agent.process(unified_data_output)
        
    except Exception as e:
        print(f"Pipeline Execution Failed: {e}")
