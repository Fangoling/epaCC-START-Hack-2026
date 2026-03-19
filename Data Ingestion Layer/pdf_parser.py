import os
import sys
import argparse
import pymupdf4llm
import fitz  # Standard PyMuPDF

class PyMuPDFParser:
    """
    Implementation of the 'PDF Parser (PyMuPDF)' component from the Data Ingestion Layer.
    Ingests a PDF file and outputs structured Markdown.
    """
    
    def __init__(self):
        # You can add initialization parameters here if needed (e.g., page limits)
        pass

    def parse_to_markdown(self, file_path: str) -> str:
        """
        Reads a PDF and converts it to a Markdown string.
        
        Args:
            file_path (str): The path to the PDF file.
            
        Returns:
            str: The extracted content in Markdown format.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} was not found.")
            
        try:
            # pymupdf4llm handles chunking, table extraction, and header formatting 
            # natively, converting the standard fitz document into clean Markdown.
            md_text = pymupdf4llm.to_markdown(file_path)
            return md_text
            
        except Exception as e:
            # Log the error appropriately based on your system's logging setup
            print(f"Error parsing PDF '{file_path}': {e}")
            raise

    def parse_with_base_fitz(self, file_path: str) -> str:
        """
        Fallback method using base PyMuPDF if you strictly want raw text 
        without the pymupdf4llm dependency. (Note: output is plain text, not true Markdown).
        """
        text_content = []
        try:
            doc = fitz.open(file_path)
            for page in doc:
                text_content.append(page.get_text("text"))
            doc.close()
            return "\n\n".join(text_content)
        except Exception as e:
            print(f"Error parsing PDF with base fitz: {e}")
            raise

def main():
    # Set up command-line argument parsing
    arg_parser = argparse.ArgumentParser(description="Parse a PDF file to Markdown using PyMuPDF4LLM.")
    arg_parser.add_argument("pdf_path", type=str, help="Path to the PDF file you want to parse.")
    arg_parser.add_argument("-o", "--output", type=str, help="Optional output file path to save the Markdown. If not provided, prints to console.", default=None)
    
    args = arg_parser.parse_args()
    
    pdf_path = args.pdf_path
    
    if not os.path.exists(pdf_path):
        print(f"Error: The file '{pdf_path}' does not exist.")
        sys.exit(1)

    parser = PyMuPDFParser()
    
    print(f"Parsing PDF: {pdf_path}...\n")
    try:
        markdown_output = parser.parse_to_markdown(pdf_path)
        
        if args.output:
            # Write to the specified output file
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(markdown_output)
            print(f"Success! Markdown saved to {args.output}")
        else:
            # Print to console
            print("--- Extracted Markdown ---")
            print(markdown_output)
            
    except Exception as e:
        print("Pipeline failed:", e)

if __name__ == "__main__":
    main()