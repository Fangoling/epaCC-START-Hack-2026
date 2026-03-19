import os
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

# ==========================================
# Example Usage (How the pipeline calls it)
# ==========================================
if __name__ == "__main__":
    parser = PyMuPDFParser()
    sample_pdf_path = "sample_document.pdf" 
    
    # Create a dummy PDF for testing if it doesn't exist
    if not os.path.exists(sample_pdf_path):
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((50, 50), "Sample PDF Document\n\nThis is a test.")
        doc.save(sample_pdf_path)
    
    # Execute the parsing
    try:
        markdown_output = parser.parse_to_markdown(sample_pdf_path)
        print("--- Extracted Markdown ---")
        print(markdown_output)
        
        # In your architecture, this 'markdown_output' string is what you 
        # will pass directly to the 'Unstructured Data Parser'.
        
    except Exception as e:
        print("Pipeline failed:", e)