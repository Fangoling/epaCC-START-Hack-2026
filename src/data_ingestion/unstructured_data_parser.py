import os

class UnstructuredDataParser:
    """
    Ingests Markdown content (either directly from a .md file or intermediate 
    Markdown from the PDF Parser) and processes it into a unified format
    for the downstream pipeline.
    """
    def __init__(self):
        pass

    def parse_markdown_file(self, file_path: str) -> dict:
        """
        Process 3: Reads a direct Markdown file and outputs processed Data.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found.")
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        return self.parse_markdown_string(content)

    def parse_markdown_string(self, markdown_text: str) -> dict:
        """
        Process 2: Ingests intermediate Markdown text (from PDF Parser) 
        and outputs processed Data.
        """
        # Here we package the unstructured text into a unified 'Data' dictionary 
        # that the downstream pipeline and AI Agent can easily recognize.
        return {
            "source_type": "markdown",
            "content": markdown_text
        }
