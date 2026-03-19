import os
import csv
import json

class CSVReader:
    """
    Implementation of the 'CSV Reader' component from the Data Ingestion Layer.
    Ingests a CSV file and outputs structured data (e.g., JSON or dictionaries) 
    that can be processed by downstream components.
    """
    
    def __init__(self, delimiter: str = ',', encoding: str = 'utf-8'):
        self.delimiter = delimiter
        self.encoding = encoding

    def parse_to_dict_list(self, file_path: str) -> list[dict]:
        """
        Reads a CSV and converts it to a list of dictionaries, where keys are column headers.
        
        Args:
            file_path (str): The path to the CSV file.
            
        Returns:
            list[dict]: The extracted data as a list of dictionaries.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} was not found.")
            
        data = []
        try:
            with open(file_path, mode='r', encoding=self.encoding) as csv_file:
                reader = csv.DictReader(csv_file, delimiter=self.delimiter)
                for row in reader:
                    data.append(row)
            return data
            
        except Exception as e:
            print(f"Error parsing CSV '{file_path}': {e}")
            raise

    def parse_to_json(self, file_path: str) -> str:
        """
        Reads a CSV and converts it to a JSON formatted string, which can be easily
        passed to LLMs or mapping agents.
        """
        dict_data = self.parse_to_dict_list(file_path)
        return json.dumps(dict_data, indent=4)

# ==========================================
# Example Usage (How the pipeline calls it)
# ==========================================
if __name__ == "__main__":
    reader = CSVReader()
    
    # We can use the CSV file already present in your project
    sample_csv_path = "../IID-SID-ITEM.csv"
    
    if os.path.exists(sample_csv_path):
        try:
            json_output = reader.parse_to_json(sample_csv_path)
            print("--- Extracted CSV Data as JSON ---")
            # Print first 500 characters to avoid flooding the console
            print(json_output[:500] + "\n... [truncated] ...")
            
            # This JSON representation can then be fed into the Unstructured Data Parser
            # or directly mapped via your AI Mapping Agent.
            
        except Exception as e:
            print("Pipeline failed:", e)
    else:
        print(f"Sample CSV {sample_csv_path} not found. Please ensure the file is present.")
