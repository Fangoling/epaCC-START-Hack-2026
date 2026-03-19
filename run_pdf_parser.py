from pdf_parser import PyMuPDFParser

if __name__ == "__main__":
    parser = PyMuPDFParser()
    sample_pdf_path = "/Users/jennifer/Projects/epaCC-START-Hack-2026/Endtestdaten_ohne_Fehler_ einheitliche ID/split_data_pat_case_altered/split_data_pat_case_altered/clinic_4_nursing.pdf"
    
    try:
        markdown_output = parser.parse_to_markdown(sample_pdf_path)
        print("--- Extracted Markdown ---")
        print(markdown_output[:2000] + "\n... [truncated] ...")
        
    except Exception as e:
        print("Pipeline failed:", e)
