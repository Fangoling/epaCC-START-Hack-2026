#!/bin/bash

# Define paths
PIPELINE_SCRIPT="Data Ingestion Layer/data_ingestion_pipeline.py"
PDF_FILE="Endtestdaten_ohne_Fehler_ einheitliche ID/split_data_pat_case_altered/clinic_data/clinic_4_nursing.pdf"
CSV_FILE="IID-SID-ITEM.csv"

echo "------------------------------------------------"
echo "Running Pipeline on Unstructured Data (PDF)"
echo "------------------------------------------------"
python3 "$PIPELINE_SCRIPT" "$PDF_FILE"
echo ""

echo "------------------------------------------------"
echo "Running Pipeline on Structured Data (CSV)"
echo "------------------------------------------------"
python3 "$PIPELINE_SCRIPT" "$CSV_FILE"
echo ""