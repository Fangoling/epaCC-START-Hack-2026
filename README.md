# epaCC-START-Hack-2026

## 🚀 Getting Started

This repository contains the architecture and tools for ingesting, standardizing, and mapping clinical data across multiple formats (CSV, PDF, Markdown) into a unified MS SQL Server schema.

---

### 🌟 Quick Start: The Magic Button
To make testing and judging as easy as possible, we have fully containerized the entire application stack via Docker.

You can spin up the Database, the Backend API, and the Frontend Interactive Dashboard all at once by running a single script:

```sh
bash start_everything.sh
```

**What this does:**
1. Spins up a Microsoft SQL Server database and injects the schema.
2. Injects test data (with purposely missing values) to demonstrate the Missing Data Tool.
3. Starts the Python FastAPI Backend.
4. Starts the React Interactive Dashboard.

Once it completes, simply navigate to 👉 **http://localhost:3000** to use the tool!

*(To cleanly shut down the entire stack later, just run `bash stop_everything.sh`)*

---

### 1. Database Details (MS SQL Server via Docker)

We use the original Microsoft SQL Server schema provided in the challenge.

If you wish to spin up *only* the database manually:
```sh
bash DB/setup_mssql_docker.sh
bash DB/test-seeds/insert_mock_data_missing.sh
```

**Connection Details:**
- **Host:** localhost
- **Port:** 1433
- **Database:** CaseDB
- **Username:** SA
- **Password:** StartHack2026!

---

### 2. Interactive Dashboard (React via Docker)

We have built a "Missing Data Tool" Dashboard to interactively visualize and manually remediate orphaned records or missing data from the `CaseDB`. 

If you wish to run *only* the UI manually:
```sh
bash src/dashboard/run_dashboard_docker.sh
```

---

### 3. Data Ingestion Pipeline

The Data Ingestion Layer intelligently processes incoming files, structures them, and outputs unified "Data" to the AI Mapping Agent.

- **Structured Data (CSV):** Read via the CSV Reader and output as structured JSON dictionaries.
- **Unstructured Data (PDF):** Ingested via PyMuPDF (`pymupdf4llm`), converted into intermediate Markdown, and processed by the Unstructured Data Parser.
- **Unstructured Data (Markdown):** Ingested directly by the Unstructured Data Parser.

**Prerequisites:**
Install the necessary Python dependencies for PDF parsing:
```sh
pip install pymupdf pymupdf4llm
```

**Running the Pipeline:**
You can process any supported file using the single entry-point script:

```sh
# For Unstructured PDF Data
python "Data Ingestion Layer/data_ingestion_pipeline.py" "Endtestdaten_ohne_Fehler_ einheitliche ID/split_data_pat_case_altered/clinic_data/clinic_4_nursing.pdf"

# For Structured CSV Data
python "Data Ingestion Layer/data_ingestion_pipeline.py" "IID-SID-ITEM.csv"
```

Alternatively, run the automated shell script to test both flows back-to-back:
```sh
bash run_pipeline.sh
```