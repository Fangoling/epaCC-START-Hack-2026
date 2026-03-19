# epaCC-START-Hack-2026

## 🚀 Getting Started

This repository contains the architecture and tools for ingesting, standardizing, and mapping clinical data across multiple formats (CSV, PDF, Markdown) into a unified PostgreSQL schema.

---

### 1. Database Setup (PostgreSQL via Docker)

The challenge originally provided a Microsoft SQL Server database schema. We have ported this exactly to **PostgreSQL**.

We have provided an automated script that will download PostgreSQL via Docker, spin up the container, create the `CaseDB` database, and build the entire schema automatically.

**Prerequisites:** Ensure you have [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running on your machine.

**Run the setup script:**
```sh
bash DB/setup_postgres_docker.sh
```

*(Optional)* To inject mock test data with broken/missing values into the database to test the dashboard, run:
```sh
bash DB/insert_mock_data_missing.sh
```

**Connection Details:**
Once the script completes, your database will be live at:
- **Host:** localhost
- **Port:** 5432
- **Database:** CaseDB
- **Username:** postgres
- **Password:** postgres

---

### 2. Interactive Dashboard (React via Docker)

We have built a "Missing Data Tool" Dashboard to interactively visualize and manually remediate orphaned records or missing data from the `CaseDB`. 

This entire UI layer runs fully containerized via Docker.

**Run the Dashboard:**
```sh
bash src/dashboard/run_dashboard_docker.sh
```

Once it builds and starts, simply open your web browser and navigate to:
👉 **http://localhost:3000**

*(To stop the dashboard later, run: `docker stop dashboard-ui`)*

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