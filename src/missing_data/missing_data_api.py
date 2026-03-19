import datetime
import decimal
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from missing_data_tool import MissingDataTool
import uvicorn

app = FastAPI(title="Missing Data Tool API")

# Enable CORS so the React frontend (running on a different port) can communicate with this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize the core logic tool
tool = MissingDataTool()

# Maps DB table names to frontend table keys
TABLE_TO_KEY = {
    "tbImportAcData": "acData",
    "tbImportLabsData": "labsData",
    "tbImportIcd10Data": "icd10Data",
    "tbImportDeviceMotionData": "deviceMotion",
    "tbImportDevice1HzMotionData": "device1HzMotion",
    "tbImportMedicationInpatientData": "medication",
    "tbImportNursingDailyReportsData": "nursingReports",
}
LINKED_TABLES = list(TABLE_TO_KEY.keys())


# --- Serialization helpers ---

def _serialize_value(v: Any) -> Any:
    """Convert types that are not JSON-serializable to primitives."""
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def serialize_row(row: dict) -> dict:
    return {k: _serialize_value(v) for k, v in row.items()}


# --- Pydantic Models ---

class FixRecordRequest(BaseModel):
    table: str
    row_id: int
    column_name: str
    new_value: str


# --- Endpoints ---

@app.get("/api/missing-data")
def get_missing_data():
    """
    Fetches all records with missing data across the database.
    Also enriches each entry with a patientId for frontend filtering.
    """
    try:
        raw_missing_data = tool.get_missing_records()

        # Build a case_id -> patient_id lookup from tbCaseData
        case_patient_map: dict[int, int] = {}
        try:
            rows = tool.db.fetch_all("SELECT coid, copatientid FROM tbCaseData WHERE copatientid IS NOT NULL")
            for row in rows:
                case_patient_map[row["coid"]] = row["copatientid"]
        except Exception:
            pass

        total_cases = 0
        try:
            total_cases_result = tool.db.fetch_all("SELECT COUNT(*) as count FROM tbCaseData;")
            if total_cases_result:
                total_cases = total_cases_result[0]["count"] * 100
        except Exception:
            total_cases = 1050

        formatted_entries = []
        entry_id_counter = 1

        for table_name, records in raw_missing_data.items():
            for record_obj in records:
                row = record_obj["row_data"]
                missing_cols = record_obj["missing_columns"]
                context = {k: v for k, v in row.items() if v is not None}

                # Determine patient ID
                patient_id = row.get("copatientid")
                if patient_id is None and "cocaseid" in row:
                    patient_id = case_patient_map.get(row["cocaseid"])

                formatted_entries.append({
                    "id": f"err-{entry_id_counter}",
                    "table": table_name,
                    "row_id": row["coid"],
                    "missing_columns": missing_cols,
                    "context": context,
                    "patient_id": patient_id,
                })
                entry_id_counter += 1

        return {
            "totalCases": total_cases,
            "brokenEntries": formatted_entries,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch missing data: {e}")


@app.post("/api/missing-data/fix")
def fix_missing_data(request: FixRecordRequest):
    """Receives a correction from the frontend and applies it to the database."""
    try:
        success = tool.fix_missing_record(
            table_name=request.table,
            row_id=request.row_id,
            column_name=request.column_name,
            new_value=request.new_value,
        )
        if success:
            return {"status": "success", "message": f"Successfully updated {request.column_name}."}
        else:
            raise HTTPException(status_code=400, detail="Update failed. Check row ID or table name.")
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to apply the fix to the database.")


@app.get("/api/patients")
def get_patients():
    """
    Returns all unique patients from tbCaseData with summary info.
    Groups by coPatientId and includes the latest case's admission/discharge info.
    """
    try:
        query = """
            SELECT
                coPatientId,
                MAX(coLastname)    AS coLastname,
                MAX(coFirstname)   AS coFirstname,
                MAX(coGender)      AS coGender,
                MAX(coDateOfBirth) AS coDateOfBirth,
                MAX(coAgeYears)    AS coAgeYears,
                MAX(coIcd)         AS coIcd,
                MAX(coState)       AS coState,
                MAX(coE2I223)      AS latestAdmission,
                MAX(coE2I228)      AS latestDischarge,
                COUNT(*)           AS caseCount
            FROM tbCaseData
            WHERE coPatientId IS NOT NULL
            GROUP BY coPatientId
            ORDER BY MAX(coLastname), MAX(coFirstname)
        """
        rows = tool.db.fetch_all_preserve_case(query)

        patients = []
        for row in rows:
            s = serialize_row(row)
            s["displayId"] = f"P-{s['coPatientId']}"
            patients.append(s)

        return patients

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch patients: {e}")


@app.get("/api/cases/{patient_id}")
def get_cases_for_patient(patient_id: int):
    """
    Returns all case records for a given patient, including linked table data.
    Response structure matches the CaseRecord TypeScript interface.
    """
    try:
        # Fetch all cases for this patient
        case_rows = tool.db.fetch_all_preserve_case(
            "SELECT * FROM tbCaseData WHERE coPatientId = ?", (patient_id,)
        )

        case_records = []
        for case_row in case_rows:
            case_data = serialize_row(case_row)
            case_id = case_row["coId"]

            tables: dict[str, list] = {}
            for db_table, frontend_key in TABLE_TO_KEY.items():
                try:
                    linked_rows = tool.db.fetch_all_preserve_case(
                        f"SELECT * FROM {db_table} WHERE coCaseId = ?", (case_id,)
                    )
                    if linked_rows:
                        tables[frontend_key] = [serialize_row(r) for r in linked_rows]
                except Exception:
                    pass

            case_records.append({
                "caseData": case_data,
                "tables": tables,
            })

        return case_records

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch cases for patient {patient_id}: {e}")


@app.get("/api/quality-metrics")
def get_quality_metrics():
    """
    Returns data quality metrics per table: total records and records with any NULL column.
    """
    try:
        all_tables = tool.target_tables  # includes tbCaseData
        by_table: dict[str, dict] = {}
        total_all = 0
        missing_all = 0

        for table in all_tables:
            # Get column names
            try:
                col_rows = tool.db.fetch_all(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?",
                    (table,),
                )
                columns = [r["column_name"] for r in col_rows]
            except Exception:
                continue

            if not columns:
                continue

            # Total rows
            try:
                total_rows = tool.db.fetch_all(f"SELECT COUNT(*) AS cnt FROM {table}")[0]["cnt"]
            except Exception:
                total_rows = 0

            # Rows with any NULL
            try:
                where = " OR ".join([f"{col} IS NULL" for col in columns])
                missing_rows = tool.db.fetch_all(
                    f"SELECT COUNT(*) AS cnt FROM {table} WHERE {where}"
                )[0]["cnt"]
            except Exception:
                missing_rows = 0

            completeness = round((1 - missing_rows / total_rows) * 100) if total_rows > 0 else 100
            frontend_key = TABLE_TO_KEY.get(table, table)

            by_table[frontend_key] = {
                "tableName": table,
                "total": total_rows,
                "missing": missing_rows,
                "completeness": completeness,
                "missingPct": round((missing_rows / total_rows) * 100) if total_rows > 0 else 0,
            }

            total_all += total_rows
            missing_all += missing_rows

        overall_completeness = round((1 - missing_all / total_all) * 100) if total_all > 0 else 100

        return {
            "overallCompleteness": overall_completeness,
            "totalRecords": total_all,
            "missingRecords": missing_all,
            "byTable": by_table,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch quality metrics: {e}")


if __name__ == "__main__":
    print("Starting FastAPI server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
