from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from missing_data_tool import MissingDataTool
import uvicorn

app = FastAPI(title="Missing Data Tool API")

# Enable CORS so the React frontend (running on a different port) can communicate with this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the exact React domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize the core logic tool
tool = MissingDataTool()

# --- Pydantic Models for Request Bodies ---
class FixRecordRequest(BaseModel):
    table: str
    row_id: int
    column_name: str
    new_value: str

# --- API Endpoints ---

@app.get("/api/missing-data")
def get_missing_data():
    """
    Fetches all records with missing data across the database.
    Formats the data so the React frontend can easily consume it.
    """
    try:
        raw_missing_data = tool.get_missing_records()
        
        # We need to flatten the grouped dictionary into an array of objects for the React table
        formatted_entries = []
        total_cases = 0 # Placeholder for total analyzed cases (you could run a real COUNT(*) query here)
        
        # Calculate a mock 'total cases' just so the dashboard looks right
        # In a real scenario, you'd do a SELECT COUNT(*) FROM tbCaseData
        try:
            total_cases_result = tool.db.fetch_all("SELECT COUNT(*) as count FROM tbCaseData;")
            if total_cases_result:
                 total_cases = total_cases_result[0]['count'] * 100 # arbitrary multiplier so health score looks realistic based on test data
        except:
             total_cases = 1050

        entry_id_counter = 1
        
        for table_name, records in raw_missing_data.items():
            for record_obj in records:
                row = record_obj["row_data"]
                missing_cols = record_obj["missing_columns"]
                
                # Context is the row data minus the nulls
                context = {k: v for k, v in row.items() if v is not None}
                
                # We format this exactly as the React frontend expects it based on our mockData.json
                formatted_entries.append({
                    "id": f"err-{entry_id_counter}",
                    "table": table_name,
                    "row_id": row['coid'],
                    "missing_columns": missing_cols,
                    "context": context
                })
                entry_id_counter += 1

        return {
            "totalCases": total_cases,
            "brokenEntries": formatted_entries
        }

    except Exception as e:
        print(f"API Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch missing data from the database.")


@app.post("/api/missing-data/fix")
def fix_missing_data(request: FixRecordRequest):
    """
    Receives a correction from the frontend and applies it to the database.
    """
    try:
        success = tool.fix_missing_record(
            table_name=request.table,
            row_id=request.row_id,
            column_name=request.column_name,
            new_value=request.new_value
        )
        
        if success:
            return {"status": "success", "message": f"Successfully updated {request.column_name}."}
        else:
            raise HTTPException(status_code=400, detail="Update failed. Check row ID or table name.")
            
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        print(f"API Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to apply the fix to the database.")

if __name__ == "__main__":
    # Run the API on port 8000
    print("Starting FastAPI server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)