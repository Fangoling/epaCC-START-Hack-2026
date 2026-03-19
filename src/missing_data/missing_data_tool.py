from db_handler import DBHandler

class MissingDataTool:
    """
    Core logic for detecting and remediating any missing data across the CaseDB.
    """
    def __init__(self):
        self.db = DBHandler()
        
        # All relevant tables we want to monitor for missing data
        self.target_tables = [
            "tbCaseData",
            "tbImportAcData",
            "tbImportLabsData",
            "tbImportIcd10Data",
            "tbImportDeviceMotionData",
            "tbImportDevice1HzMotionData",
            "tbImportMedicationInpatientData",
            "tbImportNursingDailyReportsData"
        ]

    def get_missing_records(self):
        """
        Phase 1: Detects and pulls all records across tables where ANY column is NULL.
        Returns a dictionary grouped by table name, containing the row and the specific columns that are missing.
        """
        missing_data_report = {}
        
        for table in self.target_tables:
            # Query to find records where AT LEAST ONE column is NULL
            # Since we don't know the columns ahead of time, we dynamically generate the condition
            # by fetching the column names for the table first.
            
            # Get all column names for the current table (MS SQL Server syntax)
            col_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ?;
            """
            try:
                columns = [row['column_name'] for row in self.db.fetch_all(col_query, (table,))]
            except Exception as e:
                print(f"[Error] Failed to fetch columns for {table}: {e}")
                continue
            
            if not columns:
                continue
                
            # Build a WHERE clause that checks if ANY column is NULL
            where_clauses = [f"{col} IS NULL" for col in columns]
            full_where_clause = " OR ".join(where_clauses)
            
            query = f"SELECT * FROM {table} WHERE {full_where_clause};"
            
            try:
                records = self.db.fetch_all(query)
                if records:
                    # For each record, figure out exactly WHICH columns are null
                    annotated_records = []
                    for row in records:
                        missing_columns = [k for k, v in row.items() if v is None]
                        annotated_records.append({
                            "row_data": row,
                            "missing_columns": missing_columns
                        })
                    
                    missing_data_report[table] = annotated_records
            except Exception as e:
                print(f"[Error] Failed to query {table}: {e}")
                
        return missing_data_report

    def fix_missing_record(self, table_name: str, row_id: int, column_name: str, new_value: str):
        """
        Phase 3: The Feedback Loop. Updates the database with the manually mapped value for ANY column.
        """
        # Validate table name to prevent basic SQL injection and make it case-insensitive
        valid_table = None
        for t in self.target_tables:
            if t.lower() == table_name.lower():
                valid_table = t
                break
                
        if not valid_table:
            raise ValueError(f"Invalid table name: '{table_name}'. Please check the spelling.")
            
        # Hardcoding the primary key 'coid' as it is standard across all tables
        # Also, using standard parameterization for the value, but we have to inject the column name
        query = f"UPDATE {valid_table} SET {column_name} = ? WHERE coid = ?;"
        
        try:
            rows_affected = self.db.execute_update(query, (new_value, row_id))
            if rows_affected > 0:
                print(f"[Success] Updated {valid_table} (Row ID: {row_id}). Set {column_name} = {new_value}")
                return True
            else:
                print(f"[Warning] No rows updated. Check if Row ID {row_id} exists.")
                return False
        except Exception as e:
            print(f"[Error] Failed to update record: {e}")
            return False

    def suggest_case_id(self, search_term: str):
        """
        Phase 2 Feature: Helper method to search the tbCaseData table to find the correct ID
        based on a patient's name, ID, or condition to assist the human operator.
        """
        # Note: MS SQL uses LIKE instead of ILIKE for case-insensitive matches usually
        query = """
            SELECT coid, copatientid, colastname, cofirstname, coicd, codrgname 
            FROM tbCaseData 
            WHERE CAST(copatientid AS VARCHAR) LIKE ? 
               OR colastname LIKE ? 
               OR cofirstname LIKE ?;
        """
        search_pattern = f"%{search_term}%"
        return self.db.fetch_all(query, (search_pattern, search_pattern, search_pattern))