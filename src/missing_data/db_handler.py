import os
import pyodbc

class DBHandler:
    """
    Handles connections and basic query execution for the CaseDB MS SQL Server database.
    """
    def __init__(self, host=None, port="1433", dbname="CaseDB", user="SA", password="StartHack2026!"):
        # If we are running inside Docker on Mac/Windows, we need to use host.docker.internal
        # to talk to the other Docker container running MS SQL Server.
        self.host = host or os.getenv("DB_HOST", "host.docker.internal")
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

        # Define the connection string for pyodbc
        self.connection_string = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.dbname};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
        )

    def get_connection(self):
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            # Fallback to localhost if host.docker.internal fails (e.g. running locally without docker)
            if self.host == "host.docker.internal":
                print(f"[Warning] Failed to connect to {self.host}, falling back to localhost...")
                fallback_connection_string = (
                    "DRIVER={ODBC Driver 18 for SQL Server};"
                    f"SERVER=localhost,{self.port};"
                    f"DATABASE={self.dbname};"
                    f"UID={self.user};"
                    f"PWD={self.password};"
                    "TrustServerCertificate=yes;"
                )
                return pyodbc.connect(fallback_connection_string)
            raise e

    def fetch_all(self, query, params=None):
        """Executes a SELECT query and returns all rows as dictionaries."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                
                columns = [column[0].lower() for column in cur.description]
                results = []
                for row in cur.fetchall():
                    results.append(dict(zip(columns, row)))
                return results

    def execute_update(self, query, params=None):
        """Executes an UPDATE/INSERT/DELETE query."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                conn.commit()
                return cur.rowcount
