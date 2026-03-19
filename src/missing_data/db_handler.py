import os
import psycopg2
from psycopg2.extras import RealDictCursor

class DBHandler:
    """
    Handles connections and basic query execution for the CaseDB PostgreSQL database.
    """
    def __init__(self, host=None, port="5432", dbname="CaseDB", user="postgres", password="postgres"):
        # If we are running inside Docker on Mac/Windows, we need to use host.docker.internal
        # to talk to the other Docker container running Postgres.
        self.host = host or os.getenv("DB_HOST", "host.docker.internal")
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    def get_connection(self):
        try:
            return psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
        except Exception as e:
            # Fallback to localhost if host.docker.internal fails (e.g. running locally without docker)
            if self.host == "host.docker.internal":
                print(f"[Warning] Failed to connect to {self.host}, falling back to localhost...")
                return psycopg2.connect(
                    host="localhost",
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password
                )
            raise e

    def fetch_all(self, query, params=None):
        """Executes a SELECT query and returns all rows as dictionaries."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def execute_update(self, query, params=None):
        """Executes an UPDATE/INSERT/DELETE query."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
                return cur.rowcount
