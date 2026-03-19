import psycopg2
from psycopg2.extras import RealDictCursor

class DBHandler:
    """
    Handles connections and basic query execution for the CaseDB PostgreSQL database.
    """
    def __init__(self, host="localhost", port="5432", dbname="CaseDB", user="postgres", password="postgres"):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )

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
