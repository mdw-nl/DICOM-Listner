import logging
import threading
from time import sleep

import psycopg2

from dicomsorter.src.global_var import NUMBER_ATTEMPTS, RETRY_DELAY_IN_SECONDS

logger = logging.getLogger(__name__)


class PostgresInterface:
    def __init__(self, host, database, user, password, port):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None
        self.port = port
        self._lock = threading.Lock()

    def connect(self):
        """Connect to the PostgreSQL database."""
        for attempt in range(NUMBER_ATTEMPTS):
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                self.cursor = self.conn.cursor()
                logger.info("Connection established.")
                break
            except psycopg2.OperationalError as e:
                if attempt < NUMBER_ATTEMPTS - 1:
                    logger.warning("%s", e)
                    logger.info("Retrying in %s seconds...", RETRY_DELAY_IN_SECONDS)
                    sleep(RETRY_DELAY_IN_SECONDS)
                else:
                    raise Exception("Unable to connect to the database after time.") from e

    def disconnect(self):
        """Close the connection to the database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Connection closed.")

    def execute_query(self, query, params=None):
        """Execute a query (e.g., INSERT, UPDATE, DELETE)."""
        with self._lock:
            try:
                self.cursor.execute(query, params)
                self.conn.commit()
                logger.info("Query executed successfully.")
            except psycopg2.IntegrityError as e:
                self.conn.rollback()
                if "duplicate key" in str(e).lower():
                    logger.warning("Duplicate entry ignored: %s", e)
                else:
                    logger.exception("Integrity error")
            except Exception as e:
                self.conn.rollback()
                logger.warning("Error executing query: %s", e)

    def fetch_all(self, query, params=None):
        """Fetch all results from a SELECT query."""
        with self._lock:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchall()
            except Exception as e:
                logger.warning("Error fetching results: %s", e)
                return None

    def fetch_one(self, query, params=None):
        """Fetch a single result from a SELECT query."""
        with self._lock:
            try:
                self.cursor.execute(query, params)
                return self.cursor.fetchone()
            except Exception as e:
                logger.warning("Error fetching result: %s", e)
                return None

    def create_table(self, table_name, columns):
        """Create a table."""
        columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        self.execute_query(query)

    def insert(self, table_name, data):
        """Insert a new row into a table."""
        columns = ", ".join(data)
        values = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        self.execute_query(query, tuple(data.values()))

    def update(self, table_name, data, where_conditions):
        """Update a row in a table."""
        set_clause = ", ".join([f"{col} = %s" for col in data])
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        self.execute_query(query, tuple(data.values()) + tuple(where_conditions.values()))

    def delete(self, table_name, where_conditions):
        """Delete rows from a table."""
        where_clause = " AND ".join([f"{col} = %s" for col in where_conditions])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        self.execute_query(query, tuple(where_conditions.values()))

    def check_table_exists(self, table_name):
        """Check if a table exists in the database."""
        with self._lock:
            query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
            """
            self.cursor.execute(query, (table_name,))
            return self.cursor.fetchone()[0]
