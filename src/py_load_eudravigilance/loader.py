"""
Handles the Loading phase of the ETL process.

This module defines the database loader interface and provides concrete
implementations for different database backends (e.g., PostgreSQL).
It is responsible for all database interactions, including native bulk loading.
"""
import psycopg2
from abc import ABC, abstractmethod
from io import IOBase
from typing import Any, Dict, List
from psycopg2.extensions import connection as PgConnection


# Using Any for Connection to avoid a premature driver import in the interface.
# A concrete implementation would use its specific connection object.
Connection = Any


class LoaderInterface(ABC):
    """
    Abstract Base Class for database loaders.

    This interface defines the contract for all database-specific loaders,
    ensuring that the core ETL logic remains independent of the target
    database technology, as required by the Strategy Pattern design.
    """

    @abstractmethod
    def connect(self) -> Connection:
        """Establish a connection using the appropriate native driver."""
        raise NotImplementedError

    @abstractmethod
    def validate_schema(self, schema_definition: Dict[str, Any]) -> bool:
        """Verify the target database schema matches the expected definition."""
        raise NotImplementedError

    @abstractmethod
    def prepare_load(self, load_mode: str) -> None:
        """
        Execute pre-loading tasks.

        For example, truncate tables in 'full' mode or create temporary
        staging tables.
        """
        raise NotImplementedError

    @abstractmethod
    def bulk_load_native(
        self, data_stream: IOBase, target_table: str, columns: List[str]
    ) -> None:
        """
        Stream data from data_stream into the target_table using the
        database's native bulk load utility (e.g., COPY FROM STDIN).
        """
        raise NotImplementedError

    @abstractmethod
    def handle_upsert(
        self,
        staging_table: str,
        target_table: str,
        primary_keys: List[str],
        version_key: str,
    ) -> None:
        """
        Implement the logic for merging data from a staging table into the
        final target tables (MERGE/UPSERT).
        """
        raise NotImplementedError

    @abstractmethod
    def manage_transaction(self, action: str) -> None:
        """
        Handle transaction boundaries (e.g., 'BEGIN', 'COMMIT', 'ROLLBACK').
        """
        raise NotImplementedError


class PostgresLoader(LoaderInterface):
    """
    Concrete implementation of the LoaderInterface for PostgreSQL.

    This class uses the `psycopg2` driver to connect to the database and
    leverages the high-performance `COPY FROM STDIN` command for bulk loading.
    """

    def __init__(self, dsn: str):
        """
        Initializes the PostgresLoader with database connection details.

        Args:
            dsn: The Data Source Name string for connecting to PostgreSQL
                 (e.g., "dbname=test user=postgres password=secret").
        """
        self.dsn = dsn
        self.conn: PgConnection | None = None

    def connect(self) -> PgConnection:
        """Establishes and returns a connection to the PostgreSQL database."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.dsn)
        return self.conn

    def validate_schema(self, schema_definition: Dict[str, Any]) -> bool:
        """Placeholder for schema validation logic."""
        # For now, we assume the schema is valid.
        print("Schema validation is not yet implemented.")
        return True

    def prepare_load(self, load_mode: str) -> None:
        """Placeholder for pre-load tasks like TRUNCATE."""
        # This could be used to TRUNCATE tables in a 'full' load scenario.
        print(f"Pre-load preparation for mode '{load_mode}' is not yet implemented.")
        pass

    def bulk_load_native(
        self, data_stream: IOBase, target_table: str, columns: List[str]
    ) -> None:
        """
        Loads data from an in-memory buffer into a PostgreSQL table using COPY.

        Args:
            data_stream: A file-like object (e.g., io.StringIO) containing
                         the data in CSV format with a header.
            target_table: The name of the database table to load data into.
            columns: A list of column names (currently unused, as the CSV
                     header is expected to match the table structure).
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        # The SQL command for copy_expert. It reads from STDIN and expects
        # CSV format with a header row that maps to the table's columns.
        sql = f"COPY {target_table} FROM STDIN WITH CSV HEADER"

        with self.conn.cursor() as cursor:
            # The copy_expert method efficiently streams the data to the DB.
            cursor.copy_expert(sql, data_stream)
        self.conn.commit()
        print(f"Successfully loaded data into '{target_table}'.")

    def handle_upsert(
        self,
        staging_table: str,
        target_table: str,
        primary_keys: List[str],
        version_key: str,
    ) -> None:
        """Placeholder for MERGE/UPSERT logic."""
        print("UPSERT logic is not yet implemented.")
        pass

    def manage_transaction(self, action: str) -> None:
        """Manages the database transaction."""
        if self.conn is None:
            return

        action = action.upper()
        if action == "BEGIN":
            # psycopg2 starts a transaction automatically on the first command.
            pass
        elif action == "COMMIT":
            self.conn.commit()
        elif action == "ROLLBACK":
            self.conn.rollback()
        else:
            raise ValueError(f"Unknown transaction action: {action}")
