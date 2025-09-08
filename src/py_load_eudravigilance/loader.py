"""
Handles the Loading phase of the ETL process.

This module defines the database loader interface and provides concrete
implementations for different database backends (e.g., PostgreSQL).
It is responsible for all database interactions, including native bulk loading.
"""
import psycopg2
import sqlalchemy
from abc import ABC, abstractmethod
from io import IOBase
from typing import Any, Dict, List
from psycopg2.extensions import connection as PgConnection
from sqlalchemy.dialects.postgresql import insert as pg_insert


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
    def prepare_load(self, target_table: str, load_mode: str) -> str:
        """
        Execute pre-loading tasks and return the name of the staging table.

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
        # Convert DSN string to a dict for SQLAlchemy's connect_args
        connect_args = {
            item.split("=")[0]: item.split("=")[1]
            for item in dsn.split()
            if "=" in item
        }
        self.engine = sqlalchemy.create_engine(
            "postgresql+psycopg2://", connect_args=connect_args
        )
        self.staging_table_name: str | None = None


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

    def prepare_load(self, target_table: str, load_mode: str) -> str:
        """
        Prepares the database for loading, creating a staging table for 'delta'
        loads or truncating the table for 'full' loads.

        Args:
            target_table: The name of the final destination table.
            load_mode: The loading mode ('full' or 'delta').

        Returns:
            The name of the table to load into (staging or target).
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        if load_mode == "full":
            with self.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {target_table};")
                print(f"Table '{target_table}' truncated for full load.")
            self.conn.commit()
            return target_table

        elif load_mode == "delta":
            staging_table_name = f"__staging_{target_table}"
            with self.conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")
                create_sql = (
                    f"CREATE TEMP TABLE {staging_table_name} "
                    f"(LIKE {target_table} INCLUDING ALL);"
                )
                cursor.execute(create_sql)
                print(f"Temporary staging table '{staging_table_name}' created.")
            self.conn.commit()
            return staging_table_name

        else:
            raise ValueError(f"Unknown load mode: {load_mode}")

    def bulk_load_native(
        self, data_stream: IOBase, target_table: str, columns: List[str]
    ) -> None:
        """
        Loads data from an in-memory buffer into a PostgreSQL table using COPY.

        Args:
            data_stream: A file-like object (e.g., io.StringIO) containing
                         the data in CSV format with a header.
            target_table: The name of the database table to load data into.
                          This can be a staging or a final table.
            columns: A list of column names (currently unused, as the CSV
                     header is expected to match the table structure).
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        sql = f"COPY {target_table} FROM STDIN WITH CSV HEADER"

        with self.conn.cursor() as cursor:
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
        """
        Merges data from the staging table into the target table using an
        UPSERT (INSERT ... ON CONFLICT DO UPDATE) strategy.

        Args:
            staging_table: The name of the staging table containing new data.
            target_table: The name of the final destination table.
            primary_keys: A list of column names that form the primary key.
            version_key: The column name used for version comparison to prevent
                         updating with stale data.
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        metadata = sqlalchemy.MetaData()
        target_table_obj = sqlalchemy.Table(
            target_table, metadata, autoload_with=self.engine
        )

        # The columns to update are all columns except the primary keys
        update_cols = [
            c.name for c in target_table_obj.columns if c.name not in primary_keys
        ]

        # Construct the INSERT statement from the staging table
        insert_stmt = pg_insert(target_table_obj).from_select(
            [c.name for c in target_table_obj.columns],
            sqlalchemy.text(f"SELECT * FROM {staging_table}")
        )

        # Define the ON CONFLICT behavior
        # .excluded is a special SQLAlchemy object that refers to the row
        # that was proposed for insertion.
        update_dict = {
            col: getattr(insert_stmt.excluded, col) for col in update_cols
        }

        on_conflict_stmt = insert_stmt.on_conflict_do_update(
            index_elements=primary_keys,
            set_=update_dict,
            where=getattr(target_table_obj.c, version_key) < getattr(insert_stmt.excluded, version_key)
        )

        # Compile the statement with the engine to get the correct dialect
        # and extract the parameters for psycopg2
        compiled = on_conflict_stmt.compile(
            self.engine, compile_kwargs={"literal_binds": False}
        )

        with self.conn.cursor() as cursor:
            cursor.execute(str(compiled), compiled.params)

        self.conn.commit()
        print(f"Upsert completed from '{staging_table}' to '{target_table}'.")

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
