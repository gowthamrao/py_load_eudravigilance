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

    @abstractmethod
    def create_metadata_tables(self) -> None:
        """
        Creates the necessary metadata tables (e.g., for file history)
        in the target database if they don't already exist.
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
        Prepares the database for loading. For 'delta' mode, this is part of
        the file transaction. For 'full' mode, this is a separate, committed
        action before processing begins.
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        if load_mode == "full":
            with self.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {target_table};")
                print(f"Table '{target_table}' truncated for full load.")
            # A full load truncate should be committed immediately.
            self.conn.commit()
            return target_table

        elif load_mode == "delta":
            staging_table_name = f"__staging_{target_table}"
            with self.conn.cursor() as cursor:
                # This runs inside a transaction, so the temp table is transactional.
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")
                create_sql = (
                    f"CREATE TEMP TABLE {staging_table_name} "
                    f"(LIKE {target_table} INCLUDING ALL);"
                )
                cursor.execute(create_sql)
                print(f"Temporary staging table '{staging_table_name}' created.")
            # DO NOT COMMIT HERE - this is part of the per-file transaction
            return staging_table_name

        else:
            raise ValueError(f"Unknown load mode: {load_mode}")

    def bulk_load_native(
        self, data_stream: IOBase, target_table: str, columns: List[str]
    ) -> None:
        """
        Loads data from an in-memory buffer into a PostgreSQL table using COPY.
        This method does NOT commit the transaction.
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        sql = f"COPY {target_table} FROM STDIN WITH CSV HEADER"

        with self.conn.cursor() as cursor:
            cursor.copy_expert(sql, data_stream)
        # DO NOT COMMIT HERE
        print(f"Successfully loaded data into '{target_table}'.")

    def handle_upsert(
        self,
        staging_table: str,
        target_table: str,
        primary_keys: List[str],
        version_key: str | None,
    ) -> None:
        """
        Merges data from the staging table into the target table using raw SQL
        for robustness. This method does NOT commit the transaction.
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        # Introspect the target table to get all column names
        inspector = sqlalchemy.inspect(self.engine)
        all_columns = [col["name"] for col in inspector.get_columns(target_table)]
        update_cols = [col for col in all_columns if col not in primary_keys]

        # Construct the parts of the raw SQL query
        pk_string = ", ".join(primary_keys)
        update_statements = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

        sql = f"""
            INSERT INTO {target_table}
            SELECT * FROM {staging_table}
            ON CONFLICT ({pk_string}) DO UPDATE
            SET {update_statements}
        """

        # Add the version-checking WHERE clause if a version_key is provided
        if version_key:
            sql += f" WHERE {target_table}.{version_key} < EXCLUDED.{version_key};"
        else:
            sql += ";"

        with self.conn.cursor() as cursor:
            cursor.execute(sql)
        # DO NOT COMMIT HERE
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

    def create_metadata_tables(self) -> None:
        """
        Creates the `etl_file_history` table in the database if it does not exist.
        """
        metadata = sqlalchemy.MetaData()
        sqlalchemy.Table(
            "etl_file_history",
            metadata,
            sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("filename", sqlalchemy.String(255), nullable=False),
            sqlalchemy.Column(
                "file_hash", sqlalchemy.String(64), nullable=False, unique=True
            ),
            sqlalchemy.Column("status", sqlalchemy.String(50), nullable=False),
            sqlalchemy.Column(
                "load_timestamp",
                sqlalchemy.DateTime,
                server_default=sqlalchemy.func.now(),
            ),
            sqlalchemy.Column("rows_processed", sqlalchemy.Integer),
        )
        metadata.create_all(self.engine)
        print("Metadata tables created or already exist.")

    def _log_file_status(
        self,
        filename: str,
        file_hash: str,
        status: str,
        rows_processed: int | None = None,
    ) -> None:
        """
        Logs or updates the status of a file in the history table.
        This method does NOT commit the transaction.
        """
        if self.conn is None or self.conn.closed:
            self.connect()

        metadata = sqlalchemy.MetaData()
        history_table = sqlalchemy.Table(
            "etl_file_history", metadata, autoload_with=self.engine
        )

        stmt = pg_insert(history_table).values(
            filename=filename,
            file_hash=file_hash,
            status=status,
            rows_processed=rows_processed,
            load_timestamp=sqlalchemy.func.now(),
        )

        update_stmt = stmt.on_conflict_do_update(
            index_elements=["file_hash"],
            set_={
                "status": status,
                "rows_processed": rows_processed,
                "load_timestamp": sqlalchemy.func.now(),
            },
        )

        # Execute using the main connection to be part of the transaction
        compiled = update_stmt.compile(self.engine)
        with self.conn.cursor() as cursor:
            cursor.execute(str(compiled), compiled.params)
        # DO NOT COMMIT HERE


    def get_completed_file_hashes(self) -> set[str]:
        """
        Retrieves a set of file hashes for all files that have been
        successfully processed ('completed' status).

        Returns:
            A set of SHA-256 hash strings.
        """
        metadata = sqlalchemy.MetaData()
        history_table = sqlalchemy.Table(
            "etl_file_history", metadata, autoload_with=self.engine
        )
        query = sqlalchemy.select(history_table.c.file_hash).where(
            history_table.c.status == "completed"
        )
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return {row[0] for row in result}

    def load_normalized_data(
        self,
        buffers: Dict[str, IOBase],
        row_counts: Dict[str, int],
        load_mode: str,
        file_path: str,
        file_hash: str,
    ) -> None:
        """
        Orchestrates the loading of multiple normalized data buffers into their
        respective tables within a single transaction.
        """
        TABLE_METADATA = {
            "icsr_master": {"pk": ["safetyreportid"], "version_key": "receiptdate"},
            "patient_characteristics": {"pk": ["safetyreportid"], "version_key": None},
            "reactions": {"pk": ["safetyreportid", "reactionmeddrapt"], "version_key": None},
            "drugs": {"pk": ["safetyreportid", "medicinalproduct"], "version_key": None},
        }

        self.manage_transaction("BEGIN")
        try:
            total_rows = sum(row_counts.values())
            self._log_file_status(file_path, file_hash, "running", total_rows)

            for table_name, buffer in buffers.items():
                if row_counts.get(table_name, 0) > 0:
                    print(f"Processing table: {table_name}")
                    metadata = TABLE_METADATA.get(table_name)
                    if not metadata:
                        raise ValueError(f"No metadata defined for table {table_name}")

                    staging_table = self.prepare_load(
                        target_table=table_name, load_mode=load_mode
                    )
                    self.bulk_load_native(buffer, staging_table, columns=[])

                    if load_mode == "delta":
                        self.handle_upsert(
                            staging_table=staging_table,
                            target_table=table_name,
                            primary_keys=metadata["pk"],
                            version_key=metadata["version_key"],
                        )

            self._log_file_status(file_path, file_hash, "completed", total_rows)
            self.manage_transaction("COMMIT")

        except Exception as e:
            print(f"Error during normalized load for file {file_path}. Rolling back.")
            self.manage_transaction("ROLLBACK")
            # Log failure in a separate transaction
            self._log_file_status(file_path, file_hash, "failed")
            self.manage_transaction("COMMIT")
            raise e
