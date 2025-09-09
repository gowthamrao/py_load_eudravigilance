"""
Handles the Loading phase of the ETL process.

This module defines the database loader interface and provides concrete
implementations for different database backends (e.g., PostgreSQL).
It is responsible for all database interactions, including native bulk loading.
"""
from io import IOBase
from typing import Any, Dict, List

from .interfaces import LoaderInterface
from .schema import metadata

try:
    import psycopg2
    import sqlalchemy
    from psycopg2.extensions import connection as PgConnection
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.engine.url import make_url
except ImportError:
    # This is a soft dependency, so we don't raise the error here.
    # The error will be raised by get_loader if a postgres DSN is provided.
    pass


def get_loader(dsn_or_engine: Any) -> LoaderInterface:
    """
    Factory function to get the appropriate database loader.
    Inspects the DSN string to determine which loader to instantiate.
    """
    dsn_str = str(dsn_or_engine)
    try:
        # We check the DSN string itself first.
        if "postgres" in dsn_str:
            try:
                # This will fail if the soft dependencies were not installed
                return PostgresLoader(dsn_or_engine)
            except NameError:
                raise ImportError(
                    "PostgreSQL dependencies are not installed. "
                    "Please run: pip install 'py-load-eudravigilance[postgres]'"
                )

        # If not matched by simple string search, try SQLAlchemy parsing
        url = make_url(dsn_str)
        if url.drivername.startswith("postgresql"):
            try:
                return PostgresLoader(dsn_or_engine)
            except NameError:
                 raise ImportError(
                    "PostgreSQL dependencies are not installed. "
                    "Please run: pip install 'py-load-eudravigilance[postgres]'"
                )
        # Add other database dialects here in the future
        # elif url.drivername.startswith("redshift"):
        #     return RedshiftLoader(dsn_or_engine)
        else:
            raise ValueError(f"Unsupported database dialect: {url.drivername}")
    except Exception as e:
        if isinstance(e, ImportError):
            raise  # Re-raise the specific ImportError
        raise ValueError(f"Could not determine database type from DSN: {e}")


class PostgresLoader(LoaderInterface):
    """
    Concrete implementation of the LoaderInterface for PostgreSQL.

    This class uses the `psycopg2` driver to connect to the database and
    leverages the high-performance `COPY FROM STDIN` command for bulk loading.
    """

    def __init__(self, dsn_or_engine: Any):
        """
        Initializes the PostgresLoader with database connection details.

        Args:
            dsn_or_engine: A SQLAlchemy-compatible database connection string,
                           a psycopg2-style DSN string, or a pre-existing
                           SQLAlchemy Engine instance.
        """
        self.conn: PgConnection | None = None
        self.staging_table_name: str | None = None

        if isinstance(dsn_or_engine, sqlalchemy.engine.Engine):
            self.engine = dsn_or_engine
            # Recreate the psycopg2 DSN from the engine's URL
            url = self.engine.url
            self.psycopg2_dsn = (
                f"dbname='{url.database}' user='{url.username}' "
                f"password='{url.password}' host='{url.host}' port='{url.port}'"
            )
        else: # It's a DSN string
            dsn = str(dsn_or_engine)
            try:
                # Create the SQLAlchemy engine directly from the DSN
                self.engine = sqlalchemy.create_engine(dsn)
                # For psycopg2, we need to convert the URL to a DSN string
                url = make_url(dsn)
                self.psycopg2_dsn = (
                    f"dbname='{url.database}' user='{url.username}' "
                    f"password='{url.password}' host='{url.host}' port='{url.port}'"
                )

            except Exception as e:
                # Handle cases where the DSN might be in the simple format psycopg2
                # expects but SQLAlchemy does not.
                print(f"Could not parse DSN for SQLAlchemy, falling back. Error: {e}")
                self.psycopg2_dsn = dsn
                connect_args = {
                    item.split("=")[0]: item.split("=")[1]
                    for item in dsn.split()
                    if "=" in item
                }
                self.engine = sqlalchemy.create_engine(
                    "postgresql+psycopg2://", connect_args=connect_args
                )


    def connect(self) -> PgConnection:
        """Establishes and returns a connection to the PostgreSQL database."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.psycopg2_dsn)
        return self.conn

    def validate_schema(self, expected_tables: Dict[str, sqlalchemy.Table]) -> bool:
        """
        Validates the live database schema against the expected SQLAlchemy metadata.

        This method checks for:
        - Missing tables.
        - Missing columns in existing tables.
        - Column type mismatches (by class, e.g., String, Integer).
        - Primary key constraint mismatches.

        Args:
            expected_tables: A dictionary of SQLAlchemy Table objects representing
                             the expected schema (e.g., from schema.metadata.tables).

        Returns:
            True if the schema is valid.

        Raises:
            ValueError: If any discrepancies are found, containing a detailed
                        report of all errors.
        """
        inspector = sqlalchemy.inspect(self.engine)
        db_tables = inspector.get_table_names()
        errors = []

        # 1. Check for missing tables
        for table_name in expected_tables.keys():
            if table_name not in db_tables:
                errors.append(f"Table '{table_name}' is missing in the database.")

        if errors:
            # If tables are missing, no point in checking their columns
            raise ValueError("Schema validation failed:\n" + "\n".join(errors))

        # 2. Check columns and PKs for existing tables
        for table_name, table_obj in expected_tables.items():
            db_columns = {c['name']: c for c in inspector.get_columns(table_name)}
            expected_columns = {c.name: c for c in table_obj.columns}

            # Check for missing columns in the database
            for col_name, col_obj in expected_columns.items():
                if col_name not in db_columns:
                    errors.append(f"Table '{table_name}': Missing column '{col_name}'.")
                    continue  # No point checking type if it's missing

                # Check column type compatibility (by class)
                expected_type = col_obj.type
                db_type = db_columns[col_name]['type']
                if not isinstance(db_type, expected_type.__class__):
                    errors.append(
                        f"Table '{table_name}', Column '{col_name}': Type mismatch. "
                        f"Expected a subtype of {expected_type.__class__.__name__}, "
                        f"but found {db_type.__class__.__name__}."
                    )

            # Check for extra columns in DB (log as warning, not error)
            for col_name in db_columns.keys():
                if col_name not in expected_columns:
                    print(
                        f"Warning: Table '{table_name}' has an extra column "
                        f"'{col_name}' not defined in the schema model."
                    )

            # Check primary key
            db_pk_cols = inspector.get_pk_constraint(table_name)['constrained_columns']
            expected_pk_cols = [c.name for c in table_obj.primary_key.columns]

            if set(db_pk_cols) != set(expected_pk_cols):
                errors.append(
                    f"Table '{table_name}': Primary key mismatch. "
                    f"Expected {sorted(expected_pk_cols)}, found {sorted(db_pk_cols)}."
                )

        if errors:
            raise ValueError(
                "Schema validation failed with the following issues:\n"
                + "\n".join(errors)
            )

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
                # Use CASCADE to handle foreign key constraints automatically.
                cursor.execute(f"TRUNCATE TABLE {target_table} CASCADE;")
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

        column_sql = ""
        if columns:
            column_sql = f"({', '.join(columns)})"

        sql = f"COPY {target_table} {column_sql} FROM STDIN WITH CSV HEADER"

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

        inspector = sqlalchemy.inspect(self.engine)
        all_columns = [col["name"] for col in inspector.get_columns(target_table)]
        update_cols = [col for col in all_columns if col not in primary_keys]
        pk_string = ", ".join(primary_keys)

        if not update_cols:
            sql = f"""
                INSERT INTO {target_table} ({", ".join(all_columns)})
                SELECT * FROM {staging_table}
                ON CONFLICT ({pk_string}) DO NOTHING;
            """
        else:
            update_statements = []
            if target_table == "icsr_master":
                for col in update_cols:
                    if col == version_key:
                        statement = f"{col} = CASE WHEN EXCLUDED.is_nullified THEN {target_table}.{col} ELSE EXCLUDED.{col} END"
                        update_statements.append(statement)
                    else:
                        update_statements.append(f"{col} = EXCLUDED.{col}")
            else:
                update_statements = [f"{col} = EXCLUDED.{col}" for col in update_cols]

            update_clause = ", ".join(update_statements)

            sql = f"""
                INSERT INTO {target_table} ({", ".join(all_columns)})
                SELECT * FROM {staging_table}
                ON CONFLICT ({pk_string}) DO UPDATE
                SET {update_clause}
            """
            if target_table == 'icsr_master' and version_key:
                sql += f" WHERE {target_table}.{version_key} < EXCLUDED.{version_key} OR EXCLUDED.is_nullified;"
            elif version_key:
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

    def create_all_tables(self) -> None:
        """
        Creates all defined tables in the schema.py file against the target
        database. This is an idempotent operation.
        """
        metadata.create_all(self.engine)
        print("All tables created or already exist.")

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

    def _get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Dynamically retrieves primary key and version key for a given table
        by inspecting the database via the engine.
        """
        inspector = sqlalchemy.inspect(self.engine)

        if not inspector.has_table(table_name):
            raise ValueError(f"Table '{table_name}' not found in database.")

        # Get primary keys
        pk_constraint = inspector.get_pk_constraint(table_name)
        primary_keys = pk_constraint['constrained_columns']

        # Find version key by checking column comments
        version_key = None
        columns = inspector.get_columns(table_name)
        for col in columns:
            if col.get("comment") == "VERSION_KEY":
                version_key = col["name"]
                break

        return {"pk": primary_keys, "version_key": version_key}

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
        self.manage_transaction("BEGIN")
        try:
            total_rows = sum(row_counts.values())
            self._log_file_status(file_path, file_hash, "running", total_rows)

            for table_name, buffer in buffers.items():
                if row_counts.get(table_name, 0) > 0:
                    print(f"Processing table: {table_name}")
                    table_meta = self._get_table_metadata(table_name)

                    staging_table = self.prepare_load(
                        target_table=table_name, load_mode=load_mode
                    )
                    # Get columns from the CSV header
                    buffer.seek(0)
                    header = buffer.readline().strip()
                    columns = header.split(',')
                    buffer.seek(0) # Rewind for the loader

                    self.bulk_load_native(buffer, staging_table, columns=columns)

                    if load_mode == "delta":
                        self.handle_upsert(
                            staging_table=staging_table,
                            target_table=table_name,
                            primary_keys=table_meta["pk"],
                            version_key=table_meta["version_key"],
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

    def load_audit_data(
        self,
        buffer: IOBase,
        row_count: int,
        load_mode: str,
        file_path: str,
        file_hash: str,
    ) -> None:
        """
        Orchestrates the loading of the audit data (JSON) into the
        `icsr_audit_log` table within a single transaction.
        """
        TABLE_NAME = "icsr_audit_log"
        table_meta = self._get_table_metadata(TABLE_NAME)

        self.manage_transaction("BEGIN")
        try:
            self._log_file_status(file_path, file_hash, "running_audit", row_count)

            staging_table = self.prepare_load(
                target_table=TABLE_NAME, load_mode=load_mode
            )
            self.bulk_load_native(
                buffer,
                staging_table,
                columns=["safetyreportid", "receiptdate", "icsr_payload"],
            )

            if load_mode == "delta":
                self.handle_upsert(
                    staging_table=staging_table,
                    target_table=TABLE_NAME,
                    primary_keys=table_meta["pk"],
                    version_key=table_meta["version_key"],
                )

            self._log_file_status(file_path, file_hash, "completed_audit", row_count)
            self.manage_transaction("COMMIT")

        except Exception as e:
            print(f"Error during audit load for file {file_path}. Rolling back.")
            self.manage_transaction("ROLLBACK")
            self._log_file_status(file_path, file_hash, "failed_audit")
            self.manage_transaction("COMMIT")
            raise e
