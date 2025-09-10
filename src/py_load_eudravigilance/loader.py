"""
Handles the Loading phase of the ETL process.

This module defines the database loader interface and provides concrete
implementations for different database backends (e.g., PostgreSQL).
It is responsible for all database interactions, including native bulk loading.
"""
from importlib import metadata
from io import IOBase
from typing import Any, Dict, List

import sqlalchemy
from sqlalchemy import Table, select

from . import schema as db_schema
from .interfaces import LoaderInterface

try:
    import psycopg2
    from psycopg2.extensions import connection as PgConnection
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.engine.url import make_url
except ImportError:
    # This is a soft dependency, so we don't raise the error here.
    # The error will be raised by get_loader if a postgres DSN is provided.
    pass


def get_loader(dsn_or_engine: Any) -> LoaderInterface:
    """
    Factory function to get the appropriate database loader using a plugin system.
    It discovers available loaders via the `py_load_eudravigilance.loaders`
    entry point group.
    """
    try:
        # Use SQLAlchemy to parse the DSN and identify the dialect
        url = make_url(str(dsn_or_engine))
        # The dialect name (e.g., 'postgresql', 'mysql') is used as the plugin key
        dialect_name = url.get_dialect().name
    except Exception as e:
        raise ValueError(f"Could not determine database dialect from DSN: {e}") from e

    # Discover registered loader plugins
    try:
        loaders = metadata.entry_points(group="py_load_eudravigilance.loaders")
    except Exception as e:
        # This can happen if the entry points are misconfigured.
        raise RuntimeError(f"Could not load entry points: {e}") from e

    # Find a matching loader by its registered name (which should match the dialect)
    for ep in loaders:
        if ep.name == dialect_name:
            loader_class = ep.load()
            try:
                # Instantiate and return the loader
                return loader_class(dsn_or_engine)
            except NameError as ne:
                # This typically means a soft dependency (like psycopg2) is missing.
                # The original NameError is obscure, so we raise a more helpful error.
                raise ImportError(
                    f"Dependencies for the '{dialect_name}' loader are not installed. "
                    f"Please install the required extras (e.g., 'pip install .[postgres]'). "
                    f"Original error: {ne}"
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to instantiate loader for dialect '{dialect_name}'."
                ) from e

    # If no loader was found after checking all entry points
    raise ValueError(
        f"No registered loader found for database dialect '{dialect_name}'. "
        f"Available loaders: {[ep.name for ep in loaders]}"
    )


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
        self.staging_table_name: str | None = None

        if isinstance(dsn_or_engine, sqlalchemy.engine.Engine):
            self.engine = dsn_or_engine
            # Recreate the psycopg2 DSN from the engine's URL
            url = self.engine.url
            self.psycopg2_dsn = (
                f"dbname='{url.database}' user='{url.username}' "
                f"password='{url.password}' host='{url.host}' port='{url.port}'"
            )
        else:  # It's a DSN string
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
                connect_args = {
                    item.split("=")[0]: item.split("=")[1]
                    for item in dsn.split()
                    if "=" in item
                }
                self.engine = sqlalchemy.create_engine(
                    "postgresql+psycopg2://", connect_args=connect_args
                )

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
            db_columns = {c["name"]: c for c in inspector.get_columns(table_name)}
            expected_columns = {c.name: c for c in table_obj.columns}

            # Check for missing columns in the database
            for col_name, col_obj in expected_columns.items():
                if col_name not in db_columns:
                    errors.append(f"Table '{table_name}': Missing column '{col_name}'.")
                    continue  # No point checking type if it's missing

                # Check column type compatibility (by class)
                expected_type = col_obj.type
                db_type = db_columns[col_name]["type"]
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
            db_pk_cols = inspector.get_pk_constraint(table_name)["constrained_columns"]
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

    def prepare_load(
        self, conn: sqlalchemy.Connection, target_table: str, load_mode: str
    ) -> str:
        """
        Prepares the database for loading. For 'delta' mode, this is part of
        the file transaction. For 'full' mode, this is a separate, committed
        action before processing begins.
        """
        if load_mode == "full":
            # Use CASCADE to handle foreign key constraints automatically.
            conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {target_table} CASCADE;"))
            print(f"Table '{target_table}' truncated for full load.")
            return target_table

        elif load_mode == "delta":
            staging_table_name = f"__staging_{target_table}"
            # This runs inside a transaction, so the temp table is transactional.
            conn.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {staging_table_name};"))
            create_sql = (
                f"CREATE TEMP TABLE {staging_table_name} "
                f"(LIKE {target_table} INCLUDING ALL);"
            )
            conn.execute(sqlalchemy.text(create_sql))
            print(f"Temporary staging table '{staging_table_name}' created.")
            return staging_table_name

        else:
            raise ValueError(f"Unknown load mode: {load_mode}")

    def bulk_load_native(
        self,
        conn: sqlalchemy.Connection,
        data_stream: IOBase,
        target_table: str,
        columns: List[str],
    ) -> None:
        """
        Loads data from an in-memory buffer into a PostgreSQL table using COPY.
        This method does NOT commit the transaction.
        """
        column_sql = ""
        if columns:
            column_sql = f"({', '.join(columns)})"

        sql = f"COPY {target_table} {column_sql} FROM STDIN WITH CSV HEADER"

        # Get the raw psycopg2 connection from the SQLAlchemy connection
        raw_conn = conn.connection
        with raw_conn.cursor() as cursor:
            cursor.copy_expert(sql, data_stream)

        print(f"Successfully loaded data into '{target_table}'.")

    def handle_upsert(
        self,
        conn: sqlalchemy.Connection,
        staging_table: str,
        target_table: str,
        primary_keys: List[str],
        version_key: str | None,
    ) -> None:
        """
        Merges data from the staging table into the target table using
        SQLAlchemy Core for safety and robustness. This method does NOT
        commit the transaction.
        """
        target_table_obj = self._get_table_obj(conn, target_table)
        staging_table_obj = self._get_table_obj(conn, staging_table)

        # Reflect the columns from the staging table to build the insert
        insert_stmt = pg_insert(target_table_obj).from_select(
            [c.name for c in staging_table_obj.c], select(staging_table_obj)
        )

        # For the ON CONFLICT clause, get the "excluded" table proxy
        excluded = insert_stmt.excluded

        # Dynamically build the SET clause for the UPDATE part
        update_dict = {}
        has_nullified_col = "is_nullified" in target_table_obj.c

        for col in target_table_obj.c:
            if col.name not in primary_keys and not col.server_default:
                if col.name == "is_nullified":
                    # The nullification flag should always be updated from the source.
                    update_dict[col.name] = excluded[col.name]
                elif col.name == version_key:
                    # The version key should always be updated from the incoming record
                    update_dict[col.name] = excluded[col.name]
                elif has_nullified_col:
                    # For all other columns, keep the existing value if the new
                    # record is a nullification; otherwise, take the new value.
                    # This prevents overwriting data with nulls from a sparse
                    # nullification record.
                    update_dict[col.name] = sqlalchemy.case(
                        (excluded.is_nullified, col),
                        else_=excluded[col.name],
                    )
                else:
                    # If the table doesn't have a nullification concept, always update.
                    update_dict[col.name] = excluded[col.name]

        # If there are columns to update, add the DO UPDATE clause
        if update_dict:
            where_clause = None
            if version_key:
                # The WHERE clause should apply if the new record is newer,
                # OR if the new record is a nullification instruction.
                where_clause = target_table_obj.c[version_key] < excluded[version_key]
                if has_nullified_col:
                    where_clause = sqlalchemy.or_(where_clause, excluded.is_nullified)

            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=primary_keys,
                set_=update_dict,
                where=where_clause,
            )
        else:
            # If there are no columns to update, just do nothing on conflict
            upsert_stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=primary_keys
            )

        conn.execute(upsert_stmt)

        print(f"Upsert completed from '{staging_table}' to '{target_table}'.")

    def _get_table_obj(self, conn: sqlalchemy.Connection, table_name: str) -> Table:
        """
        Gets a SQLAlchemy Table object from the schema or by reflection.
        It uses the provided connection for reflection, which is crucial
        for transaction-scoped temporary tables.
        """
        # First, try to get the table from our predefined schema
        table_obj = db_schema.metadata.tables.get(table_name)
        if table_obj is not None:
            return table_obj

        # If not found (e.g., a temp or test-specific table), reflect it
        temp_meta = sqlalchemy.MetaData()
        return Table(table_name, temp_meta, autoload_with=conn)

    def create_all_tables(self) -> None:
        """
        Creates all defined tables in the schema.py file against the target
        database. This is an idempotent operation.
        """
        db_schema.metadata.create_all(self.engine)
        print("All tables created or already exist.")

    def _log_file_status(
        self,
        conn: sqlalchemy.Connection,
        filename: str,
        file_hash: str,
        status: str,
        rows_processed: int | None = None,
    ) -> None:
        """
        Logs or updates the status of a file in the history table.
        This method does NOT commit the transaction.
        """
        history_table = self._get_table_obj(conn, "etl_file_history")

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
        conn.execute(update_stmt)

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
        primary_keys = pk_constraint["constrained_columns"]

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
        # For a full load, truncate all tables first in a separate transaction.
        if load_mode == "full":
            with self.engine.begin() as conn:
                for table_name in buffers.keys():
                    self.prepare_load(conn, table_name, load_mode)

        try:
            with self.engine.begin() as conn:
                total_rows = sum(row_counts.values())
                self._log_file_status(conn, file_path, file_hash, "running", total_rows)

                for table_name, buffer in buffers.items():
                    if row_counts.get(table_name, 0) > 0:
                        print(f"Processing table: {table_name}")
                        # In 'full' mode, data goes directly to the target table.
                        # In 'delta' mode, it goes to a staging table.
                        if load_mode == "full":
                            target_or_staging_table = table_name
                        else:  # delta
                            target_or_staging_table = self.prepare_load(
                                conn, target_table=table_name, load_mode=load_mode
                            )

                        buffer.seek(0)
                        header = buffer.readline().strip()
                        columns = header.split(",")
                        buffer.seek(0)

                        self.bulk_load_native(
                            conn, buffer, target_or_staging_table, columns
                        )

                        if load_mode == "delta":
                            table_meta = self._get_table_metadata(table_name)
                            self.handle_upsert(
                                conn,
                                staging_table=target_or_staging_table,
                                target_table=table_name,
                                primary_keys=table_meta["pk"],
                                version_key=table_meta["version_key"],
                            )

                self._log_file_status(
                    conn, file_path, file_hash, "completed", total_rows
                )
        except Exception as e:
            print(f"Error during normalized load for file {file_path}. Rolling back.")
            with self.engine.begin() as conn:
                self._log_file_status(conn, file_path, file_hash, "failed")
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

        if load_mode == "full":
            with self.engine.begin() as conn:
                self.prepare_load(conn, TABLE_NAME, load_mode)

        try:
            with self.engine.begin() as conn:
                self._log_file_status(
                    conn, file_path, file_hash, "running_audit", row_count
                )

                if load_mode == "full":
                    target_or_staging_table = TABLE_NAME
                else:  # delta
                    target_or_staging_table = self.prepare_load(
                        conn, target_table=TABLE_NAME, load_mode=load_mode
                    )

                # The columns must match the CSV output of `transform_for_audit`
                self.bulk_load_native(
                    conn,
                    buffer,
                    target_or_staging_table,
                    columns=[
                        "safetyreportid",
                        "date_of_most_recent_info",
                        "receiptdate",
                        "icsr_payload",
                    ],
                )

                if load_mode == "delta":
                    table_meta = self._get_table_metadata(TABLE_NAME)
                    self.handle_upsert(
                        conn,
                        staging_table=target_or_staging_table,
                        target_table=TABLE_NAME,
                        primary_keys=table_meta["pk"],
                        version_key=table_meta["version_key"],
                    )

                self._log_file_status(
                    conn, file_path, file_hash, "completed_audit", row_count
                )
        except Exception as e:
            print(f"Error during audit load for file {file_path}. Rolling back.")
            with self.engine.begin() as conn:
                self._log_file_status(conn, file_path, file_hash, "failed_audit")
            raise e
