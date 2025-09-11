import os
from contextlib import contextmanager
from io import IOBase
from typing import Any, Dict, Generator, List

import sqlalchemy
from sqlalchemy import Table, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine.url import make_url

import pandas as pd
import io

from . import schema as db_schema
from .base import BaseLoader


class PostgresLoader(BaseLoader):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.engine = None

        if "engine" in config:
            self.engine = config["engine"]
        elif "dsn" in config:
            dsn = str(config["dsn"])
            try:
                self.engine = sqlalchemy.create_engine(dsn)
            except Exception as e:
                print(f"Could not parse DSN for SQLAlchemy, falling back. Error: {e}")
                connect_args = {
                    item.split("=")[0]: item.split("=")[1]
                    for item in dsn.split()
                    if "=" in item
                }
                self.engine = sqlalchemy.create_engine(
                    "postgresql+psycopg2://", connect_args=connect_args
                )
        else:
            raise ValueError("PostgresLoader config must contain 'dsn' or 'engine'")

    @contextmanager
    def connect(self) -> Generator:
        """Connect to the database."""
        with self.engine.connect() as connection:
            with connection.begin():
                yield connection

    def load_dataframe(
        self, df: pd.DataFrame, table_name: str, connection: Any
    ) -> None:
        """
        Loads a pandas DataFrame into the specified table using a bulk upsert
        strategy (INSERT ... ON CONFLICT ...).
        """
        if df.empty:
            return

        if table_name not in db_schema.metadata.tables:
            # Fallback to simple copy for tables not in our schema
            # (e.g., for integration tests)
            with io.StringIO() as buffer:
                df.to_csv(buffer, index=False, header=True)
                buffer.seek(0)
                self._bulk_copy(connection, buffer, table_name, list(df.columns))
            return

        pk_cols = [
            c.name for c in db_schema.metadata.tables[table_name].primary_key.columns
        ]

        with io.StringIO() as buffer:
            df.to_csv(buffer, index=False, header=True)
            buffer.seek(0)
            self._bulk_upsert_from_stream(
                connection, buffer, table_name, list(df.columns), pk_cols
            )

    def _bulk_upsert_from_stream(
        self,
        conn: sqlalchemy.Connection,
        data_stream: IOBase,
        target_table: str,
        columns: List[str],
        pk_cols: List[str],
    ):
        temp_table_name = f"temp_{target_table}"

        conn.execute(
            text(f"CREATE TEMP TABLE {temp_table_name} (LIKE {target_table});")
        )

        self._bulk_copy(conn, data_stream, temp_table_name, columns)

        update_cols = [col for col in columns if col not in pk_cols]

        if not update_cols:
            insert_sql = f"""
            INSERT INTO {target_table} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table_name}
            ON CONFLICT ({', '.join(pk_cols)}) DO NOTHING;
            """
        else:
            update_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            where_clause = ""
            if target_table == "icsr_master":
                where_clause = "WHERE icsr_master.date_of_most_recent_info <= EXCLUDED.date_of_most_recent_info"

            insert_sql = f"""
            INSERT INTO {target_table} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table_name}
            ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {update_stmt}
            {where_clause};
            """
        conn.execute(text(insert_sql))

    def _bulk_copy(
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

    def create_all_tables(self) -> None:
        """
        Creates all defined tables in the schema.py file against the target
        database. This is an idempotent operation.
        """
        db_schema.metadata.create_all(self.engine)
        print("All tables created or already exist.")

    def get_completed_file_hashes(self, connection: Any) -> set[str]:
        """
        Retrieves a set of file hashes for all files that have been
        successfully processed ('completed' status).

        Returns:
            A set of SHA-256 hash strings.
        """
        metadata = sqlalchemy.MetaData()
        history_table = sqlalchemy.Table(
            "etl_file_history", metadata, autoload_with=connection.engine
        )
        query = sqlalchemy.select(history_table.c.file_hash).where(
            history_table.c.status == "completed"
        )
        result = connection.execute(query)
        return {row[0] for row in result}

    def validate_schema(self, tables: dict) -> None:
        """
        Validates the database schema against the provided table definitions.
        """
        # This is a placeholder. A real implementation would inspect the database.
        pass

    def add_file_to_history(
        self, file_path: str, file_hash: str, connection: Any
    ) -> None:
        """Add a file to the ETL history table."""
        metadata = sqlalchemy.MetaData()
        history_table = sqlalchemy.Table(
            "etl_file_history", metadata, autoload_with=connection.engine
        )
        stmt = pg_insert(history_table).values(
            filename=os.path.basename(file_path),
            file_hash=file_hash,
            status="completed",
        )
        connection.execute(stmt)

    def get_table_schema(self, table_name: str) -> Any:
        """Get the schema for a specific table."""
        return db_schema.metadata.tables[table_name]
