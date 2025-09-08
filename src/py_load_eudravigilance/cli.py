"""
Defines the Command Line Interface (CLI) for the application.

This module uses Typer to create a user-friendly CLI for running ETL
processes, managing the database, and validating files.
"""

import hashlib
import io
from pathlib import Path
from typing import Optional

import fsspec
import typer
from typing_extensions import Annotated

from .config import load_config, CONFIG_FILE_NAME
from .loader import PostgresLoader
from .parser import parse_icsr_xml
from .transformer import transform_to_csv_buffer

# Create a Typer application instance
app = typer.Typer(
    help="A high-performance ETL tool for EudraVigilance ICSR XML files."
)


@app.command()
def run(
    source_uri: Annotated[
        Optional[str],
        typer.Argument(
            help="URI for the source XML files (e.g., 'data/*.xml', 's3://my-bucket/data/*.xml'). "
            "Overrides the source_uri in the config file."
        ),
    ] = None,
    table_name: Annotated[
        str,
        typer.Option(
            "--table-name",
            help="Name of the target database table.",
        ),
    ] = "icsr_master",
    mode: Annotated[
        str,
        typer.Option(
            help="Load mode: 'delta' for incremental upserts or 'full' for a full refresh."
        ),
    ] = "delta",
    pk: Annotated[
        str,
        typer.Option(
            "--pk",
            help="The primary key column for the upsert operation (used in delta mode).",
        ),
    ] = "safetyreportid",
    version_key: Annotated[
        str,
        typer.Option(
            "--version-key",
            help="The column used for versioning (used in delta mode).",
        ),
    ] = "receiptdate",
    config_file: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help=f"Path to the configuration file (default: ./{CONFIG_FILE_NAME}).",
        ),
    ] = f"./{CONFIG_FILE_NAME}",
):
    """
    Run the full ETL pipeline: Parse, Transform, and Load XML files from a source URI.
    """
    # 1. Load Configuration
    try:
        settings = load_config(path=str(config_file))
    except (ValueError, FileNotFoundError) as e:
        typer.secho(f"Configuration Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # Determine the source URI (CLI argument takes precedence)
    final_source_uri = source_uri or settings.source_uri
    if not final_source_uri:
        typer.secho(
            "Error: A source URI must be provided either as an argument or in the config file.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    typer.echo(f"Starting ETL process (mode: {mode})")
    typer.echo(f"Source: {final_source_uri}")

    # 2. Use fsspec to open all files matching the URI
    try:
        # mode='rb' is important for reading XML files correctly
        input_files = fsspec.open_files(final_source_uri, mode="rb")
        if not input_files:
            typer.secho("No files found at the specified source URI.", fg=typer.colors.YELLOW)
            raise typer.Exit()
    except Exception as e:
        typer.secho(f"Error accessing source files: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


    # 3. Initialize the Loader
    loader = PostgresLoader(dsn=settings.database.dsn)
    loader.connect()

    files_processed = 0
    files_failed = 0

    try:
        # 4. Get completed file hashes if in delta mode
        completed_hashes = set()
        if mode == "delta":
            typer.echo("Fetching history of completed files...")
            completed_hashes = loader.get_completed_file_hashes()
            typer.echo(f"Found {len(completed_hashes)} previously processed files.")

        # 5. Loop through each file and process it
        for file in input_files:
            file_content: bytes | None = None
            file_hash: str | None = None
            file_path = file.path

            try:
                with file as f:
                    typer.echo(f"\n--- Processing file: {file_path} ---")
                    file_content = f.read()
                    file_hash = hashlib.sha256(file_content).hexdigest()

                # 5a. Check if file has already been processed
                if file_hash in completed_hashes:
                    typer.secho(f"Skipping already processed file: {file_path}", fg=typer.colors.YELLOW)
                    continue

                file_buffer = io.BytesIO(file_content)

                # 5b. Extract & Transform
                typer.echo("Parsing and transforming XML data...")
                icsr_generator = parse_icsr_xml(file_buffer)
                csv_buffer, row_count = transform_to_csv_buffer(icsr_generator)

                if row_count == 0:
                    typer.echo("No ICSR messages found in file. Skipping.")
                    # Log as completed with 0 rows
                    loader._log_file_status(file_path, file_hash, "completed", 0)
                    loader.manage_transaction("COMMIT")
                    continue

                # 5c. Load Phase (inside a transaction)
                loader.manage_transaction("BEGIN")

                # Log initial status
                loader._log_file_status(file_path, file_hash, "running", row_count)

                load_table = loader.prepare_load(target_table=table_name, load_mode=mode)
                loader.bulk_load_native(csv_buffer, load_table, columns=[])

                if mode == "delta":
                    typer.echo(f"Merging data from '{load_table}' into '{table_name}'...")
                    loader.handle_upsert(
                        staging_table=load_table,
                        target_table=table_name,
                        primary_keys=[pk],
                        version_key=version_key,
                    )

                # Finalize by logging 'completed' and committing
                loader._log_file_status(file_path, file_hash, "completed", row_count)
                loader.manage_transaction("COMMIT")

                files_processed += 1
                typer.secho(f"Successfully processed file: {file_path}", fg=typer.colors.GREEN)

            except Exception as e:
                # Rollback transaction and log failure
                loader.manage_transaction("ROLLBACK")
                if file_hash:
                    # This update runs in its own transaction
                    loader._log_file_status(file_path, file_hash, "failed")
                    loader.manage_transaction("COMMIT")

                typer.secho(f"Failed to process file {file_path}: {e}", fg=typer.colors.RED)
                files_failed += 1

        typer.secho(
            f"\nETL process finished. {files_processed} files processed successfully, {files_failed} failed.",
            fg=typer.colors.GREEN if files_failed == 0 else typer.colors.YELLOW,
        )

    finally:
        # Ensure the database connection is closed
        if loader.conn and not loader.conn.closed:
            loader.conn.close()
            typer.echo("Database connection closed.")


@app.command()
def init_db(
    config_file: Annotated[
        Path,
        typer.Option(
            "--config",
            "-c",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help=f"Path to the configuration file (default: ./{CONFIG_FILE_NAME}).",
        ),
    ] = f"./{CONFIG_FILE_NAME}",
):
    """
    Initializes the database with the required metadata tables.
    """
    typer.echo("Initializing database...")
    try:
        settings = load_config(path=str(config_file))
        loader = PostgresLoader(dsn=settings.database.dsn)
        loader.create_metadata_tables()
        typer.secho("Database initialization complete.", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Database initialization failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
