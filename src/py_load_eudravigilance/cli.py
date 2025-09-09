"""
Defines the Command Line Interface (CLI) for the application.

This module uses Typer to create a user-friendly CLI for running ETL
processes, managing the database, and validating files.
"""

import hashlib
import io
import os
import concurrent.futures
from pathlib import Path
from typing import Optional, Tuple

import fsspec
import typer
import sqlalchemy
from typing_extensions import Annotated

from .config import load_config, CONFIG_FILE_NAME
from .loader import PostgresLoader
from .parser import parse_icsr_xml, parse_icsr_xml_for_audit
from .transformer import transform_and_normalize, transform_for_audit

# Create a Typer application instance
app = typer.Typer(
    help="A high-performance ETL tool for EudraVigilance ICSR XML files."
)


def process_single_file(
    file_content: bytes,
    file_path: str,
    db_dsn: str,
    schema_type: str,
    mode: str,
) -> Tuple[str, str]:
    """
    Processes a single XML file content.

    This function is designed to be called by a ProcessPoolExecutor. It
    initializes its own database loader to ensure process safety.
    """
    try:
        file_hash = hashlib.sha256(file_content).hexdigest()
        # Each worker creates its own engine and loader
        engine = sqlalchemy.create_engine(db_dsn)
        loader = PostgresLoader(engine)

        file_buffer = io.BytesIO(file_content)

        if schema_type == "normalized":
            icsr_generator = parse_icsr_xml(file_buffer)
            buffers, row_counts = transform_and_normalize(icsr_generator)
            if not row_counts.get("icsr_master"):
                loader._log_file_status(file_path, file_hash, "completed", 0)
                loader.manage_transaction("COMMIT")
                return file_path, "skipped_no_icsr"
            loader.load_normalized_data(
                buffers=buffers,
                row_counts=row_counts,
                load_mode=mode,
                file_path=file_path,
                file_hash=file_hash,
            )
        elif schema_type == "audit":
            icsr_generator = parse_icsr_xml_for_audit(file_buffer)
            buffer, row_count = transform_for_audit(icsr_generator)
            if row_count == 0:
                loader._log_file_status(file_path, file_hash, "completed_audit", 0)
                loader.manage_transaction("COMMIT")
                return file_path, "skipped_no_icsr"
            loader.load_audit_data(
                buffer=buffer,
                row_count=row_count,
                load_mode=mode,
                file_path=file_path,
                file_hash=file_hash,
            )
        return file_path, "success"
    except Exception as e:
        print(f"Error processing file {file_path} in worker: {e}")
        return file_path, "failure"
    finally:
        if 'engine' in locals():
            engine.dispose()


@app.command()
def run(
    source_uri: Annotated[
        Optional[str],
        typer.Argument(
            help="URI for the source XML files (e.g., 'data/*.xml', 's3://my-bucket/data/*.xml'). "
            "Overrides the source_uri in the config file."
        ),
    ] = None,
    mode: Annotated[
        str,
        typer.Option(
            help="Load mode: 'delta' for incremental upserts or 'full' for a full refresh."
        ),
    ] = "delta",
    workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel worker processes to use."
        ),
    ] = os.cpu_count() or 1,
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
    Run the full ETL pipeline in parallel: Parse, Transform, and Load XML files.
    """
    # 1. Load Configuration and create main engine
    try:
        settings = load_config(path=str(config_file))
        engine = sqlalchemy.create_engine(settings.database.dsn)
    except (ValueError, FileNotFoundError) as e:
        typer.secho(f"Configuration Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    final_source_uri = source_uri or settings.source_uri
    if not final_source_uri:
        typer.secho("Error: A source URI must be provided.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    typer.echo(f"Starting ETL process (mode: {mode}, workers: {workers})")
    typer.echo(f"Source: {final_source_uri}")

    # 2. Use fsspec to find files
    try:
        input_files = fsspec.open_files(final_source_uri, mode="rb")
        if not input_files:
            typer.secho("No files found at the specified URI.", fg=typer.colors.YELLOW)
            raise typer.Exit()
    except Exception as e:
        typer.secho(f"Error accessing source files: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    files_to_process = []
    if mode == "delta":
        typer.echo("Fetching history of completed files...")
        main_loader = PostgresLoader(engine)
        completed_hashes = main_loader.get_completed_file_hashes()
        typer.echo(f"Found {len(completed_hashes)} previously processed files to skip.")
        for file in input_files:
            with file as f:
                content = f.read()
                file_hash = hashlib.sha256(content).hexdigest()
                if file_hash not in completed_hashes:
                    files_to_process.append((content, file.path))
                else:
                    typer.secho(f"Skipping already processed file: {file.path}", fg=typer.colors.YELLOW)
    else: # full mode
        for file in input_files:
             with file as f:
                content = f.read()
                files_to_process.append((content, file.path))

    if not files_to_process:
        typer.secho("No new files to process.", fg=typer.colors.GREEN)
        raise typer.Exit()

    typer.echo(f"Found {len(files_to_process)} files to process.")

    files_processed = 0
    files_failed = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_path = {
            executor.submit(
                process_single_file,
                content,
                path,
                settings.database.dsn,
                settings.schema_type,
                mode,
            ): path
            for content, path in files_to_process
        }

        for future in concurrent.futures.as_completed(future_to_path):
            path = future_to_path[future]
            try:
                _, status = future.result()
                if status == "success":
                    files_processed += 1
                    typer.secho(f"Successfully processed file: {path}", fg=typer.colors.GREEN)
                elif status == "failure":
                    files_failed += 1
                    typer.secho(f"Failed to process file: {path}", fg=typer.colors.RED)
                else: # Skipped
                    typer.secho(f"Skipped file (no data or already processed in worker): {path}", fg=typer.colors.YELLOW)
            except Exception as exc:
                files_failed += 1
                typer.secho(f"File {path} generated an exception: {exc}", fg=typer.colors.RED)

    typer.secho(
        f"\nETL process finished. {files_processed} files processed successfully, {files_failed} failed.",
        fg=typer.colors.GREEN if files_failed == 0 else typer.colors.YELLOW,
    )
    if files_failed > 0:
        raise typer.Exit(code=1)

    engine.dispose()


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
        engine = sqlalchemy.create_engine(settings.database.dsn)
        loader = PostgresLoader(engine)
        loader.create_metadata_tables()
        typer.secho("Database initialization complete.", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Database initialization failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    finally:
        if 'engine' in locals():
            engine.dispose()


if __name__ == "__main__":
    app()
