"""
Defines the Command Line Interface (CLI) for the application.

This module uses Typer to create a user-friendly CLI for running ETL
processes, managing the database, and validating files.
"""

import os
from pathlib import Path
from typing import Optional

import fsspec
import typer
from typing_extensions import Annotated

from .config import load_config, CONFIG_FILE_NAME
from .loader import get_loader
from .parser import validate_xml_with_xsd
from . import schema as db_schema
from . import run as etl_run

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
    mode: Annotated[
        str,
        typer.Option(
            help="Load mode: 'delta' for incremental upserts or 'full' for a full refresh."
        ),
    ] = "delta",
    validate: Annotated[
        bool,
        typer.Option(
            "--validate",
            help="Enable XSD validation for each XML file before processing. Requires xsd_schema_path in config.",
        ),
    ] = False,
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
    Run the full ETL pipeline: Discover, Parse, Transform, and Load XML files.
    """
    # 1. Load Configuration
    try:
        settings = load_config(path=str(config_file))
    except (ValueError, FileNotFoundError) as e:
        typer.secho(f"Configuration Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # Override config with CLI arguments if provided
    if source_uri:
        settings.source_uri = source_uri

    if not settings.source_uri:
        typer.secho("Error: A source URI must be provided via argument or in the config file.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    if validate and not settings.xsd_schema_path:
        typer.secho("Error: --validate flag requires 'xsd_schema_path' to be set in the config file.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    typer.echo(f"Starting ETL process (mode: {mode}, workers: {workers}, validate: {validate})")

    # 2. Call the main ETL orchestration function
    try:
        etl_run.run_etl(settings, mode=mode, max_workers=workers, validate=validate)
        typer.secho("\nETL process completed successfully.", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"\nAn error occurred during the ETL process: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


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
    Initializes the database with all required tables (data, audit, metadata).
    """
    typer.echo("Initializing database...")
    try:
        settings = load_config(path=str(config_file))
        # The DSN is now passed to the loader, not the engine directly
        loader = get_loader(settings.database.dsn)
        loader.create_all_tables()
        typer.secho(
            "Database initialization complete. All tables created.",
            fg=typer.colors.GREEN,
        )
    except Exception as e:
        typer.secho(f"Database initialization failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@app.command()
def validate(
    source_uri: Annotated[
        str,
        typer.Argument(
            help="URI for the source XML files to validate (e.g., 'data/*.xml', 's3://my-bucket/data/*.xml')."
        ),
    ],
    schema: Annotated[
        Path,
        typer.Option(
            "--schema",
            "-s",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Path to the XSD schema file to validate against.",
        ),
    ],
):
    """
    Validate one or more XML files against an XSD schema.
    """
    typer.echo(f"Starting validation using schema: {schema}")
    typer.echo(f"Source: {source_uri}")

    try:
        input_files = fsspec.open_files(source_uri, mode="rb")
        if not input_files:
            typer.secho("No files found at the specified URI.", fg=typer.colors.YELLOW)
            raise typer.Exit()
    except Exception as e:
        typer.secho(f"Error accessing source files: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    valid_count = 0
    invalid_count = 0

    for file in input_files:
        typer.echo(f"--> Validating file: {file.path}")
        with file as f:
            is_valid, errors = validate_xml_with_xsd(f, str(schema))
            if is_valid:
                valid_count += 1
                typer.secho(f"    [VALID] {file.path}", fg=typer.colors.GREEN)
            else:
                invalid_count += 1
                typer.secho(f"    [INVALID] {file.path}", fg=typer.colors.RED)
                for error in errors:
                    typer.secho(f"      - {error}", fg=typer.colors.RED)

    summary_color = typer.colors.GREEN if invalid_count == 0 else typer.colors.YELLOW
    typer.secho(
        f"\nValidation summary: {valid_count} file(s) valid, {invalid_count} file(s) invalid.",
        fg=summary_color,
    )

    if invalid_count > 0:
        raise typer.Exit(code=1)


@app.command()
def validate_db_schema(
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
    Validates that the live database schema matches the expected schema model.
    """
    typer.echo("Connecting to database for schema validation...")
    try:
        settings = load_config(path=str(config_file))
        loader = get_loader(settings.database.dsn)

        typer.echo("Running schema validation...")
        # We pass the tables dictionary from our central schema definition
        loader.validate_schema(db_schema.metadata.tables)

        typer.secho(
            "Schema validation successful. The database schema matches the expected definitions.",
            fg=typer.colors.GREEN,
        )
    except ValueError as e:
        # ValueError is raised by our validation logic on failure
        typer.secho("Schema Validation Failed:", fg=typer.colors.RED)
        typer.secho(str(e), fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"An unexpected error occurred during validation: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
