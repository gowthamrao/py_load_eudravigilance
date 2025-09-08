"""
Defines the Command Line Interface (CLI) for the application.

This module uses Typer to create a user-friendly CLI for running ETL
processes, managing the database, and validating files.
"""

from pathlib import Path
from typing_extensions import Annotated
import typer

from .loader import PostgresLoader
from .parser import parse_icsr_xml
from .transformer import transform_to_csv_buffer

# Create a Typer application instance
app = typer.Typer(
    help="A high-performance ETL tool for EudraVigilance ICSR XML files."
)


@app.command()
def run(
    xml_file: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Path to the source EudraVigilance XML file.",
        ),
    ],
    db_dsn: Annotated[
        str,
        typer.Argument(
            help="Database connection string (DSN), e.g., 'dbname=test user=postgres'."
        ),
    ],
    table_name: Annotated[
        str, typer.Argument(help="Name of the target database table.")
    ],
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
            default="safetyreportid",
        ),
    ],
    version_key: Annotated[
        str,
        typer.Option(
            "--version-key",
            help="The column used for versioning (used in delta mode).",
            default="receiptdate",
        ),
    ],
):
    """
    Run the full ETL pipeline: Parse, Transform, and Load an XML file.
    """
    typer.echo(f"Starting ETL process for: {xml_file.name} (mode: {mode})")

    # 1. Initialize the Loader
    loader = PostgresLoader(dsn=db_dsn)
    loader.connect()

    try:
        # 2. Extract & Transform Phase
        typer.echo("Parsing and transforming XML data...")
        with open(xml_file, "rb") as f:
            icsr_generator = parse_icsr_xml(f)
            csv_buffer = transform_to_csv_buffer(icsr_generator)

        if csv_buffer.getbuffer().nbytes == 0:
            typer.echo("No ICSR messages found in the file. Nothing to load.")
            raise typer.Exit()

        # 3. Load Phase
        typer.echo("Preparing database for loading...")
        load_table = loader.prepare_load(target_table=table_name, load_mode=mode)

        typer.echo(f"Loading data into table: {load_table}")
        loader.bulk_load_native(csv_buffer, load_table, columns=[])

        if mode == "delta":
            typer.echo(f"Merging data from '{load_table}' into '{table_name}'...")
            loader.handle_upsert(
                staging_table=load_table,
                target_table=table_name,
                primary_keys=[pk],
                version_key=version_key,
            )

        typer.secho(
            "ETL process completed successfully.", fg=typer.colors.GREEN
        )

    except Exception as e:
        typer.secho(f"An error occurred: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    finally:
        # Ensure the database connection is closed
        if loader.conn and not loader.conn.closed:
            loader.conn.close()
            typer.echo("Database connection closed.")


@app.command()
def init_db():
    """Placeholder for a command to initialize the database schema."""
    typer.echo("Database initialization logic is not yet implemented.")


if __name__ == "__main__":
    app()
