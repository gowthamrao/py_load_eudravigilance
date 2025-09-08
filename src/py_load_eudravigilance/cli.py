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
):
    """
    Run the full ETL pipeline: Parse, Transform, and Load an XML file.
    """
    typer.echo(f"Starting ETL process for: {xml_file.name}")

    # 1. Initialize the Loader
    loader = PostgresLoader(dsn=db_dsn)

    try:
        # 2. Extract & Transform Phase
        typer.echo("Parsing and transforming XML data...")
        with open(xml_file, "rb") as f:
            # The parser yields ICSR dictionaries
            icsr_generator = parse_icsr_xml(f)
            # The transformer converts them to an in-memory CSV buffer
            csv_buffer = transform_to_csv_buffer(icsr_generator)

        # Check if any data was produced
        if csv_buffer.getbuffer().nbytes == 0:
            typer.echo("No ICSR messages found in the file. Nothing to load.")
            raise typer.Exit()

        # 3. Load Phase
        typer.echo(f"Loading data into table: {table_name}")
        # We don't need to pass columns; the loader uses CSV HEADER
        loader.bulk_load_native(csv_buffer, table_name, columns=[])

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
