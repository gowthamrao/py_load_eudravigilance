from pathlib import Path

import pytest
import sqlalchemy
from py_load_eudravigilance.config import DatabaseConfig, Settings
from py_load_eudravigilance.run import run_etl
from testcontainers.postgres import PostgresContainer

# Define the path to the test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture(scope="function")
def db_container():
    """A fixture to provide a fresh, ephemeral PostgreSQL database for each test function."""
    with PostgresContainer("postgres:14") as container:
        # The container is started when entering the 'with' block
        # and automatically stopped/cleaned up on exit.
        yield container


@pytest.fixture(scope="function")
def test_settings(db_container: PostgresContainer) -> Settings:
    """A fixture to create a Settings object configured to use the test database."""
    dsn = db_container.get_connection_url()
    return Settings(
        source_uri="",  # Will be set by each test
        schema_type="normalized",
        database=DatabaseConfig(dsn=dsn),
        quarantine_uri=None,  # Not testing quarantine in this suite
    )


def run_single_file_etl(settings: Settings, xml_filename: str):
    """Helper function to run the main ETL process on a single test file."""
    file_path = TEST_DATA_DIR / xml_filename
    settings.source_uri = str(file_path)

    # Initialize the database schema for the test
    from py_load_eudravigilance.loader import get_loader

    loader = get_loader(settings.database.dsn)
    loader.create_all_tables()

    # Run the ETL in delta mode
    run_etl(settings=settings, mode="delta", max_workers=1)


def get_icsr_master_row(
    engine: sqlalchemy.engine.Engine, report_id: str
) -> dict | None:
    """Helper function to fetch a specific row from the icsr_master table."""
    with engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).first()
        return result._asdict() if result else None


def test_initial_insert(db_container: PostgresContainer, test_settings: Settings):
    """
    Tests that processing a new XML file correctly inserts a new record
    into the database.
    """
    # Arrange
    dsn = db_container.get_connection_url()
    engine = sqlalchemy.create_engine(dsn)
    report_id = "TEST-SCENARIO-01"

    # Act
    run_single_file_etl(test_settings, "test_insert.xml")

    # Assert
    row = get_icsr_master_row(engine, report_id)
    assert row is not None
    assert row["safetyreportid"] == report_id
    assert row["date_of_most_recent_info"] == "20250101"
    assert row["senderidentifier"] == "SENDER-ID"
    assert row["is_nullified"] is False


def test_successful_amendment(db_container: PostgresContainer, test_settings: Settings):
    """
    Tests that processing an amendment with a newer version date correctly
    updates the existing record.
    """
    # Arrange: Load the initial record first
    dsn = db_container.get_connection_url()
    engine = sqlalchemy.create_engine(dsn)
    report_id = "TEST-SCENARIO-01"
    run_single_file_etl(test_settings, "test_insert.xml")

    # Act: Process the amendment file
    run_single_file_etl(test_settings, "test_amendment.xml")

    # Assert: The record should be updated
    row = get_icsr_master_row(engine, report_id)
    assert row is not None
    assert row["date_of_most_recent_info"] == "20250102"
    assert row["senderidentifier"] == "SENDER-ID-UPDATED"


def test_nullification(db_container: PostgresContainer, test_settings: Settings):
    """
    Tests that a nullification record correctly updates the 'is_nullified'
    flag and preserves existing data from the previous version.
    """
    # Arrange: Load an up-to-date, non-nullified record first.
    dsn = db_container.get_connection_url()
    engine = sqlalchemy.create_engine(dsn)
    report_id = "TEST-SCENARIO-01"
    run_single_file_etl(test_settings, "test_amendment.xml")

    # Act: Process the nullification file.
    run_single_file_etl(test_settings, "test_nullification.xml")

    # Assert: The record should be marked as nullified.
    row = get_icsr_master_row(engine, report_id)
    assert row is not None
    assert row["is_nullified"] is True
    # The version should be updated to the nullification's version.
    assert row["date_of_most_recent_info"] == "20250103"
    # The loader logic should preserve the data from the previous version
    # if the nullification record is sparse.
    assert row["senderidentifier"] == "SENDER-ID-UPDATED"


def test_stale_amendment_is_ignored(
    db_container: PostgresContainer, test_settings: Settings
):
    """
    Tests that processing an amendment with an older or same version date
    does NOT update the existing record.
    """
    # Arrange: Load the newest version of the record first
    dsn = db_container.get_connection_url()
    engine = sqlalchemy.create_engine(dsn)
    report_id = "TEST-SCENARIO-01"
    run_single_file_etl(test_settings, "test_amendment.xml")

    # Act: Process the stale amendment file
    run_single_file_etl(test_settings, "test_stale_amendment.xml")

    # Assert: The record should NOT be updated
    row = get_icsr_master_row(engine, report_id)
    assert row is not None
    # Data should be from the 'test_amendment.xml' file, not the stale one.
    assert row["date_of_most_recent_info"] == "20250102"
    assert row["senderidentifier"] == "SENDER-ID-UPDATED"


def test_idempotency_and_reprocessing(
    db_container: PostgresContainer, test_settings: Settings
):
    """
    Tests that reprocessing the same files does not change the database state
    and that an older version cannot overwrite a newer nullification.
    """
    # Arrange
    dsn = db_container.get_connection_url()
    engine = sqlalchemy.create_engine(dsn)
    report_id = "TEST-SCENARIO-01"

    # Act & Assert Phase 1: Run the full sequence
    run_single_file_etl(test_settings, "test_insert.xml")  # version 20250101
    run_single_file_etl(test_settings, "test_amendment.xml")  # version 20250102
    run_single_file_etl(test_settings, "test_nullification.xml")  # version 20250103

    # Assert state after nullification
    row_after_null = get_icsr_master_row(engine, report_id)
    assert row_after_null is not None
    assert row_after_null["is_nullified"] is True
    assert row_after_null["date_of_most_recent_info"] == "20250103"

    # Act & Assert Phase 2: Try to re-process the original insert (stale)
    run_single_file_etl(test_settings, "test_insert.xml")
    row_after_reinsert = get_icsr_master_row(engine, report_id)
    # The record should be unchanged because the insert is older than the nullification
    assert row_after_reinsert == row_after_null

    # Act & Assert Phase 3: Reprocess the nullification file (idempotency)
    run_single_file_etl(test_settings, "test_nullification.xml")
    row_after_idempotency = get_icsr_master_row(engine, report_id)
    # The record should be unchanged
    assert row_after_idempotency == row_after_null
