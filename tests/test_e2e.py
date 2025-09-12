"""
End-to-end tests for the py_load_eudravigilance package.

These tests are designed to cover the full application lifecycle, including:
- Running the CLI tool.
- Handling files with mixed valid/invalid data.
- Testing idempotency and versioning through the full pipeline.
"""
import subprocess
import sys
from pathlib import Path

import pytest
import sqlalchemy
from py_load_eudravigilance.loader import PostgresLoader
from testcontainers.postgres import PostgresContainer

# Get the root of the project to correctly reference test data files
TEST_ROOT = Path(__file__).parent


@pytest.fixture(scope="function")
def postgres_container():
    """Spins up a PostgreSQL container for each test function."""
    with PostgresContainer("postgres:13") as container:
        yield container


@pytest.fixture(scope="function")
def db_engine(postgres_container):
    """Provides a SQLAlchemy engine for the test container for each test function."""
    return postgres_container.get_connection_url()


@pytest.fixture(scope="function")
def loader(db_engine):
    """
    Provides a configured PostgresLoader instance for each test function.
    This ensures that each test runs with a clean loader state.
    """
    # Create the loader
    loader = PostgresLoader(db_engine)
    # Ensure the database schema is created before each test
    loader.create_all_tables()
    return loader


def test_process_file_with_mixed_validity(loader):
    """
    Tests the system's ability to process a file containing both valid and
    malformed ICSR records, ensuring valid data is loaded and errors are
    captured.
    """
    from py_load_eudravigilance.parser import parse_icsr_xml
    from py_load_eudravigilance.transformer import transform_and_normalize

    # 1. Define the path to the test file
    xml_file_path = TEST_ROOT / "data" / "mixed_validity.xml"

    # 2. Parse the XML and transform the data
    with open(xml_file_path, "rb") as f:
        icsr_generator = parse_icsr_xml(f)
        buffers, row_counts, parsing_errors = transform_and_normalize(icsr_generator)

    # 3. Assert that errors were correctly identified
    assert len(parsing_errors) == 1
    # After recovery, the parser often can't find the main safetyreport tag
    # in the malformed block. So we check for the expected recovery behavior.
    assert "Missing required element: safetyreport" in parsing_errors[0].message

    # 4. Assert that the correct number of valid records were processed
    assert row_counts.get("icsr_master", 0) == 2
    assert row_counts.get("patient_characteristics", 0) == 2

    # 5. Load the processed valid data into the database
    loader.load_normalized_data(
        buffers=buffers,
        row_counts=row_counts,
        load_mode="delta",
        file_path=str(xml_file_path),
        file_hash="fake_hash_mixed",
    )

    # 6. Verify the database state
    with loader.engine.connect() as conn:
        # Check that the two valid records were inserted
        master_table = sqlalchemy.Table("icsr_master", sqlalchemy.MetaData(), autoload_with=conn)
        result = conn.execute(
            sqlalchemy.select(master_table.c.safetyreportid).order_by(
                master_table.c.safetyreportid
            )
        ).fetchall()

        assert len(result) == 2
        assert result[0][0] == "VALID-01"
        assert result[1][0] == "VALID-02"

        # Ensure the invalid record was not loaded
        count = conn.execute(
            sqlalchemy.select(sqlalchemy.func.count())
            .select_from(master_table)
            .where(master_table.c.safetyreportid == "INVALID-01")
        ).scalar_one()
        assert count == 0


def test_cli_end_to_end_run(db_engine, tmp_path):
    """
    Tests the full end-to-end process using the CLI, covering initialization,
    delta loading, file-level idempotency, and record-level updates.
    """
    # 1. Setup: Create a temporary config file
    config_path = tmp_path / "config.yaml"
    config_content = f"""
database:
  dsn: {db_engine}
"""
    config_path.write_text(config_content)

    # Define the command base for all CLI calls
    # Define the command base for all CLI calls by finding the executable
    # script that poetry installed in the virtual environment's bin directory.
    venv_bin_path = Path(sys.executable).parent
    cli_path = venv_bin_path / "eudravigloader"
    assert cli_path.exists(), f"CLI executable not found at {cli_path}"

    cli_path = venv_bin_path / "eudravigloader"
    assert cli_path.exists(), f"CLI executable not found at {cli_path}"

    # 2. Test `init-db`
    init_result = subprocess.run(
        [str(cli_path), "init-db", "--config", str(config_path)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "All tables created or already exist" in init_result.stdout

    # Verify tables were created
    engine = sqlalchemy.create_engine(db_engine)
    inspector = sqlalchemy.inspect(engine)
    assert "icsr_master" in inspector.get_table_names()
    assert "etl_file_history" in inspector.get_table_names()

    # 3. Test initial delta load
    insert_file = TEST_ROOT / "data" / "test_insert.xml"
    run_result_1 = subprocess.run(
        [
            str(cli_path),
            "run",
            "--config",
            str(config_path),
            str(insert_file),  # Positional argument
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "ETL process finished" in run_result_1.stderr
    assert "Success: 1, Failures: 0" in run_result_1.stderr

    # Verify data in the database
    engine = sqlalchemy.create_engine(db_engine)
    with engine.connect() as conn:
        master_table = sqlalchemy.Table("icsr_master", sqlalchemy.MetaData(), autoload_with=conn)
        count = conn.execute(sqlalchemy.select(sqlalchemy.func.count()).select_from(master_table)).scalar_one()
        assert count == 1
        record = conn.execute(sqlalchemy.select(master_table).where(master_table.c.safetyreportid == "TEST-SCENARIO-01")).first()
        assert record is not None
        assert record._asdict()["reportercountry"] == "US"

    # 4. Test file-level idempotency (re-running the same file)
    run_result_2 = subprocess.run(
        [
            str(cli_path),
            "run",
            "--config",
            str(config_path),
            str(insert_file),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "0 new files to process" in run_result_2.stderr
    assert "ETL process finished" in run_result_2.stderr

    # Verify no new data was added
    engine = sqlalchemy.create_engine(db_engine)
    with engine.connect() as conn:
        master_table = sqlalchemy.Table("icsr_master", sqlalchemy.MetaData(), autoload_with=conn)
        count = conn.execute(sqlalchemy.select(sqlalchemy.func.count()).select_from(master_table)).scalar_one()
        assert count == 1

    # 5. Test record-level amendment
    amend_file = TEST_ROOT / "data" / "test_amendment.xml"
    run_result_3 = subprocess.run(
        [
            str(cli_path),
            "run",
            "--config",
            str(config_path),
            str(amend_file),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Success: 1, Failures: 0" in run_result_3.stderr

    # Verify the record was updated
    engine = sqlalchemy.create_engine(db_engine)
    with engine.connect() as conn:
        master_table = sqlalchemy.Table("icsr_master", sqlalchemy.MetaData(), autoload_with=conn)
        # Still only one record total
        count = conn.execute(sqlalchemy.select(sqlalchemy.func.count()).select_from(master_table)).scalar_one()
        assert count == 1
        # The record should be updated
        record = conn.execute(sqlalchemy.select(master_table).where(master_table.c.safetyreportid == "TEST-SCENARIO-01")).first()
        assert record is not None
        assert record._asdict()["reportercountry"] == "CA" # This was changed in the amendment file
        assert record._asdict()["date_of_most_recent_info"] == "20250102"
