"""
Integration tests for the Loader component.

These tests use testcontainers to spin up a real PostgreSQL database
in a Docker container to validate the entire ETL pipeline end-to-end.
"""

import pytest
from pathlib import Path
from testcontainers.postgres import PostgresContainer

from py_load_eudravigilance.parser import parse_icsr_xml
from py_load_eudravigilance.transformer import transform_to_csv_buffer
from py_load_eudravigilance.loader import PostgresLoader

# Get the directory of the current test file to build a path to the sample data.
TEST_DIR = Path(__file__).parent
SAMPLE_XML_PATH = TEST_DIR / "sample_e2b.xml"


@pytest.fixture(scope="module")
def postgres_container():
    """
    Pytest fixture to manage the lifecycle of a PostgreSQL test container.
    The container is started once per module and torn down after all tests
    in the module have run.
    """
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


def test_full_etl_pipeline_integration(postgres_container: PostgresContainer):
    """
    Tests the full ETL pipeline from parsing to loading into a live PostgreSQL DB.

    It verifies that:
    1. A connection to the test database can be established.
    2. A target table can be created.
    3. The PostgresLoader can successfully execute a native bulk load.
    4. The data loaded into the database is correct and matches the source.
    """
    # 1. Get database connection details from the container
    dsn = postgres_container.get_connection_url()
    # The DSN from testcontainers might use 'psycopg2', which we can remove
    # for direct use with psycopg2.connect
    dsn = dsn.replace("postgresql+psycopg2://", "").replace(
        "@", f"@{postgres_container.get_container_host_ip()}:"
    )

    # 2. Setup: Create a target table in the test database
    conn = postgres_container.get_driver().connect(dsn)
    table_name = "icsr_master_test"
    with conn.cursor() as cursor:
        # Using IF NOT EXISTS for safety, though the container is fresh
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                safetyreportid VARCHAR(255) PRIMARY KEY,
                receiptdate VARCHAR(255),
                patientinitials VARCHAR(255),
                patientonsetage VARCHAR(10),
                patientsex VARCHAR(1)
            );
        """)
    conn.commit()
    conn.close()

    # 3. Run the E-T-L process
    # Initialize the loader with the test DB's DSN
    loader = PostgresLoader(dsn=dsn)

    # Parse and transform the sample file
    with open(SAMPLE_XML_PATH, "rb") as f:
        icsr_generator = parse_icsr_xml(f)
        csv_buffer = transform_to_csv_buffer(icsr_generator)

    # Load the data into the test database
    # The columns list is currently unused but required by the interface
    loader.bulk_load_native(csv_buffer, table_name, columns=[])

    # 4. Assertions: Verify the data was loaded correctly
    conn = postgres_container.get_driver().connect(dsn)
    with conn.cursor() as cursor:
        # Check that all 3 records from the sample file were inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        assert count == 3

        # Check the content of a specific record to ensure data integrity
        cursor.execute(f"SELECT patientinitials, patientonsetage FROM {table_name} WHERE safetyreportid = 'TEST-CASE-001';")
        record = cursor.fetchone()
        assert record is not None
        assert record[0] == "FN"
        assert record[1] == "55"
    conn.close()
