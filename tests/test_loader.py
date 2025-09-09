import pytest
from testcontainers.postgres import PostgresContainer
import sqlalchemy
from sqlalchemy import text

from py_load_eudravigilance.loader import PostgresLoader

@pytest.fixture(scope="module")
def postgres_container():
    """Pytest fixture to manage a PostgreSQL test container."""
    with PostgresContainer("postgres:13") as container:
        yield container

@pytest.fixture(scope="module")
def db_engine(postgres_container):
    """Pytest fixture to provide a SQLAlchemy engine for the container."""
    engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    yield engine
    engine.dispose()

from py_load_eudravigilance.parser import parse_icsr_xml
from py_load_eudravigilance.transformer import transform_and_normalize
import io

def test_full_normalized_load(postgres_container, db_engine):
    """
    End-to-end integration test for the 'normalized' schema workflow.
    """
    # 1. DDL for all normalized tables
    ddl_statements = [
        """CREATE TABLE icsr_master (safetyreportid TEXT PRIMARY KEY, receiptdate TEXT);""",
        """CREATE TABLE patient_characteristics (safetyreportid TEXT, patientinitials TEXT, patientonsetage TEXT, patientsex TEXT);""",
        """CREATE TABLE reactions (safetyreportid TEXT, primarysourcereaction TEXT, reactionmeddrapt TEXT);""",
        """CREATE TABLE drugs (safetyreportid TEXT, drugcharacterization TEXT, medicinalproduct TEXT, drugstructuredosagenumb TEXT, drugstructuredosageunit TEXT, drugdosagetext TEXT);""",
        """CREATE TABLE tests_procedures (safetyreportid TEXT, testdate TEXT, testname TEXT, testresult TEXT, testresultunit TEXT, testcomments TEXT);""",
        """CREATE TABLE case_summary_narrative (safetyreportid TEXT, narrative TEXT);"""
    ]

    # 2. Setup: Create tables
    with db_engine.connect() as connection:
        for ddl in ddl_statements:
            connection.execute(text(ddl))
        connection.commit()

    # 3. E&T: Parse and transform the sample file
    with open("tests/sample_e2b.xml", "rb") as f:
        xml_content = f.read()

    file_buffer = io.BytesIO(xml_content)
    icsr_generator = parse_icsr_xml(file_buffer)
    buffers, row_counts = transform_and_normalize(icsr_generator)

    # 4. Load: Instantiate loader and run the load
    loader = PostgresLoader(dsn=postgres_container.get_connection_url())
    loader.create_metadata_tables() # This was the missing step
    # The load_normalized_data method handles the transaction itself
    loader.load_normalized_data(
        buffers=buffers,
        row_counts=row_counts,
        load_mode="full", # Use 'full' to test the TRUNCATE logic
        file_path="tests/sample_e2b.xml",
        file_hash="dummy_hash_for_testing"
    )

    # 5. Assert: Verify data was loaded correctly
    with db_engine.connect() as connection:
        # The sample file contains 3 ICSRs
        master_count = connection.execute(text("SELECT COUNT(*) FROM icsr_master")).scalar_one()
        assert master_count == 3
        # Only the first ICSR has drugs
        drug_count = connection.execute(text("SELECT COUNT(*) FROM drugs")).scalar_one()
        assert drug_count == 2
        # Only the first ICSR has a test
        test_count = connection.execute(text("SELECT COUNT(*) FROM tests_procedures")).scalar_one()
        assert test_count == 1
        # Only the first ICSR has a narrative
        narrative_count = connection.execute(text("SELECT COUNT(*) FROM case_summary_narrative")).scalar_one()
        assert narrative_count == 1


from py_load_eudravigilance.parser import parse_icsr_xml_for_audit
from py_load_eudravigilance.transformer import transform_for_audit
import json

def test_delta_audit_load(postgres_container, db_engine):
    """
    End-to-end integration test for the 'audit' schema workflow.
    """
    # 1. Setup: Create metadata tables, which includes the audit log table
    loader = PostgresLoader(dsn=postgres_container.get_connection_url())
    loader.create_metadata_tables()

    # 2. E&T: Parse and transform the sample file for audit
    with open("tests/sample_e2b.xml", "rb") as f:
        xml_content = f.read()

    file_buffer = io.BytesIO(xml_content)
    icsr_generator = parse_icsr_xml_for_audit(file_buffer)
    buffer, row_count = transform_for_audit(icsr_generator)

    # 3. Load: Run the audit data load
    loader.load_audit_data(
        buffer=buffer,
        row_count=row_count,
        load_mode="delta", # Test the staging/upsert path
        file_path="tests/sample_e2b.xml",
        file_hash="dummy_hash_for_audit_testing"
    )

    # 4. Assert: Verify the data was loaded
    with db_engine.connect() as connection:
        # The sample file contains 3 ICSRs
        audit_count = connection.execute(text("SELECT COUNT(*) FROM icsr_audit_log")).scalar_one()
        assert audit_count == 3

        # Just check the first payload for validity
        payload = connection.execute(text("SELECT icsr_payload FROM icsr_audit_log WHERE safetyreportid = 'TEST-CASE-001'")).scalar_one()
        assert isinstance(payload, dict) # SQLAlchemy auto-deserializes JSONB to dict
        assert "patient" in payload
        assert "test" in payload
        assert payload["test"]["testname"] == "Blood Pressure"
