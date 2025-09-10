import io
from unittest.mock import patch

import pytest
import sqlalchemy
from py_load_eudravigilance.loader import PostgresLoader, get_loader
from py_load_eudravigilance.parser import parse_icsr_xml, parse_icsr_xml_for_audit
from py_load_eudravigilance.transformer import (
    transform_and_normalize,
    transform_for_audit,
)
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer
from pytest_mock import MockerFixture


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


def test_create_all_tables(db_engine):
    """
    Tests the `create_all_tables` method of the PostgresLoader.
    It verifies that all tables defined in the schema are created in the DB.
    """
    loader = PostgresLoader(db_engine.url.render_as_string(hide_password=False))
    loader.create_all_tables()

    inspector = sqlalchemy.inspect(db_engine)
    tables = inspector.get_table_names()

    expected_tables = {
        "icsr_master",
        "patient_characteristics",
        "reactions",
        "drugs",
        "drug_substances",
        "tests_procedures",
        "case_summary_narrative",
        "icsr_audit_log",
        "etl_file_history",
    }

    assert expected_tables.issubset(set(tables))


def test_full_normalized_load(postgres_container, db_engine):
    """
    End-to-end integration test for the 'normalized' schema workflow.
    """
    # 1. Setup: Create all tables using the loader
    loader = PostgresLoader(db_engine.url.render_as_string(hide_password=False))
    loader.create_all_tables()

    # 3. E&T: Parse and transform the sample file
    with open("tests/sample_e2b.xml", "rb") as f:
        xml_content = f.read()

    # Use only the first 3 unique records for the full load test
    initial_xml = (
        xml_content.split(b"</ichicsrMessage>")[0]
        + b"</ichicsrMessage>\n"
        + xml_content.split(b"</ichicsrMessage>")[1]
        + b"</ichicsrMessage>\n"
        + xml_content.split(b"</ichicsrMessage>")[2]
        + b"</ichicsrMessage>\n</ichicsr>"
    )

    file_buffer = io.BytesIO(initial_xml)
    icsr_generator = parse_icsr_xml(file_buffer)
    buffers, row_counts, errors = transform_and_normalize(icsr_generator)
    assert not errors

    # 4. Load: The load_normalized_data method handles the transaction itself
    loader.load_normalized_data(
        buffers=buffers,
        row_counts=row_counts,
        load_mode="full",  # Use 'full' to test the TRUNCATE logic
        file_path="tests/sample_e2b.xml",
        file_hash="dummy_hash_for_testing",
    )

    # 5. Assert: Verify data was loaded correctly
    with db_engine.connect() as connection:
        # The sample file contains 3 ICSRs
        master_count = connection.execute(
            text("SELECT COUNT(*) FROM icsr_master")
        ).scalar_one()
        assert master_count == 3
        # The first ICSR has 2 drugs
        drug_count = connection.execute(text("SELECT COUNT(*) FROM drugs")).scalar_one()
        assert drug_count == 2
        # The two drugs have a total of 3 substances
        substance_count = connection.execute(
            text("SELECT COUNT(*) FROM drug_substances")
        ).scalar_one()
        assert substance_count == 3
        # Only the first ICSR has a test
        test_count = connection.execute(
            text("SELECT COUNT(*) FROM tests_procedures")
        ).scalar_one()
        assert test_count == 1
        # Only the first ICSR has a narrative
        narrative_count = connection.execute(
            text("SELECT COUNT(*) FROM case_summary_narrative")
        ).scalar_one()
        assert narrative_count == 1

        # Verify the content of drug_substances
        substances = connection.execute(
            text("SELECT * FROM drug_substances ORDER BY activesubstancename")
        ).fetchall()
        assert substances[0].activesubstancename == "SubstanceX"
        assert substances[0].drug_seq == 1
        assert substances[1].activesubstancename == "SubstanceY"
        assert substances[1].drug_seq == 2
        assert substances[2].activesubstancename == "SubstanceZ"
        assert substances[2].drug_seq == 2


def test_delta_audit_load(postgres_container, db_engine):
    """
    End-to-end integration test for the 'audit' schema workflow.
    """
    # 1. Setup: Create all tables using the loader
    loader = PostgresLoader(db_engine.url.render_as_string(hide_password=False))
    loader.create_all_tables()

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
        load_mode="delta",  # Test the staging/upsert path
        file_path="tests/sample_e2b.xml",
        file_hash="dummy_hash_for_audit_testing",
    )

    # 4. Assert: Verify the data was loaded
    with db_engine.connect() as connection:
        # After de-duplication, there should be 4 unique ICSRs
        audit_count = connection.execute(
            text("SELECT COUNT(*) FROM icsr_audit_log")
        ).scalar_one()
        assert audit_count == 4

        # Just check the first payload for validity
        payload = connection.execute(
            text(
                "SELECT icsr_payload FROM icsr_audit_log "
                "WHERE safetyreportid = 'TEST-CASE-001'"
            )
        ).scalar_one()
        assert isinstance(payload, dict)  # SQLAlchemy auto-deserializes JSONB to dict
        assert "patient" in payload
        assert "test" in payload
        assert payload["test"]["testname"] == "Blood Pressure"


def test_delta_load_with_nullification(postgres_container, db_engine):
    """
    Tests that a delta load correctly handles an ICSR nullification.
    """
    # 1. Setup: Create all tables using the loader
    loader = PostgresLoader(db_engine.url.render_as_string(hide_password=False))
    loader.create_all_tables()

    # 2. Initial Load: Load a file with 4 cases, including the one to be nullified.

    with open("tests/sample_e2b.xml", "rb") as f:
        xml_content = f.read()

    # Create an initial version of the file without the nullification message
    initial_xml = (
        xml_content.split(b"</ichicsrMessage>")[0]
        + b"</ichicsrMessage>\n"
        + xml_content.split(b"</ichicsrMessage>")[1]
        + b"</ichicsrMessage>\n"
        + xml_content.split(b"</ichicsrMessage>")[2]
        + b"</ichicsrMessage>\n</ichicsr>"
    )

    file_buffer_initial = io.BytesIO(initial_xml)
    icsr_generator_initial = parse_icsr_xml(file_buffer_initial)
    buffers_initial, row_counts_initial, errors_initial = transform_and_normalize(
        icsr_generator_initial
    )
    assert not errors_initial

    loader.load_normalized_data(
        buffers=buffers_initial,
        row_counts=row_counts_initial,
        load_mode="full",
        file_path="tests/initial.xml",
        file_hash="initial_hash",
    )

    # 3. Assert initial state
    with db_engine.connect() as connection:
        case2_initial = connection.execute(
            text("SELECT * FROM icsr_master WHERE safetyreportid = 'TEST-CASE-002'")
        ).first()
        assert case2_initial.is_nullified is False
        assert case2_initial.receiptdate == "20240102"

    # 4. Delta Load: Create a new file with just the nullification message
    # for TEST-CASE-002
    nullification_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<ichicsr xmlns="urn:hl7-org:v3">
  <ichicsrMessage>
    <safetyreport>
      <safetyreportversion>2</safetyreportversion>
      <safetyreportid>TEST-CASE-002</safetyreportid>
      <reporttype>4</reporttype>
      <receiptdate>20240104</receiptdate>
      <reportnullification>true</reportnullification>
    </safetyreport>
  </ichicsrMessage>
</ichicsr>
"""
    file_buffer_delta = io.BytesIO(nullification_xml)
    icsr_generator_delta = parse_icsr_xml(file_buffer_delta)
    buffers_delta, row_counts_delta, errors_delta = transform_and_normalize(
        icsr_generator_delta
    )
    assert not errors_delta

    loader.load_normalized_data(
        buffers=buffers_delta,
        row_counts=row_counts_delta,
        load_mode="delta",
        file_path="tests/delta.xml",
        file_hash="delta_hash",
    )

    # 5. Assert final state
    with db_engine.connect() as connection:
        master_count = connection.execute(
            text("SELECT COUNT(*) FROM icsr_master")
        ).scalar_one()
        assert master_count == 3  # Should not have added a new record

        case2_final = connection.execute(
            text("SELECT * FROM icsr_master WHERE safetyreportid = 'TEST-CASE-002'")
        ).first()
        assert case2_final.is_nullified is True
        # The receiptdate should NOT have been updated, as per our logic
        # for nullification
        assert case2_final.receiptdate == "20240102"


def test_get_loader_missing_dependency(mocker: MockerFixture):
    """
    Tests that get_loader raises an ImportError if the loader's dependencies
    (e.g., psycopg2) are not installed, resulting in a NameError on import.
    """
    class LoaderWithMissingDep:
        def __init__(self, dsn):
            # This will raise a NameError because a_missing_dep is not defined
            a_missing_dep.do_something()

    mock_ep = mocker.MagicMock()
    mock_ep.name = "postgresql"
    mock_ep.load.return_value = LoaderWithMissingDep
    mocker.patch("importlib.metadata.entry_points", return_value=[mock_ep])

    with pytest.raises(ImportError) as excinfo:
        get_loader("postgresql://user:pass@host/db")

    assert "Dependencies for the 'postgresql' loader are not installed" in str(excinfo.value)

def test_get_loader_plugin_system(mocker: MockerFixture):
    """
    Tests the plugin-based get_loader function.
    This test relies on the entry point being correctly configured in pyproject.toml.
    """
    # Note: This test requires the package to be installed in editable mode
    # (`poetry install`) for the entry points to be discoverable.

    # 1. Test successful discovery of the registered PostgresLoader
    dsn = "postgresql://user:pass@host:5432/dbname"
    loader = get_loader(dsn)
    assert isinstance(loader, PostgresLoader)

    # 2. Test that it finds the loader with a different driver variant
    dsn_psycopg2 = "postgresql+psycopg2://user:pass@host:5432/dbname"
    loader_psycopg2 = get_loader(dsn_psycopg2)
    assert isinstance(loader_psycopg2, PostgresLoader)

    # 3. Test for a non-existent loader
    dsn_mysql = "mysql://user:pass@host:3306/dbname"
    with pytest.raises(ValueError) as excinfo:
        get_loader(dsn_mysql)
    # Check that the error message is helpful
    assert "No registered loader for dialect 'mysql'" in str(excinfo.value)
    assert "Available loaders: ['postgresql']" in str(excinfo.value)

    # 4. Test for an invalid DSN
    with pytest.raises(ValueError) as excinfo:
        get_loader("not-a-valid-dsn")
    assert "Could not determine database dialect from DSN" in str(excinfo.value)

    # 5. Test for entry point loading error
    mocker.patch(
        "importlib.metadata.entry_points", side_effect=Exception("EP loading failed")
    )
    with pytest.raises(RuntimeError) as excinfo:
        get_loader(dsn)
    assert "Could not load entry points: EP loading failed" in str(excinfo.value)

    # 6. Test for loader instantiation error
    class BadLoader:
        def __init__(self, dsn):
            raise ValueError("bad loader")

    mock_ep = mocker.MagicMock()
    mock_ep.name = "postgresql"
    mock_ep.load.return_value = BadLoader
    mocker.patch("importlib.metadata.entry_points", return_value=[mock_ep])
    with pytest.raises(RuntimeError) as excinfo:
        get_loader(dsn)
    assert "Failed to instantiate loader for dialect 'postgresql'" in str(excinfo.value)


def test_postgres_loader_init_with_simple_dsn():
    """Test PostgresLoader with a simple DSN string."""
    loader = PostgresLoader("dbname=test user=user")
    assert loader.engine is not None


def test_prepare_load_unknown_mode(db_engine):
    """Test prepare_load with an unknown load mode."""
    loader = PostgresLoader(db_engine)
    with db_engine.connect() as conn:
        with pytest.raises(ValueError):
            loader.prepare_load(conn, "icsr_master", "unknown_mode")


def test_validate_schema_failures(db_engine):
    """Test schema validation failures."""
    loader = PostgresLoader(db_engine)
    loader.create_all_tables()

    # Test missing table
    with pytest.raises(ValueError) as excinfo:
        loader.validate_schema({"missing_table": None})
    assert "Table 'missing_table' is missing" in str(excinfo.value)

    # Test missing column
    from sqlalchemy import Table, Column, Integer, MetaData
    bad_table = Table("icsr_master", MetaData(), Column("missing_col", Integer))
    with pytest.raises(ValueError) as excinfo:
        loader.validate_schema({"icsr_master": bad_table})
    assert "Missing column 'missing_col'" in str(excinfo.value)

    # Test wrong column type
    bad_type_table = Table("icsr_master", MetaData(), Column("safetyreportid", Integer))
    with pytest.raises(ValueError) as excinfo:
        loader.validate_schema({"icsr_master": bad_type_table})
    assert "Type mismatch" in str(excinfo.value)

    # Test wrong PK
    bad_pk_table = Table("icsr_master", MetaData(), Column("safetyreportid", Integer, primary_key=True), Column("extra_pk", Integer, primary_key=True))
    with pytest.raises(ValueError) as excinfo:
        loader.validate_schema({"icsr_master": bad_pk_table})
    assert "PK mismatch" in str(excinfo.value)

def test_load_normalized_data_failure_logs_status(db_engine):
    """
    Tests that if an exception occurs during `load_normalized_data`,
    the file status is logged as 'failed'.
    """
    loader = PostgresLoader(db_engine)
    loader.create_all_tables()

    # Mock the internal bulk load to raise an error
    with patch.object(
        loader, "bulk_load_native", side_effect=Exception("Disk is full")
    ) as mock_bulk_load, patch.object(
        loader, "_log_file_status"
    ) as mock_log_status:
        buffers = {"icsr_master": io.StringIO("c1,c2\nv1,v2")}
        row_counts = {"icsr_master": 1}

        with pytest.raises(Exception, match="Disk is full"):
            loader.load_normalized_data(
                buffers=buffers,
                row_counts=row_counts,
                load_mode="delta",
                file_path="f.xml",
                file_hash="h1",
            )

        # Check that the status was first set to 'running', then to 'failed'
        assert mock_log_status.call_count == 2
        # The first call is to set status to 'running'
        mock_log_status.assert_any_call(
            mock_bulk_load.call_args.args[0], "f.xml", "h1", "running", 1
        )
        # The second call is in the except block to set status to 'failed'
        mock_log_status.assert_called_with(
            mock_log_status.call_args.args[0], "f.xml", "h1", "failed"
        )


def test_load_normalized_data_zero_rows(db_engine):
    """
    Tests that `load_normalized_data` does not attempt to load data for a
    table if the row count is zero.
    """
    loader = PostgresLoader(db_engine)
    with patch.object(loader, "bulk_load_native") as mock_bulk_load:
        loader.load_normalized_data(
            buffers={"table1": io.StringIO()},
            row_counts={"table1": 0},
            load_mode="delta",
            file_path="f.xml",
            file_hash="h1",
        )
        mock_bulk_load.assert_not_called()


def test_load_audit_data_failure_logs_status(db_engine):
    """
    Tests that if an exception occurs during `load_audit_data`,
    the file status is logged as 'failed_audit'.
    """
    loader = PostgresLoader(db_engine)
    loader.create_all_tables()

    with patch.object(
        loader, "bulk_load_native", side_effect=Exception("Disk is full")
    ) as mock_bulk_load, patch.object(
        loader, "_log_file_status"
    ) as mock_log_status:
        buffer = io.StringIO("c1,c2\nv1,v2")

        with pytest.raises(Exception, match="Disk is full"):
            loader.load_audit_data(
                buffer=buffer,
                row_count=1,
                load_mode="delta",
                file_path="f.xml",
                file_hash="h1",
            )

        assert mock_log_status.call_count == 2
        mock_log_status.assert_any_call(
            mock_bulk_load.call_args.args[0], "f.xml", "h1", "running_audit", 1
        )
        mock_log_status.assert_called_with(
            mock_log_status.call_args.args[0], "f.xml", "h1", "failed_audit"
        )


def test_get_table_metadata_not_found(db_engine):
    """Test _get_table_metadata for a non-existent table."""
    loader = PostgresLoader(db_engine)
    with pytest.raises(ValueError):
        loader._get_table_metadata("non_existent_table")
