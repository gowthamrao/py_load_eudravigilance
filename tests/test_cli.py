import pytest
import yaml
from typer.testing import CliRunner
from testcontainers.postgres import PostgresContainer
from sqlalchemy import create_engine, text

from py_load_eudravigilance.cli import app

runner = CliRunner()

@pytest.fixture(scope="module")
def postgres_container():
    """Fixture to start and stop a PostgreSQL test container."""
    with PostgresContainer("postgres:13-alpine") as postgres:
        yield postgres

@pytest.fixture(scope="module")
def db_engine(postgres_container: PostgresContainer):
    """Provides a SQLAlchemy engine for the test container."""
    url = postgres_container.get_connection_url()
    engine = create_engine(url)
    yield engine
    engine.dispose()


def test_cli_integration_flow(db_engine, tmp_path):
    """
    Tests the full CLI flow: init-db, run, run again (skipped), and failed run.
    """
    # 1. Prepare test files and config
    config_path = tmp_path / "config.yaml"
    source_data_path = tmp_path / "data"
    source_data_path.mkdir()

    # Valid XML file
    valid_xml_path = source_data_path / "good.xml"
    valid_xml_content = """
    <ichicsrMessage>
      <safetyreport>
        <safetyreportid>TEST-1</safetyreportid>
        <receiptdate>20250101</receiptdate>
        <patient>
          <patientinitials>AB</patientinitials>
          <patientonsetage>30</patientonsetage>
          <patientsex>1</patientsex>
        </patient>
      </safetyreport>
    </ichicsrMessage>
    """
    valid_xml_path.write_text(valid_xml_content)

    # Malformed XML file
    malformed_xml_path = source_data_path / "bad.xml"
    malformed_xml_path.write_text("<unclosedtag>")

    # Config file
    db_url = db_engine.url
    # DSN format for psycopg2
    dsn = f"dbname='{db_url.database}' user='{db_url.username}' password='{db_url.password}' host='{db_url.host}' port='{db_url.port}'"
    config_content = {"database": {"dsn": dsn}}
    config_path.write_text(yaml.dump(config_content))

    # 2. Run init-db
    result = runner.invoke(app, ["init-db", "--config", str(config_path)])
    assert result.exit_code == 0
    assert "Database initialization complete" in result.stdout

    # Create the target table for the test
    with db_engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE icsr_master (
                safetyreportid TEXT PRIMARY KEY,
                receiptdate TEXT,
                patientinitials TEXT,
                patientonsetage TEXT,
                patientsex TEXT
            );
        """))
        conn.commit()

    # 3. Run the first time with the valid file
    result_run1 = runner.invoke(app, [
        "run",
        str(valid_xml_path),
        "--table-name", "icsr_master",
        "--config", str(config_path)
    ])
    print(f"Run 1 stdout:\n{result_run1.stdout}")
    print(f"Run 1 stderr:\n{result_run1.stderr}")
    assert result_run1.exit_code == 0
    assert "Successfully processed file" in result_run1.stdout
    assert "1 files processed successfully, 0 failed" in result_run1.stdout

    # 4. Verify the database state after the first run
    with db_engine.connect() as conn:
        history_count = conn.execute(text("SELECT COUNT(*) FROM etl_file_history WHERE status = 'completed'")).scalar_one()
        assert history_count == 1
        data_count = conn.execute(text("SELECT COUNT(*) FROM icsr_master")).scalar_one()
        assert data_count == 1

    # 5. Run the second time, expecting to skip the file
    result_run2 = runner.invoke(app, [
        "run",
        str(valid_xml_path),
        "--table-name", "icsr_master",
        "--config", str(config_path)
    ])
    assert result_run2.exit_code == 0
    assert "Skipping already processed file" in result_run2.stdout
    assert "0 files processed successfully, 0 failed" in result_run2.stdout

    # 6. Run with the malformed file
    result_run3 = runner.invoke(app, [
        "run",
        str(malformed_xml_path),
        "--table-name", "icsr_master",
        "--config", str(config_path)
    ])
    assert result_run3.exit_code == 0 # The CLI itself should not crash
    assert "Failed to process file" in result_run3.stdout
    assert "0 files processed successfully, 1 failed" in result_run3.stdout

    # 7. Verify the database state after the failed run
    with db_engine.connect() as conn:
        failed_count = conn.execute(text("SELECT COUNT(*) FROM etl_file_history WHERE status = 'failed'")).scalar_one()
        assert failed_count == 1
        # No new data should have been loaded
        data_count = conn.execute(text("SELECT COUNT(*) FROM icsr_master")).scalar_one()
        assert data_count == 1
