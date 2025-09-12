from io import StringIO
import io

import pytest
import sqlalchemy
from py_load_eudravigilance.loader import PostgresLoader
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:13") as container:
        yield container


@pytest.fixture(scope="module")
def loader(postgres_container):
    engine = postgres_container.get_connection_url()
    return PostgresLoader(engine)


def test_connection(loader):
    """Tests that the loader is initialized with an engine."""
    assert loader.engine is not None


def test_create_and_load_data(loader):
    # 1. Create a test table
    metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        "test_data",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("name", sqlalchemy.String(50)),
    )
    metadata.create_all(loader.engine)

    # 2. Prepare data
    csv_data = "id,name\n1,Alice\n2,Bob\n"
    data_stream = StringIO(csv_data)
    columns = ["id", "name"]

    # 3. Load data using a transaction
    with loader.engine.begin() as conn:
        loader.bulk_load_native(conn, data_stream, "test_data", columns)

    # 4. Verify data
    with loader.engine.connect() as connection:
        result = connection.execute(
            sqlalchemy.text("SELECT * FROM test_data ORDER BY id;")
        ).fetchall()
        assert len(result) == 2
        assert result[0][0] == 1
        assert result[0][1] == "Alice"
        assert result[1][0] == 2
        assert result[1][1] == "Bob"


def test_dynamic_upsert(loader):
    # 1. Define a table with a composite PK and a version key comment
    metadata = sqlalchemy.MetaData()
    table_name = "upsert_test_table"
    sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column("pk1", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("pk2", sqlalchemy.String(10), primary_key=True),
        sqlalchemy.Column("data", sqlalchemy.String(50)),
        sqlalchemy.Column("version", sqlalchemy.Integer, comment="VERSION_KEY"),
    )
    metadata.create_all(loader.engine)

    # 2. Pre-populate the table with initial data
    test_table = metadata.tables[table_name]
    with loader.engine.begin() as conn:
        conn.execute(
            test_table.insert(),
            [
                {"pk1": 1, "pk2": "a", "data": "initial_A", "version": 1},
                {"pk1": 1, "pk2": "b", "data": "initial_B", "version": 1},
            ],
        )

    # 3. Use the loader's methods to create a staging table and load data
    with loader.engine.begin() as conn:
        staging_table_name = loader.prepare_load(conn, table_name, load_mode="delta")

        # The data needs to be in a file-like object (CSV)
        csv_data = (
            "pk1,pk2,data,version\n"
            "1,a,updated_A,2\n"
            "1,b,stale_B,0\n"
            "2,c,new_C,1\n"
        )
        data_stream = StringIO(csv_data)
        columns = ["pk1", "pk2", "data", "version"]
        loader.bulk_load_native(conn, data_stream, staging_table_name, columns)

        # 4. Run the upsert logic
        table_meta = loader._get_table_metadata(table_name)
        assert table_meta["pk"] == ["pk1", "pk2"]
        assert table_meta["version_key"] == "version"

        loader.handle_upsert(
            conn,
            staging_table=staging_table_name,
            target_table=table_name,
            primary_keys=table_meta["pk"],
            version_key=table_meta["version_key"],
        )

    # 5. Verify the final state of the table
    test_table = metadata.tables[table_name]
    with loader.engine.connect() as connection:
        result = connection.execute(
            sqlalchemy.select(test_table).order_by("pk1", "pk2")
        ).fetchall()

        assert len(result) == 3

        # Convert rows to dicts for easier validation
        result_dicts = [row._asdict() for row in result]

        assert {"pk1": 1, "pk2": "a", "data": "updated_A", "version": 2} in result_dicts
        assert {"pk1": 1, "pk2": "b", "data": "initial_B", "version": 1} in result_dicts
        assert {"pk1": 2, "pk2": "c", "data": "new_C", "version": 1} in result_dicts


def test_full_load_truncates_data(loader):
    """
    Tests that running a normalized load in 'full' mode correctly
    truncates existing data before loading new data.
    """
    # 1. Ensure the required schemas from the application are created
    loader.create_all_tables()

    # 2. Prepare mock data for two tables
    master_csv = (
        "safetyreportid,receiptdate,is_nullified,senderidentifier,receiveridentifier\n"
        "test-id-1,2024-01-01,False,sender,receiver\n"
    )
    patient_csv = "safetyreportid,patientinitials\ntest-id-1,AB\n"

    buffers = {
        "icsr_master": StringIO(master_csv),
        "patient_characteristics": StringIO(patient_csv),
    }
    row_counts = {"icsr_master": 1, "patient_characteristics": 1}

    # 3. Perform the first full load
    loader.load_normalized_data(
        buffers=buffers,
        row_counts=row_counts,
        load_mode="full",
        file_path="/fake/path/1",
        file_hash="hash1",
    )

    # 4. Verify the initial load
    with loader.engine.connect() as conn:
        count = conn.execute(
            sqlalchemy.text("SELECT COUNT(*) FROM icsr_master")
        ).scalar_one()
        assert count == 1

    # 5. Reset buffers and perform a second full load with the same data
    buffers["icsr_master"].seek(0)
    buffers["patient_characteristics"].seek(0)

    loader.load_normalized_data(
        buffers=buffers,
        row_counts=row_counts,
        load_mode="full",
        file_path="/fake/path/2",  # Different path to simulate a re-run
        file_hash="hash2",
    )

    # 6. Verify that the table was truncated and now contains only the new load
    with loader.engine.connect() as conn:
        count = conn.execute(
            sqlalchemy.text("SELECT COUNT(*) FROM icsr_master")
        ).scalar_one()
        # If TRUNCATE worked, the count should still be 1, not 2.
        assert count == 1


def test_icsr_amendment_update(loader):
    """
    Tests that the delta load logic correctly handles ICSR amendments
    by updating existing records based on the version key. This is a
    critical test for the bug fix.
    """
    # 1. Ensure all application tables are created
    loader.create_all_tables()

    # 2. Define the initial version of an ICSR
    report_id = "TEST-AMEND-01"
    initial_csv = (
        "safetyreportid,date_of_most_recent_info,receiptdate,is_nullified,senderidentifier\n"
        f"{report_id},20250101,20250101,False,InitialSender\n"
    )
    initial_buffers = {"icsr_master": StringIO(initial_csv)}
    initial_counts = {"icsr_master": 1}

    # 3. Load the initial version
    loader.load_normalized_data(
        buffers=initial_buffers,
        row_counts=initial_counts,
        load_mode="delta",
        file_path="/fake/amend/1",
        file_hash="hash_amend_1",
    )

    # 4. Verify the initial state
    with loader.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).first()
        assert result is not None
        assert result._asdict()["senderidentifier"] == "InitialSender"
        assert result._asdict()["date_of_most_recent_info"] == "20250101"

    # 5. Define the amended (newer) version of the same ICSR
    amended_csv = (
        "safetyreportid,date_of_most_recent_info,receiptdate,is_nullified,senderidentifier\n"
        f"{report_id},20250102,20250101,False,UpdatedSender\n"  # Newer date, new sender
    )
    amended_buffers = {"icsr_master": StringIO(amended_csv)}
    amended_counts = {"icsr_master": 1}

    # 6. Load the amended version
    loader.load_normalized_data(
        buffers=amended_buffers,
        row_counts=amended_counts,
        load_mode="delta",
        file_path="/fake/amend/2",
        file_hash="hash_amend_2",
    )

    # 7. Verify the final state
    with loader.engine.connect() as conn:
        # Check that there is still only one record
        count = conn.execute(
            sqlalchemy.text(
                f"SELECT COUNT(*) FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).scalar_one()
        assert count == 1

        # Check that the record was updated
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).first()
        assert result is not None
        assert result._asdict()["senderidentifier"] == "UpdatedSender"
        assert result._asdict()["date_of_most_recent_info"] == "20250102"


def test_icsr_nullification(loader):
    """
    Tests that the delta load logic correctly handles ICSR nullifications.
    """
    # 1. Ensure all application tables are created
    loader.create_all_tables()

    # 2. Define the initial version of an ICSR
    report_id = "TEST-NULLIFY-01"
    initial_csv = (
        "safetyreportid,date_of_most_recent_info,receiptdate,is_nullified,senderidentifier\n"
        f"{report_id},20250201,20250201,False,OriginalSender\n"
    )
    initial_buffers = {"icsr_master": StringIO(initial_csv)}
    initial_counts = {"icsr_master": 1}

    # 3. Load the initial version
    loader.load_normalized_data(
        buffers=initial_buffers,
        row_counts=initial_counts,
        load_mode="delta",
        file_path="/fake/nullify/1",
        file_hash="hash_nullify_1",
    )

    # 4. Verify the initial state
    with loader.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).first()
        assert result is not None
        assert result._asdict()["is_nullified"] is False
        assert result._asdict()["senderidentifier"] == "OriginalSender"

    # 5. Define the nullification record (newer date, is_nullified=True)
    # Note: The senderidentifier is missing here to simulate a sparse record.
    # The loader logic should preserve the original senderidentifier.
    nullify_csv = (
        "safetyreportid,date_of_most_recent_info,receiptdate,is_nullified\n"
        f"{report_id},20250202,20250201,True\n"
    )
    nullify_buffers = {"icsr_master": StringIO(nullify_csv)}
    nullify_counts = {"icsr_master": 1}

    # 6. Load the nullification record
    loader.load_normalized_data(
        buffers=nullify_buffers,
        row_counts=nullify_counts,
        load_mode="delta",
        file_path="/fake/nullify/2",
        file_hash="hash_nullify_2",
    )

    # 7. Verify the final state
    with loader.engine.connect() as conn:
        # Check that there is still only one record
        count = conn.execute(
            sqlalchemy.text(
                f"SELECT COUNT(*) FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).scalar_one()
        assert count == 1

        # Check that the record was correctly nullified
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).first()
        assert result is not None
        # The primary assertion: the flag is now True
        assert result._asdict()["is_nullified"] is True
        # The secondary assertion: original data was preserved
        assert result._asdict()["senderidentifier"] == "OriginalSender"
        assert result._asdict()["date_of_most_recent_info"] == "20250202"


def test_upsert_with_server_default(loader):
    """
    Tests that the upsert logic correctly ignores columns with server-side
    defaults, preventing them from being included in the SET clause.
    """
    metadata = sqlalchemy.MetaData()
    table_name = "server_default_test"
    sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("data", sqlalchemy.String(50)),
        sqlalchemy.Column(
            "last_updated",
            sqlalchemy.DateTime,
            server_default=sqlalchemy.func.now(),
        ),
    )
    metadata.create_all(loader.engine)

    # Insert initial data
    with loader.engine.begin() as conn:
        conn.execute(
            sqlalchemy.text(f"INSERT INTO {table_name} (id, data) VALUES (1, 'initial')")
        )

    # Prepare for upsert
    with loader.engine.begin() as conn:
        staging_table_name = loader.prepare_load(conn, table_name, load_mode="delta")
        csv_data = "id,data\n1,updated\n"
        data_stream = io.StringIO(csv_data)
        loader.bulk_load_native(conn, data_stream, staging_table_name, ["id", "data"])

        # This is the key part of the test: handle_upsert should not fail
        # and should not try to update the 'last_updated' column.
        loader.handle_upsert(
            conn,
            staging_table=staging_table_name,
            target_table=table_name,
            primary_keys=["id"],
            version_key=None,
        )

    # Verify that the data was updated
    with loader.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(f"SELECT data FROM {table_name} WHERE id = 1")
        ).scalar_one()
        assert result == "updated"


def test_drugs_table_loading(loader):
    """
    Tests that data is correctly loaded into the `drugs` table,
    specifically verifying the `drugdosagetext` field.
    """
    # 1. Ensure all application tables are created
    loader.create_all_tables()
    report_id = "TEST-DRUG-01"

    # We need to load a master record first due to foreign key constraints
    master_csv = (
        "safetyreportid,date_of_most_recent_info,is_nullified\n"
        f"{report_id},20250301,False\n"
    )
    # 2. Define the drug data, including the field in question
    drug_csv = (
        "safetyreportid,drug_seq,medicinalproduct,drugdosagetext\n"
        f'{report_id},1,Test-Drug-A,"Take one tablet daily"\n'
        f'{report_id},2,Test-Drug-B,"Two pills, twice a day"\n'
    )

    buffers = {
        "icsr_master": StringIO(master_csv),
        "drugs": StringIO(drug_csv),
    }
    counts = {"icsr_master": 1, "drugs": 2}

    # 3. Load the data
    loader.load_normalized_data(
        buffers=buffers,
        row_counts=counts,
        load_mode="delta",
        file_path="/fake/drug/1",
        file_hash="hash_drug_1",
    )

    # 4. Verify the data in the drugs table
    with loader.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM drugs WHERE safetyreportid = '{report_id}' "
                "ORDER BY drug_seq"
            )
        ).fetchall()

        assert len(result) == 2

        # Check the first drug record
        drug1 = result[0]._asdict()
        assert drug1["medicinalproduct"] == "Test-Drug-A"
        assert drug1["drugdosagetext"] == "Take one tablet daily"

        # Check the second drug record
        drug2 = result[1]._asdict()
        assert drug2["medicinalproduct"] == "Test-Drug-B"
        assert drug2["drugdosagetext"] == "Two pills, twice a day"


def test_stale_icsr_nullification_is_processed_correctly(loader):
    """
    Tests that a "stale" nullification (a nullification message with an
    older version date than the existing record) correctly nullifies the
    record without overwriting newer data.
    """
    # 1. Ensure all application tables are created
    loader.create_all_tables()

    # 2. Define and load the initial, newer version of an ICSR
    report_id = "TEST-STALE-NULL-01"
    initial_csv = (
        "safetyreportid,date_of_most_recent_info,receiptdate,is_nullified,senderidentifier\n"
        f"{report_id},20250302,20250301,False,NewerSender\n"
    )
    initial_buffers = {"icsr_master": StringIO(initial_csv)}
    initial_counts = {"icsr_master": 1}

    loader.load_normalized_data(
        buffers=initial_buffers,
        row_counts=initial_counts,
        load_mode="delta",
        file_path="/fake/stale_null/1",
        file_hash="hash_stale_null_1",
    )

    # 3. Verify the initial state
    with loader.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).first()
        assert result is not None
        assert result._asdict()["is_nullified"] is False
        assert result._asdict()["senderidentifier"] == "NewerSender"
        assert result._asdict()["date_of_most_recent_info"] == "20250302"

    # 4. Define the stale nullification record (older date, is_nullified=True)
    stale_nullify_csv = (
        "safetyreportid,date_of_most_recent_info,receiptdate,is_nullified,senderidentifier\n"
        f"{report_id},20250301,20250301,True,OlderSender\n"
    )
    stale_nullify_buffers = {"icsr_master": StringIO(stale_nullify_csv)}
    stale_nullify_counts = {"icsr_master": 1}

    # 5. Load the stale nullification record
    loader.load_normalized_data(
        buffers=stale_nullify_buffers,
        row_counts=stale_nullify_counts,
        load_mode="delta",
        file_path="/fake/stale_null/2",
        file_hash="hash_stale_null_2",
    )

    # 6. Verify the final state
    with loader.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                f"SELECT * FROM icsr_master WHERE safetyreportid = '{report_id}'"
            )
        ).first()
        assert result is not None
        # The record should be nullified.
        assert result._asdict()["is_nullified"] is True
        # The version key should NOT have been updated to the older date.
        assert result._asdict()["date_of_most_recent_info"] == "20250302"
        # Other data should NOT have been overwritten by the stale record.
        assert result._asdict()["senderidentifier"] == "NewerSender"
