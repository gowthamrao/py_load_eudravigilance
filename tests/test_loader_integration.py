import pytest
from testcontainers.postgres import PostgresContainer
from py_load_eudravigilance.loader import PostgresLoader
from io import StringIO
import sqlalchemy

@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:13") as container:
        yield container

@pytest.fixture(scope="module")
def loader(postgres_container):
    engine = postgres_container.get_connection_url()
    return PostgresLoader(engine)

def test_connection(loader):
    assert loader.engine is not None
    connection = loader.connect()
    assert connection is not None
    assert not connection.closed
    connection.close()

def test_create_and_load_data(loader):
    # 1. Create a test table
    metadata = sqlalchemy.MetaData()
    test_table = sqlalchemy.Table(
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

    # 3. Load data
    loader.bulk_load_native(data_stream, "test_data", columns)
    loader.manage_transaction("COMMIT") # Manually commit for the test

    # 4. Verify data
    with loader.engine.connect() as connection:
        result = connection.execute(sqlalchemy.text("SELECT * FROM test_data ORDER BY id;")).fetchall()
        assert len(result) == 2
        assert result[0][0] == 1
        assert result[0][1] == "Alice"
        assert result[1][0] == 2
        assert result[1][1] == "Bob"


def test_dynamic_upsert(loader):
    # 1. Define a table with a composite PK and a version key comment
    metadata = sqlalchemy.MetaData()
    table_name = "upsert_test_table"
    test_table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column("pk1", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("pk2", sqlalchemy.String(10), primary_key=True),
        sqlalchemy.Column("data", sqlalchemy.String(50)),
        sqlalchemy.Column(
            "version", sqlalchemy.Integer, comment="VERSION_KEY"
        ),
    )
    metadata.create_all(loader.engine)

    # 2. Pre-populate the table with initial data
    with loader.engine.begin() as conn:
        conn.execute(test_table.insert(), [
            {"pk1": 1, "pk2": "a", "data": "initial_A", "version": 1},
            {"pk1": 1, "pk2": "b", "data": "initial_B", "version": 1},
        ])

    # 3. Use the loader's methods to create a staging table and load data
    loader.manage_transaction("BEGIN")
    staging_table_name = loader.prepare_load(table_name, load_mode="delta")

    # The data needs to be in a file-like object (CSV)
    csv_data = (
        "pk1,pk2,data,version\n"
        "1,a,updated_A,2\n"
        "1,b,stale_B,0\n"
        "2,c,new_C,1\n"
    )
    data_stream = StringIO(csv_data)
    columns = ["pk1", "pk2", "data", "version"]
    loader.bulk_load_native(data_stream, staging_table_name, columns)

    # 4. Run the upsert logic
    table_meta = loader._get_table_metadata(table_name)
    assert table_meta["pk"] == ["pk1", "pk2"]
    assert table_meta["version_key"] == "version"

    loader.handle_upsert(
        staging_table=staging_table_name,
        target_table=table_name,
        primary_keys=table_meta["pk"],
        version_key=table_meta["version_key"],
    )
    loader.manage_transaction("COMMIT")

    # 5. Verify the final state of the table
    with loader.engine.connect() as connection:
        result = connection.execute(sqlalchemy.select(test_table).order_by("pk1", "pk2")).fetchall()

        assert len(result) == 3

        # Convert rows to dicts for easier validation
        result_dicts = [row._asdict() for row in result]

        assert {"pk1": 1, "pk2": "a", "data": "updated_A", "version": 2} in result_dicts
        assert {"pk1": 1, "pk2": "b", "data": "initial_B", "version": 1} in result_dicts
        assert {"pk1": 2, "pk2": "c", "data": "new_C", "version": 1} in result_dicts
