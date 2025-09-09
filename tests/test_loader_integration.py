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
