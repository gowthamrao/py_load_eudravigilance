import io

import pandas as pd
import pytest
import sqlalchemy
from py_load_eudravigilance.postgres import PostgresLoader
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:13") as container:
        yield container


@pytest.fixture(scope="module")
def loader(postgres_container):
    engine = postgres_container.get_connection_url()
    return PostgresLoader({"dsn": engine})


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
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

    # 3. Load data
    with loader.connect() as connection:
        loader.load_dataframe(df, "test_data", connection)

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
