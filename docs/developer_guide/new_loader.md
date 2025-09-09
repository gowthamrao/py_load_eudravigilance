# Creating a New Loader

The package is designed to be extensible, allowing you to add support for new databases without modifying the core code. This is achieved through a plugin system.

To create a new loader (e.g., for Redshift), you need to follow two main steps:

1.  Implement the `LoaderInterface`.
2.  Register your new loader as a plugin.

## 1. Implement the `LoaderInterface`

Create a new Python class that inherits from `py_load_eudravigilance.interfaces.LoaderInterface` and implements all of its abstract methods.

Here is a skeleton for a hypothetical `RedshiftLoader`:

```python
# in my_redshift_loader/loader.py

from py_load_eudravigilance.interfaces import LoaderInterface
from io import IOBase
from typing import Any, Dict, List
from sqlalchemy.engine import Connection

class RedshiftLoader(LoaderInterface):
    """
    Loader implementation for Amazon Redshift.
    """
    def __init__(self, dsn_or_engine: Any):
        # Initialize your engine, connection details, etc.
        # e.g., self.engine = sqlalchemy.create_engine(dsn_or_engine)
        pass

    def validate_schema(self, schema_definition: Dict[str, Any]) -> bool:
        # Implement logic to validate the schema in Redshift.
        raise NotImplementedError

    def prepare_load(self, conn: Connection, target_table: str, load_mode: str) -> str:
        # Implement logic for 'full' (TRUNCATE) and 'delta' (create staging table) modes.
        raise NotImplementedError

    def bulk_load_native(self, conn: Connection, data_stream: IOBase, target_table: str, columns: List[str]) -> None:
        # This is the most critical method for performance.
        # For Redshift, this would likely use the `COPY` command from an S3 bucket.
        # You would need to:
        # 1. Upload the data_stream to a temporary S3 location.
        # 2. Execute a `COPY my_table FROM 's3://.../temp_file.csv' ...` command.
        # 3. Clean up the temporary S3 file.
        raise NotImplementedError

    def handle_upsert(self, conn: Connection, staging_table: str, target_table: str, primary_keys: List[str], version_key: str | None) -> None:
        # Implement Redshift's version of MERGE/UPSERT.
        # This typically involves a `DELETE` from the target table where keys
        # exist in the staging table, followed by an `INSERT` from staging.
        # `DELETE FROM target USING staging WHERE target.id = staging.id;`
        # `INSERT INTO target SELECT * FROM staging;`
        raise NotImplementedError

    def create_all_tables(self) -> None:
        # Use self.engine to create all tables defined in the schema.
        raise NotImplementedError

    def get_completed_file_hashes(self) -> set[str]:
        # Query your `etl_file_history` table to get hashes of completed files.
        raise NotImplementedError

    def load_normalized_data(self, buffers, row_counts, load_mode, file_path, file_hash) -> None:
        # This method orchestrates the load. The default implementation in the
        # interface might be a good starting point, or you can customize it.
        # It should wrap the prepare, bulk_load, and handle_upsert calls in a transaction.
        raise NotImplementedError

    def load_audit_data(self, buffer, row_count, load_mode, file_path, file_hash) -> None:
        # Similar to the above, but for the audit table.
        raise NotImplementedError
```

## 2. Register the Loader as a Plugin

The application uses Python's `entry_points` mechanism to discover available loaders. To make your new loader available, you need to register it in your package's `pyproject.toml` (or `setup.py`).

The entry point group is `py_load_eudravigilance.loaders`. The name of the entry point should match the **SQLAlchemy dialect name** for your database. For Redshift, the dialect is `redshift`.

```toml
[tool.poetry.plugins."py_load_eudravigilance.loaders"]
"redshift" = "my_redshift_loader.loader:RedshiftLoader"
```

When a user provides a DSN like `redshift+psycopg2://...`, the application will:
1.  Parse the DSN and identify the `redshift` dialect.
2.  Look for a registered plugin named `redshift`.
3.  Find your entry point and load the `RedshiftLoader` class.
4.  Instantiate and use your loader for the ETL process.
