# FRD Compliance Analysis: py-load-eudravigilance

## 1. Introduction and Scope

### 1.1 Purpose and Objectives

#### Requirement: Performance
To process large volumes (millions) of ICSRs efficiently by utilizing the native bulk loading mechanisms of the target database.

**Status:** Met

**Analysis:**
The software uses the `COPY FROM STDIN` command for PostgreSQL, which is the most performant method for bulk data insertion. This is implemented in the `PostgresLoader.bulk_load_native` method.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
```python
def bulk_load_native(
    self,
    conn: sqlalchemy.Connection,
    data_stream: IOBase,
    target_table: str,
    columns: List[str],
) -> None:
    """
    Loads data from an in-memory buffer into a PostgreSQL table using COPY.
    This method does NOT commit the transaction.
    """
    column_sql = ""
    if columns:
        column_sql = f"({', '.join(columns)})"

    sql = f"COPY {target_table} {column_sql} FROM STDIN WITH CSV HEADER"

    # Get the raw psycopg2 connection from the SQLAlchemy connection
    raw_conn = conn.connection
    with raw_conn.cursor() as cursor:
        cursor.copy_expert(sql, data_stream)

    print(f"Successfully loaded data into '{target_table}'.")
```

#### Requirement: Accuracy
To accurately parse and map the E2B(R3) standard, ensuring data fidelity and correctly handling ICSR versioning.

**Status:** Partially Met

**Analysis:**
The software uses `lxml.etree.iterparse` to parse E2B(R3) XML files, which is accurate for the implemented fields. ICSR versioning is handled during the `UPSERT` operation by comparing the `date_of_most_recent_info` (C.1.5) field. However, the schema mapping is not exhaustive, and some fields from the E2B(R3) standard might be missing. The current implementation covers the key fields mentioned in the FRD.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
The `handle_upsert` method includes a `WHERE` clause to only update if the incoming record is newer.
```python
            where_clause = None
            if version_key:
                # The WHERE clause should apply if the new record is newer,
                # OR if the new record is a nullification instruction.
                where_clause = target_table_obj.c[version_key] < excluded[version_key]
                if has_nullified_col:
                    where_clause = sqlalchemy.or_(where_clause, excluded.is_nullified)

            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=primary_keys,
                set_=update_dict,
                where=where_clause,
            )
```

#### Requirement: Extensibility
To provide a cloud-agnostic and database-agnostic architecture that facilitates easy integration with various data warehouses.

**Status:** Met

**Analysis:**
The architecture is highly extensible.
1.  **Cloud-Agnostic File Access:** It uses the `fsspec` library to read from local files, S3, GCS, and Azure Blob Storage, abstracting the file system.
2.  **Database-Agnostic Loading:** It uses a `LoaderInterface` and a plugin system based on Python's `entry_points`. This allows new database loaders to be added without modifying the core codebase.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
The `get_loader` factory function discovers and instantiates the correct loader based on the DSN, using `importlib.metadata.entry_points`.
```python
def get_loader(dsn_or_engine: Any) -> LoaderInterface:
    # ...
    # Discover registered loader plugins
    try:
        loaders = metadata.entry_points(group="py_load_eudravigilance.loaders")
    except Exception as e:
        # ...
    # Find a matching loader by its registered name
    for ep in loaders:
        if ep.name == dialect_name:
            loader_class = ep.load()
            # ...
```
**Configuration (`pyproject.toml`):**
The entry point is registered in `pyproject.toml`.
```toml
[tool.poetry.plugins."py_load_eudravigilance.loaders"]
"postgresql" = "py_load_eudravigilance.loader:PostgresLoader"
```

#### Requirement: Robustness
To ensure data integrity through transactional processing and idempotency.

**Status:** Met

**Analysis:**
1.  **Transactional Processing:** The entire load process for a single file, including bulk loading into staging tables and the final `UPSERT`, is wrapped in a single database transaction. If any step fails, the transaction is rolled back. This is managed by the `with self.engine.begin() as conn:` blocks in `loader.py`.
2.  **Idempotency:** Idempotency is achieved through two mechanisms:
    *   **File-level:** The `etl_file_history` table tracks successfully processed files by their hash. The ETL job skips files that are already marked as 'completed'.
    *   **Record-level:** The `UPSERT` (`INSERT ... ON CONFLICT DO UPDATE`) logic ensures that re-processing the same ICSR does not create duplicate records.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
The `load_normalized_data` method demonstrates the transactional block.
```python
        try:
            with self.engine.begin() as conn:
                # ... all loading and upserting happens here ...
                self._log_file_status(
                    conn, file_path, file_hash, "completed", total_rows
                )
        except Exception as e:
            print(f"Error during normalized load for file {file_path}. Rolling back.")
            # ... log failure status ...
            raise e
```

## 2. Architectural Design and Principles

### 2.1 High-Level Architecture

**Requirement:** The architecture shall be modular, adhering to the separation of concerns principle, divided into three main components: Parser Core, Transformation Layer, and Loader Interface.

**Status:** Met

**Analysis:**
The codebase is clearly divided into three main components, reflecting the specified architecture:
-   **Parser Core (`src/py_load_eudravigilance/parser.py`):** Handles XML parsing.
-   **Transformation Layer (`src/py_load_eudravigilance/transformer.py`):** Transforms parsed data into relational format.
-   **Loader Interface (`src/py_load_eudravigilance/interfaces.py` and `src/py_load_eudravigilance/loader.py`):** Defines the loading interface and provides a concrete implementation for PostgreSQL.

This separation makes the codebase modular and easy to maintain.

### 2.2 Database Abstraction Strategy

**Requirement:** Utilize the Strategy Pattern to abstract the data loading implementation. An ABC named `LoaderInterface` shall be defined.

**Status:** Met

**Analysis:**
The `LoaderInterface` is defined as an Abstract Base Class (ABC) in `src/py_load_eudravigilance/interfaces.py`. The `PostgresLoader` in `src/py_load_eudravigilance/loader.py` is a concrete implementation of this interface, which is a clear application of the Strategy Pattern.

**Code Example (`src/py_load_eudravigilance/interfaces.py`):**
```python
class LoaderInterface(ABC):
    """
    Abstract Base Class for database loaders.
    """

    @abstractmethod
    def validate_schema(self, schema_definition: Dict[str, Any]) -> bool:
        raise NotImplementedError

    @abstractmethod
    def bulk_load_native(
        self,
        conn: Connection,
        data_stream: IOBase,
        target_table: str,
        columns: List[str],
    ) -> None:
        raise NotImplementedError

    # ... other abstract methods
```

#### 2.2.2 Use of SQLAlchemy Core

**Requirement:** SQLAlchemy Core shall be utilized for database-agnostic operations like DDL and metadata management.

**Status:** Met

**Analysis:**
The project uses SQLAlchemy Core for several database-agnostic tasks:
-   **Schema Definition:** All tables are defined in `src/py_load_eudravigilance/schema.py` using SQLAlchemy's declarative syntax.
-   **DDL Creation:** The `create_all_tables` method uses `metadata.create_all(self.engine)` to generate and execute the DDL.
-   **Complex Queries:** The `handle_upsert` method uses SQLAlchemy Core's `insert` and `on_conflict_do_update` constructs to build the complex `UPSERT` statement in a dialect-agnostic way (for PostgreSQL).

**Code Example (`src/py_load_eudravigilance/schema.py`):**
```python
import sqlalchemy

metadata = sqlalchemy.MetaData()

icsr_master = sqlalchemy.Table(
    "icsr_master",
    metadata,
    sqlalchemy.Column("safetyreportid", sqlalchemy.String(255), primary_key=True),
    # ... other columns
)
```

### 2.3 Extensibility

**Requirement:** The architecture must facilitate the easy addition of new database adapters, using a factory and a plugin system (`entry_points`).

**Status:** Met

**Analysis:**
The `get_loader` function in `src/py_load_eudravigilance/loader.py` acts as a factory. It discovers available loaders using `importlib.metadata.entry_points` with the group `py_load_eudravigilance.loaders`. This allows third-party packages to provide their own loaders without modifying the core package.

**Code Example (`pyproject.toml`):**
The plugin is registered in `pyproject.toml`, making it discoverable.
```toml
[tool.poetry.plugins."py_load_eudravigilance.loaders"]
"postgresql" = "py_load_eudravigilance.loader:PostgresLoader"
```

**Code Example (`src/py_load_eudravigilance/loader.py`):**
The factory function that loads the plugin.
```python
def get_loader(dsn_or_engine: Any) -> LoaderInterface:
    # ...
    loaders = metadata.entry_points(group="py_load_eudravigilance.loaders")
    for ep in loaders:
        if ep.name == dialect_name:
            loader_class = ep.load()
            return loader_class(dsn_or_engine)
    # ...
```

## 3. Data Extraction and Parsing (E)

### 3.1 Input Source Configuration

**Requirement:** The package must support reading input XML files from local file systems and cloud object storage (S3, GCS, Azure Blob) via URI configuration, using the `fsspec` ecosystem.

**Status:** Met

**Analysis:**
The `discover_files` function in `src/py_load_eudravigilance/run.py` uses `fsspec.open_files` to abstract file system access. This allows the use of various backends (local, S3, etc.) by simply providing the appropriate URI. The necessary `fsspec`-compatible libraries (`s3fs`, `gcsfs`, etc.) are included as optional dependencies.

**Code Example (`src/py_load_eudravigilance/run.py`):**
```python
import fsspec

def discover_files(uri: str) -> list[str]:
    """
    Discovers files from a given URI, supporting local paths, glob patterns,
    and cloud storage URIs.
    """
    # ...
    try:
        file_objects = fsspec.open_files(uri)
        return [f.path for f in file_objects]
    # ...
```

### 3.2 XML Parsing Strategy

**Requirement:** Iterative, stream-based parsing is mandatory. DOM-based parsing is strictly prohibited. The chosen approach must correctly handle XML namespaces.

**Status:** Met

**Analysis:**
The software uses `lxml.etree.iterparse` in `src/py_load_eudravigilance/parser.py`, which is a memory-efficient, stream-based parser. It processes the XML file element by element, avoiding loading the entire document into memory. It also correctly defines and uses the `urn:hl7-org:v3` namespace to find elements.

**Code Example (`src/py_load_eudravigilance/parser.py`):**
```python
from lxml import etree

NAMESPACES = {"hl7": "urn:hl7-org:v3"}

def parse_icsr_xml(
    xml_source: IO[bytes],
) -> Generator[Dict[str, Any] | InvalidICSRError, None, None]:
    """
    Parses an ICH E2B(R3) XML file and yields individual ICSRs.
    """
    context = etree.iterparse(
        xml_source, events=("end",), tag=f"{{{NAMESPACES['hl7']}}}ichicsrMessage"
    )

    for _, elem in context:
        # ... processing logic ...
        # Crucial for memory efficiency: clear the element from memory.
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
```

### 3.2.3 Validation

**Requirement:** The package shall provide an optional feature to validate input XML files against the official ICH E2B(R3) XSD schemas.

**Status:** Met

**Analysis:**
The CLI provides a `--validate` flag for the `run` command and a dedicated `validate` command. The implementation in `parser.py` uses a streaming validation approach with `lxml.etree.XMLParser(schema=xmlschema)`, which is efficient for large files.

**Code Example (`src/py_load_eudravigilance/parser.py`):**
```python
def validate_xml_with_xsd(
    xml_source: IO[bytes], xsd_path: str
) -> Tuple[bool, List[str]]:
    """
    Validates an XML source against a given XSD schema file using a streaming
    parser to ensure low memory usage.
    """
    try:
        xmlschema_doc = etree.parse(xsd_path)
        xmlschema = etree.XMLSchema(xmlschema_doc)
        parser = etree.XMLParser(schema=xmlschema)

        for chunk in iter(lambda: xml_source.read(16384), b""):
            parser.feed(chunk)
        # ...
```

### 3.3 Error Handling during Parsing

**Requirement:** Files that fail parsing must be logged and moved to a quarantine location. The process must continue with valid files. If an individual ICSR within a batch file is invalid, the valid ICSRs should be processed.

**Status:** Partially Met

**Analysis:**
-   **File-level Quarantine:** The `process_file` function in `run.py` has a `try...except` block that calls a `_quarantine_file` function upon any failure during processing. This correctly moves the entire failed file.
-   **Intra-file Errors:** The `parse_icsr_xml` function is designed to handle this. It yields `InvalidICSRError` objects for invalid ICSRs and continues parsing. The `transform_and_normalize` function correctly separates these errors from valid data. However, the quarantine mechanism for these *individual* failed ICSRs is noted as a "Future enhancement" and not fully implemented.

**Code Example (`src/py_load_eudravigilance/transformer.py`):**
```python
def transform_and_normalize(
    icsr_generator: Generator[Dict[str, Any] | InvalidICSRError, None, None]
) -> Tuple[Dict[str, io.StringIO], Dict[str, int], List[InvalidICSRError]]:
    # ...
    parsing_errors = []
    for item in icsr_generator:
        if isinstance(item, InvalidICSRError):
            parsing_errors.append(item)
            continue
        # ... process valid item ...
    return buffers, row_counts, parsing_errors
```

### 3.4 Parallelization

**Requirement:** The extraction and parsing phase must utilize multiprocessing to parse multiple input files simultaneously.

**Status:** Met

**Analysis:**
The `run_etl` function in `run.py` uses a `concurrent.futures.ProcessPoolExecutor` to parallelize the processing of files. The `process_file` function, which contains the parsing and loading logic for a single file, is submitted as a task to the pool.

**Code Example (`src/py_load_eudravigilance/run.py`):**
```python
import concurrent.futures

def process_files_parallel(
    files_map: dict[str, str],
    # ...
):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(
                process_file, path, hash_val, settings, mode, validate
            ): path
            for path, hash_val in files_map.items()
        }
        # ...
```

## 4. Data Transformation and Structure (T)

### 4.1 Transformation Logic

**Requirement:** The transformation layer must convert the hierarchical E2B(R3) structure into a relational format, involving flattening, normalization, and data typing.

**Status:** Met

**Analysis:**
The `transform_and_normalize` function in `src/py_load_eudravigilance/transformer.py` is responsible for this. It takes the generator of parsed ICSR dictionaries and performs:
-   **Flattening:** Extracts nested elements (e.g., `primarysourcereaction` from a `reaction` element) into columns.
-   **Normalization:** Populates multiple in-memory CSV buffers corresponding to different tables (e.g., `icsr_master`, `reactions`, `drugs`), creating the one-to-many relationships.
-   **Data Typing:** While the intermediate format is text-based (CSV), the database schema in `schema.py` defines the correct data types, and the database handles the conversion during the `COPY` operation.

**Code Example (`src/py_load_eudravigilance/transformer.py`):**
This snippet shows how one-to-many relationships for reactions are handled.
```python
        # 2. Populate the reactions table (one-to-many)
        for reaction in icsr_dict.get("reactions", []):
            reaction["safetyreportid"] = safetyreportid
            writers["reactions"].writerow(reaction)
            row_counts["reactions"] += 1
```

### 4.2 Target Relational Data Models

**Requirement:** The package must support two distinct data representations: a normalized schema and a full/audit schema using JSONB.

**Status:** Met

**Analysis:**
-   **Normalized Schema:** The `schema.py` file defines a comprehensive set of normalized tables (`icsr_master`, `patient_characteristics`, etc.). The `transform_and_normalize` function populates these.
-   **Audit Schema:** `schema.py` also defines the `icsr_audit_log` table with a `JSONB` column. The `transform_for_audit` function prepares the data for this schema, and `parser.py` has a dedicated `parse_icsr_xml_for_audit` function to convert the XML structure into a dictionary suitable for JSON serialization.

**Code Example (`src/py_load_eudravigilance/schema.py`):**
```python
# Audit Schema Definition
icsr_audit_log = sqlalchemy.Table(
    "icsr_audit_log",
    metadata,
    sqlalchemy.Column("safetyreportid", sqlalchemy.String(255), primary_key=True),
    # ...
    sqlalchemy.Column("icsr_payload", JSONB),
    # ...
)
```

**Code Example (`src/py_load_eudravigilance/transformer.py`):**
```python
def transform_for_audit(
    icsr_generator: Generator[Dict[str, Any], None, None]
) -> Tuple[io.StringIO, int]:
    # ...
    # De-duplication logic
    # ...
    for safetyreportid, data in latest_icsrs.items():
        row = {
            # ...
            "icsr_payload": json.dumps(data["payload"]),
        }
        writer.writerow(row)
    # ...
```

### 4.3 Metadata Handling

**Requirement:** The ETL process must capture and store ETL metadata (load timestamp, filename, etc.) and E2B transmission metadata.

**Status:** Met

**Analysis:**
-   **ETL Metadata:** The `etl_file_history` table is defined in `schema.py` and used by the `PostgresLoader`'s `_log_file_status` method to record the filename, hash, status, timestamp, and row count for every file processed.
-   **E2B Transmission Metadata:** Key transmission fields like `senderidentifier` and `receiveridentifier` are included as columns in the `icsr_master` table.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
```python
    def _log_file_status(
        self,
        conn: sqlalchemy.Connection,
        filename: str,
        file_hash: str,
        status: str,
        rows_processed: int | None = None,
    ) -> None:
        history_table = self._get_table_obj(conn, "etl_file_history")
        stmt = pg_insert(history_table).values(
            filename=filename,
            file_hash=file_hash,
            status=status,
            rows_processed=rows_processed,
            load_timestamp=sqlalchemy.func.now(),
        )
        # ... on conflict do update ...
        conn.execute(update_stmt)
```

### 4.4 Intermediate Format

**Requirement:** The format between Transformation and Loading must be an in-memory buffer (e.g., `io.StringIO`) formatted as CSV.

**Status:** Met

**Analysis:**
The `transform_and_normalize` and `transform_for_audit` functions in `transformer.py` both use `io.StringIO` to create in-memory CSV buffers. These buffers are then passed directly to the `bulk_load_native` method of the loader, minimizing disk I/O.

**Code Example (`src/py_load_eudravigilance/transformer.py`):**
```python
import io
import csv

def transform_and_normalize(
    # ...
):
    # ...
    buffers = {table: io.StringIO() for table in schemas}
    writers = {
        table: csv.DictWriter(buffers[table], fieldnames=fields)
        for table, fields in schemas.items()
    }
    # ...
    # Rewind all buffers to be ready for reading
    for buffer in buffers.values():
        buffer.seek(0)

    return buffers, row_counts, parsing_errors
```

## 5. Data Loading (L)

### 5.1 Default Adapter: PostgreSQL

**Requirement:** The adapter shall use `psycopg2` and the `COPY FROM STDIN` command. Row-by-row `INSERT` statements are strictly prohibited.

**Status:** Met

**Analysis:**
The `PostgresLoader` in `src/py_load_eudravigilance/loader.py` uses `psycopg2` and specifically calls `cursor.copy_expert()` with the `COPY ... FROM STDIN` SQL command. This is the correct, high-performance approach for bulk loading into PostgreSQL.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
```python
    def bulk_load_native(
        # ...
    ) -> None:
        # ...
        sql = f"COPY {target_table} {column_sql} FROM STDIN WITH CSV HEADER"

        # Get the raw psycopg2 connection from the SQLAlchemy connection
        raw_conn = conn.connection
        with raw_conn.cursor() as cursor:
            cursor.copy_expert(sql, data_stream)
```

### 5.2 Loading Modes and Delta Strategy

**Requirement:** Implement Full Load (Truncate and Load) and Delta Load (Incremental) modes.

**Status:** Met

**Analysis:**
The CLI `run` command accepts a `--mode` flag (`full` or `delta`).
-   **Full Load:** In `full` mode, the `prepare_load` method in the `PostgresLoader` issues a `TRUNCATE TABLE` command.
-   **Delta Load:** In `delta` mode, the `run_etl` function first filters out already processed files using the `etl_file_history` table. For the remaining files, data is loaded into a temporary staging table, and then an `UPSERT` operation is performed.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
```python
    def prepare_load(
        self, conn: sqlalchemy.Connection, target_table: str, load_mode: str
    ) -> str:
        if load_mode == "full":
            conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {target_table} CASCADE;"))
            return target_table
        elif load_mode == "delta":
            staging_table_name = f"__staging_{target_table}"
            # ... create temporary staging table ...
            return staging_table_name
```

### 5.2.3 State Management

**Requirement:** A metadata table (`etl_file_history`) shall be maintained to track the status and checksum of every processed input file.

**Status:** Met

**Analysis:**
The `etl_file_history` table is defined in `schema.py`. The `PostgresLoader` has methods to `_log_file_status` and `get_completed_file_hashes`. The `run_etl` orchestrator uses these methods to determine the delta for an incremental load.

**Code Example (`src/py_load_eudravigilance/run.py`):**
```python
def filter_completed_files(files: list[str], settings: Settings) -> dict[str, str]:
    # ...
    db_loader = loader.get_loader(settings.database.dsn)
    completed_hashes = db_loader.get_completed_file_hashes()
    # ...
    for file_path in files:
        file_hash = _calculate_file_hash(file_path)
        if file_hash not in completed_hashes:
            files_to_process[file_path] = file_hash
    return files_to_process
```

### 5.2.4 ICSR Versioning and Updates

**Requirement:** The loading process must handle ICSR amendments (updates) and nullifications using an `UPSERT`/`MERGE` strategy. Uniqueness is based on "Safety Report Unique Identifier" (C.1.1) and versioning on "Date of Most Recent Information" (C.1.5).

**Status:** Met

**Analysis:**
The `handle_upsert` method in `PostgresLoader` implements this logic correctly for PostgreSQL.
-   It uses `INSERT ... ON CONFLICT DO UPDATE`.
-   The conflict target is the primary key, which includes `safetyreportid` (C.1.1).
-   The `DO UPDATE` clause has a `WHERE` condition that compares the existing record's `date_of_most_recent_info` with the incoming one, ensuring updates only happen for newer versions.
-   It also correctly handles nullifications by updating the `is_nullified` flag.

**Code Example (`src/py_load_eudravigilance/loader.py`):**
```python
def handle_upsert(
    # ...
    version_key: str | None,
) -> None:
    # ...
    insert_stmt = pg_insert(target_table_obj).from_select(...)
    # ...
    if version_key:
        where_clause = target_table_obj.c[version_key] < excluded[version_key]
        if has_nullified_col:
            where_clause = sqlalchemy.or_(where_clause, excluded.is_nullified)

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=primary_keys,
        set_=update_dict,
        where=where_clause,
    )
    conn.execute(upsert_stmt)
```

### 5.3 Transaction Management and Idempotency

**Requirement:** The loading of a batch must be wrapped in a single atomic transaction. The entire process must be idempotent.

**Status:** Met

**Analysis:**
-   **Atomicity:** The `load_normalized_data` and `load_audit_data` methods in `PostgresLoader` use a `with self.engine.begin() as conn:` block. This ensures that all operations within the block (logging status, bulk loading, upserting, updating status to 'completed') are part of a single transaction. If any step fails, the `with` block ensures the transaction is rolled back.
-   **Idempotency:** As described in 1.1 and 5.2.3, the combination of file-level hash checking and record-level `UPSERT` logic makes the pipeline idempotent. Rerunning it with the same input files will not change the state of the database.

## 6. Configuration, Execution, and NFRs

### 6.1 Configuration Management

**Requirement:** Configuration shall be managed using a hierarchical approach with YAML files and environment variables for overrides.

**Status:** Met

**Analysis:**
The `config.py` module uses the `pydantic-settings` library to achieve this. The `load_config` function first loads a base configuration from a YAML file (`config.yaml`) and then merges (overwrites) it with settings from environment variables. Environment variables are prefixed with `PY_LOAD_EUDRAVIGILANCE_` and use `__` as a nested delimiter (e.g., `PY_LOAD_EUDRAVIGILANCE_DATABASE__DSN`).

**Code Example (`src/py_load_eudravigilance/config.py`):**
```python
def load_config(path: Optional[str] = None) -> Settings:
    # ...
    # 1. Load base configuration from the YAML file.
    if Path(config_path).is_file():
        with open(config_path, "r") as f:
            file_config = yaml.safe_load(f) or {}

    # 2. Load settings from environment variables
    env_loader = _EnvSettings()
    env_config = env_loader.model_dump(exclude_unset=True)

    # 3. Merge the configurations.
    merged_config = deep_merge(source=env_config, destination=file_config)

    # 4. Create the final, validated settings object
    return Settings(**merged_config)
```

### 6.2 Execution Modes

**Requirement:** The package must be usable as a standalone CLI (using `Typer` or `Click`) and as an importable library.

**Status:** Met

**Analysis:**
-   **CLI:** The `cli.py` module implements a comprehensive CLI using `Typer`. It exposes commands like `run`, `init-db`, and `validate`, as required.
-   **Library:** The core logic is decoupled from the CLI. The `run_etl` function in `run.py` can be imported and called from other Python applications, allowing the package to be used as a library.

**Code Example (`src/py_load_eudravigilance/cli.py`):**
```python
import typer

app = typer.Typer()

@app.command()
def run(
    # ... options
):
    # ... loads config ...
    etl_run.run_etl(settings, mode=mode, max_workers=workers, validate=validate)

@app.command()
def init_db(
    # ...
):
    # ...
```

### 6.3 Performance

**Requirement:** The target benchmark is to parse and load at least 1,500 ICSRs per second. Memory usage must remain stable.

**Status:** Not Met

**Analysis:**
The architecture is designed for performance by using stream parsing and native bulk loading, which contributes to stable memory usage. However, the project lacks a dedicated performance testing suite. There are no tests to measure the throughput (ICSRs/sec) against a baseline dataset and hardware. Therefore, compliance with the specific benchmark cannot be verified. This is a gap.

### 6.4 Security

**Requirement:** Credentials must not be logged or stored in plain text. Connections must support encryption.

**Status:** Met

**Analysis:**
-   **Credentials:** The configuration system encourages the use of environment variables for sensitive data like the database DSN, which prevents credentials from being stored in version-controlled configuration files. The DSN string itself is a standard way to handle credentials.
-   **Encryption:** The use of standard database drivers like `psycopg2` means that connection encryption (SSL/TLS) is supported out-of-the-box. It can be enabled by including the appropriate parameters in the DSN (e.g., `sslmode=require`).

## 7. Package Design, Quality, and Maintenance

### 7.1 Development Standards

**Requirement:** Adherence to Python 3.10+, PEP 8 (Ruff/Flake8), Black, isort, and mandatory type hinting (MyPy).

**Status:** Met

**Analysis:**
The `pyproject.toml` file lists `ruff`, `black`, `isort`, and `mypy` as development dependencies. The configuration for these tools is also present in the `pyproject.toml` file, indicating that they are used to enforce coding standards. The code is well-formatted and includes type hints.

**Code Example (`pyproject.toml`):**
```toml
[tool.poetry.group.dev.dependencies]
pytest = "^7.4.2"
ruff = "^0.1.0"
black = "^23.9.1"
isort = "^5.12.0"
mypy = "^1.5.1"
# ...

[tool.ruff]
line-length = 88
select = ["E", "W", "F", "I", "C", "B"]

[tool.black]
line-length = 88

[tool.isort]
profile = "black"
```

### 7.2 Dependency Management

**Requirement:** Use `pyproject.toml` and `Poetry`. Database drivers and other optional components must be defined as optional extras.

**Status:** Met

**Analysis:**
The project uses a `pyproject.toml` file and `poetry.lock`, indicating the use of Poetry for dependency management. Optional dependencies are correctly defined under the `[tool.poetry.extras]` section, allowing users to install only the components they need (e.g., `pip install .[postgres,s3]`).

**Code Example (`pyproject.toml`):**
```toml
[tool.poetry.dependencies]
python = "^3.10"
psycopg2-binary = {version = "^2.9.9", optional = true}
s3fs = {version = "^2023.10.0", optional = true}
# ...

[tool.poetry.extras]
postgres = ["psycopg2-binary", "sqlalchemy"]
s3 = ["s3fs"]
all = ["psycopg2-binary", "sqlalchemy", "s3fs", "gcsfs", "azure-storage-blob", "typer"]
```

### 7.3 Testing Strategy

**Requirement:** A comprehensive testing strategy including unit tests (>90% coverage), integration tests (with Docker), and end-to-end tests.

**Status:** Partially Met

**Analysis:**
The project has a `tests` directory with a good structure, including unit tests for individual components (`test_parser.py`, `test_transformer.py`) and an integration test for the loader (`test_loader_integration.py`) that appears to be designed for use with `testcontainers`.

However, there are significant gaps:
-   **Coverage:** There is no configuration or report to verify the ">90% coverage" requirement.
-   **End-to-End Tests:** There are no dedicated end-to-end tests that simulate a full CLI run and verify the entire ETL cycle, including idempotency and versioning scenarios from start to finish.
-   **Performance Tests:** As mentioned in 6.3, there are no performance tests.

### 7.4 CI/CD

**Requirement:** Automated testing, linting, and publishing pipelines (e.g., GitHub Actions).

**Status:** Not Met

**Analysis:**
There is no CI/CD pipeline configuration (e.g., a `.github/workflows` directory) included in the provided codebase. This is a gap.

### 7.5 Documentation

**Requirement:** Comprehensive documentation (User Guide, API Docs, Architecture, Developer Guide) managed via Sphinx or MkDocs.

**Status:** Partially Met

**Analysis:**
The project has a `docs` directory with a structure that suggests the use of MkDocs (indicated by `mkdocs.yml`). The documentation is organized into a user guide, developer guide, and architecture sections.

However, the content is incomplete. The FRD requires more detailed content, including ERDs for the schemas and a comprehensive guide for creating new loaders. The current documentation is a good start but needs to be expanded.
