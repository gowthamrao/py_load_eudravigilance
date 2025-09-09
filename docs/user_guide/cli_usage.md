# CLI Usage

The command-line interface, `eudravigloader`, is the primary way to interact with the application. Ensure you have installed the `cli` extra (`pip install "py-load-eudravigilance[cli]"`).

You can get help for any command by passing `--help`.

```bash
eudravigloader --help
eudravigloader run --help
```

## `run`

This is the main command to execute the ETL pipeline.

```bash
eudravigloader run [OPTIONS] [SOURCE_URI]
```

### Arguments

*   `[SOURCE_URI]` (Optional): The URI for the source XML files. This can be a local path, a glob pattern, or a cloud URI (e.g., `s3://...`). If provided, it overrides the `source_uri` from your config file.

### Options

*   `--mode TEXT`: The load mode. Can be `delta` (default) or `full`.
    *   `delta`: Processes only new or changed files and upserts data into the target tables.
    *   `full`: Truncates all target tables and performs a fresh load of all discovered files.
*   `--workers INTEGER`: The number of parallel processes to use for file processing. Defaults to the number of CPU cores on your machine.
*   `--config, -c PATH`: Path to your `config.yaml` file. Defaults to `./config.yaml`.

### Examples

*   Run an incremental (delta) load using settings from `config.yaml`:
    ```bash
    eudravigloader run
    ```
*   Run a full load from a specific S3 bucket path:
    ```bash
    eudravigloader run --mode full "s3://my-prod-bucket/icsrs-2024/"
    ```
*   Run a delta load using only 4 worker processes:
    ```bash
    eudravigloader run --workers 4
    ```

## `init-db`

This command initializes the database by creating all the necessary tables (for data, audit logs, and ETL metadata). This should be run once before you run the pipeline for the first time.

```bash
eudravigloader init-db [OPTIONS]
```

### Options

*   `--config, -c PATH`: Path to your `config.yaml` file.

## `validate`

This command validates one or more XML files against a given XSD schema without loading any data.

```bash
eudravigloader validate [OPTIONS] SOURCE_URI
```

### Arguments

*   `SOURCE_URI` (Required): The URI of the XML file(s) to validate.

### Options

*   `--schema, -s PATH`: The file path to the XSD schema to validate against. This is required.

### Example

```bash
eudravigloader validate --schema schemas/ich-icsr-e2b-r3.xsd "data/input/*.xml"
```

## `validate-db-schema`

This command connects to the database and validates that the existing table structures match the schema expected by the application. This is useful for diagnosing migration issues or incorrect setups.

```bash
eudravigloader validate-db-schema [OPTIONS]
```

### Options

*   `--config, -c PATH`: Path to your `config.yaml` file.
