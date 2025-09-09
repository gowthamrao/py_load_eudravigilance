# Configuration

The application is configured through a combination of a YAML file (`config.yaml`) and environment variables. Environment variables will always override settings defined in the YAML file.

## `config.yaml`

By default, the application looks for a `config.yaml` file in the directory where it is run. You can specify a different path using the `--config` option in the CLI.

Here is a full example of a `config.yaml` file:

```yaml
# The database connection string (DSN).
# It's highly recommended to set this via an environment variable for security.
database:
  dsn: "postgresql://user:password@localhost:5432/mydatabase"

# The URI for the source XML files.
# Can be a local path, a glob pattern, or a cloud URI.
# e.g., "data/icsr/*.xml" or "s3://my-bucket/incoming-icsrs/"
source_uri: "data/input/*.xml"

# The schema to load. Can be "normalized" or "audit".
# "normalized": A relational schema optimized for analytics.
# "audit": A single table with the full ICSR payload in a JSONB column.
schema_type: "normalized"

# (Optional) The URI for the quarantine location.
# If a file fails processing, it will be moved here.
# e.g., "data/quarantine/" or "s3://my-bucket/quarantined-files/"
quarantine_uri: "data/quarantine/"
```

## Environment Variables

All settings can be provided or overridden via environment variables. This is the recommended way to provide sensitive information like the database DSN.

The environment variables are prefixed with `PY_LOAD_EUDRAVIGILANCE_`.

*   `PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN`
    *   Overrides `database.dsn`.
    *   **Example:** `export PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN="postgresql://prod_user:secret@prod-db:5432/prod_db"`

*   `PY_LOAD_EUDRAVIGILANCE_SOURCE_URI`
    *   Overrides `source_uri`.

*   `PY_LOAD_EUDRAVIGILANCE_SCHEMA_TYPE`
    *   Overrides `schema_type`.

*   `PY_LOAD_EUDRAVIGILANCE_QUARANTINE_URI`
    *   Overrides `quarantine_uri`.

### Precedence

1.  **Command-line arguments** (e.g., `--source-uri` in the `run` command) have the highest precedence.
2.  **Environment variables** are checked next.
3.  **`config.yaml` file** is used as the base.
