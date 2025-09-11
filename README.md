# py_load_eudravigilance

A high-performance, extensible, and robust Python package for the Extraction, Transformation, and Loading (ETL) of EudraVigilance Individual Case Safety Reports (ICSRs).

This package provides a flexible and modular way to load data into various database systems, including PostgreSQL, Amazon Redshift, Databricks, and Google BigQuery.

## Installation

Install the core package using pip:

```bash
pip install .
```

To include support for a specific database, install the corresponding extra:

```bash
# For PostgreSQL
pip install .[postgres]

# For Amazon Redshift
pip install .[redshift]

# For Databricks
pip install .[databricks]

# For Google BigQuery
pip install .[bigquery]

# To install all dependencies
pip install .[all]
```

## Usage

Here's how to use the library to load a pandas DataFrame into different databases:

```python
import pandas as pd
from py_load_eudravigilance.loader import get_loader

# --- Example Data ---
data = {'col1': [1, 2], 'col2': ['A', 'B']}
df = pd.DataFrame(data)

# --- Load to PostgreSQL ---
postgres_config = {
    "type": "postgresql",
    "url": "postgresql://user:password@host:port/database"
}
pg_loader = get_loader(postgres_config)
# pg_loader.load_normalized_data(...) # or other specific loading methods

# --- Load to Redshift ---
redshift_config = {
    "type": "redshift",
    "host": "my-cluster.redshift.amazonaws.com",
    "user": "awsuser",
    "iam_role_arn": "arn:aws:iam::12345:role/MyRedshiftRole",
    "s3_staging_path": "s3://my-bucket/staging-data/"
    # ... other redshift_connector params
}
redshift_loader = get_loader(redshift_config)
redshift_loader.connect()
# redshift_loader.load_normalized_data(...)
redshift_loader.close()

# --- Load to Databricks ---
databricks_config = {
    "type": "databricks",
    "server_hostname": "...",
    "http_path": "...",
    "access_token": "...",
    "storage_path": "s3a://my-bucket/staging-data/"
}
databricks_loader = get_loader(databricks_config)
databricks_loader.connect()
# databricks_loader.load_normalized_data(...)
databricks_loader.close()

# --- Load to BigQuery ---
bigquery_config = {
    "type": "bigquery",
    "project": "my-gcp-project",
    "dataset": "my_dataset"
}
bq_loader = get_loader(bigquery_config)
bq_loader.connect()
# bq_loader.load_normalized_data(...)
bq_loader.close()
```
