# Installation

`py-load-eudravigilance` is packaged using Poetry and is available on PyPI.

## Standard Installation

You can install the core package using pip:

```bash
pip install py-load-eudravigilance
```

However, the core package does not include dependencies for specific databases or cloud file systems. To use the tool, you must install it with "extras".

## Installation with Extras

Extras are optional sets of dependencies that you can install based on your needs.

### Database Adapters

You must install the extra for the database you intend to use.

*   **For PostgreSQL:**
    ```bash
    pip install "py-load-eudravigilance[postgres]"
    ```

### Cloud Storage Support

If you need to read files from a cloud provider, install the relevant extra:

*   **For AWS S3:**
    ```bash
    pip install "py-load-eudravigilance[s3]"
    ```
*   **For Google Cloud Storage:**
    ```bash
    pip install "py-load-eudravigilance[gcs]"
    ```
*   **For Azure Blob Storage:**
    ```bash
    pip install "py-load-eudravigilance[azure]"
    ```

### CLI Support

To use the command-line interface, you must install the `cli` extra:

```bash
pip install "py-load-eudravigilance[cli]"
```

### Installing Multiple Extras

You can combine extras by separating them with commas. For example, to use the CLI with PostgreSQL and S3, you would install:

```bash
pip install "py-load-eudravigilance[cli,postgres,s3]"
```

To install everything, use the `all` extra:

```bash
pip install "py-load-eudravigilance[all]"
```
