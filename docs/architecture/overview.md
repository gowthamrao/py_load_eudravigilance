# Architectural Overview

The `py-load-eudravigilance` package is designed with a modular, three-tier architecture that promotes separation of concerns, flexibility, and performance. The three core components are the **Parser**, the **Transformer**, and the **Loader**.

```mermaid
graph TD
    A[Source Files (.xml)] --> B{Parser Core};
    B -->|Parsed ICSR Dictionaries| C{Transformation Layer};
    C -->|Normalized CSV Buffers| D{Loader Interface};
    D --> E[PostgreSQL];
    D --> F[Other Databases...];

    subgraph Extraction
        B
    end

    subgraph Transformation
        C
    end

    subgraph Loading
        D
        E
        F
    end
```

## 1. Parser Core (Extraction)

*   **Responsibility:** Locating, reading, and parsing the source ICH E2B(R3) XML files.
*   **Key Features:**
    *   **Filesystem Agnostic:** Uses `fsspec` to read from local disks, network shares, or cloud storage (S3, GCS, Azure Blob) using the same URI-based interface.
    *   **Memory Efficient:** Employs a streaming parser (`lxml.etree.iterparse`) to process XML files of any size with a minimal and constant memory footprint. It never loads the entire file into memory.
    *   **Parallel Processing:** The orchestration layer uses a `ProcessPoolExecutor` to run the parsing of multiple files in parallel, maximizing CPU usage.
    *   **Error Handling:** It can identify and isolate invalid ICSRs within a larger batch file, allowing valid records to be processed while invalid ones are flagged.

## 2. Transformation Layer

*   **Responsibility:** Converting the parsed, hierarchical Python dictionaries into a flat, relational format suitable for bulk loading.
*   **Key Features:**
    *   **Normalization:** It correctly maps the nested E2B(R3) structure to the target relational schema, handling one-to-one and one-to-many relationships.
    *   **Schema-Aware:** The transformation logic is dynamically driven by the central SQLAlchemy schema definition, ensuring that the output is always in sync with the database model.
    *   **In-Memory Buffers:** The transformed data is written to in-memory CSV buffers (`io.StringIO`). This avoids writing temporary files to disk and allows data to be streamed directly from the transformation step to the loading step, which is critical for performance.
    *   **Dual-Mode:** It supports transforming data for both the `normalized` analytical schema and the `audit` JSON schema.

## 3. Loader Interface (Loading)

*   **Responsibility:** All interactions with the target database.
*   **Key Features:**
    *   **Database Abstraction:** The `LoaderInterface` is an Abstract Base Class that defines a standard contract for all database-specific loaders. This decouples the core ETL logic from the database technology.
    *   **Native Bulk Loading:** Concrete implementations (like `PostgresLoader`) are required to use the database's most efficient native bulk loading utility (e.g., `COPY FROM STDIN` in PostgreSQL). This is the key to achieving high-throughput data loading.
    *   **Transactional Integrity:** The entire process for a given file—including staging, bulk loading, and upserting—is wrapped in a single database transaction to ensure atomicity. If any step fails, the entire operation is rolled back.
    *   **Idempotency:** A combination of file hash tracking and a version-aware `UPSERT` (or `MERGE`) strategy ensures that re-processing the same files will not create duplicate data or overwrite newer records with older ones.
    *   **Extensibility:** A plugin system based on Python's `entry_points` allows new loaders to be added and discovered dynamically without modifying the core package code.
