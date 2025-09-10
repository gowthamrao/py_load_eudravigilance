from abc import ABC, abstractmethod
from io import IOBase
from typing import Any, Dict, List

# Using Any for Connection to avoid a premature driver import in the interface.
# A concrete implementation would use its specific connection object.
from sqlalchemy.engine import Connection


class LoaderInterface(ABC):
    """
    Abstract Base Class for database loaders.

    This interface defines the contract for all database-specific loaders,
    ensuring that the core ETL logic remains independent of the target
    database technology, as required by the Strategy Pattern design.
    """

    @abstractmethod
    def validate_schema(self, schema_definition: Dict[str, Any]) -> bool:
        """Verify the target database schema matches the expected definition."""
        raise NotImplementedError

    @abstractmethod
    def prepare_load(self, conn: Connection, target_table: str, load_mode: str) -> str:
        """
        Execute pre-loading tasks and return the name of the staging table.

        For example, truncate tables in 'full' mode or create temporary
        staging tables.
        """
        raise NotImplementedError

    @abstractmethod
    def bulk_load_native(
        self,
        conn: Connection,
        data_stream: IOBase,
        target_table: str,
        columns: List[str],
    ) -> None:
        """
        Stream data from data_stream into the target_table using the
        database's native bulk load utility (e.g., COPY FROM STDIN).
        """
        raise NotImplementedError

    @abstractmethod
    def handle_upsert(
        self,
        conn: Connection,
        staging_table: str,
        target_table: str,
        primary_keys: List[str],
        version_key: str | None,
    ) -> None:
        """
        Implement the logic for merging data from a staging table into the
        final target tables (MERGE/UPSERT).
        """
        raise NotImplementedError

    @abstractmethod
    def create_all_tables(self) -> None:
        """
        Creates all necessary tables (data, metadata, audit) in the
        target database if they don't already exist.
        """
        raise NotImplementedError

    @abstractmethod
    def get_completed_file_hashes(self) -> set[str]:
        """
        Retrieves a set of file hashes for all files that have been
        successfully processed ('completed' status).
        """
        raise NotImplementedError

    @abstractmethod
    def load_normalized_data(
        self,
        buffers: Dict[str, IOBase],
        row_counts: Dict[str, int],
        load_mode: str,
        file_path: str,
        file_hash: str,
    ) -> None:
        """
        Orchestrates the loading of multiple normalized data buffers into their
        respective tables within a single transaction.
        """
        raise NotImplementedError

    @abstractmethod
    def load_audit_data(
        self,
        buffer: IOBase,
        row_count: int,
        load_mode: str,
        file_path: str,
        file_hash: str,
    ) -> None:
        """
        Orchestrates the loading of the audit data (JSON) into the
        `icsr_audit_log` table within a single transaction.
        """
        raise NotImplementedError
