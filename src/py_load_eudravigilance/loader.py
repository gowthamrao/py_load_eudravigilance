"""
Handles the Loading phase of the ETL process.

This module defines the database loader interface and provides concrete
implementations for different database backends (e.g., PostgreSQL).
It is responsible for all database interactions, including native bulk loading.
"""
from abc import ABC, abstractmethod
from io import IOBase
from typing import Any, Dict, List

# Using Any for Connection to avoid a premature driver import in the interface.
# A concrete implementation would use its specific connection object.
Connection = Any


class LoaderInterface(ABC):
    """
    Abstract Base Class for database loaders.

    This interface defines the contract for all database-specific loaders,
    ensuring that the core ETL logic remains independent of the target
    database technology, as required by the Strategy Pattern design.
    """

    @abstractmethod
    def connect(self) -> Connection:
        """Establish a connection using the appropriate native driver."""
        raise NotImplementedError

    @abstractmethod
    def validate_schema(self, schema_definition: Dict[str, Any]) -> bool:
        """Verify the target database schema matches the expected definition."""
        raise NotImplementedError

    @abstractmethod
    def prepare_load(self, load_mode: str) -> None:
        """
        Execute pre-loading tasks.

        For example, truncate tables in 'full' mode or create temporary
        staging tables.
        """
        raise NotImplementedError

    @abstractmethod
    def bulk_load_native(
        self, data_stream: IOBase, target_table: str, columns: List[str]
    ) -> None:
        """
        Stream data from data_stream into the target_table using the
        database's native bulk load utility (e.g., COPY FROM STDIN).
        """
        raise NotImplementedError

    @abstractmethod
    def handle_upsert(
        self,
        staging_table: str,
        target_table: str,
        primary_keys: List[str],
        version_key: str,
    ) -> None:
        """
        Implement the logic for merging data from a staging table into the
        final target tables (MERGE/UPSERT).
        """
        raise NotImplementedError

    @abstractmethod
    def manage_transaction(self, action: str) -> None:
        """
        Handle transaction boundaries (e.g., 'BEGIN', 'COMMIT', 'ROLLBACK').
        """
        raise NotImplementedError
