from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Generator

import pandas as pd


class BaseLoader(ABC):
    """Abstract base class for all database loaders."""

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    @contextmanager
    def connect(self) -> Generator:
        """A context manager for the database connection."""
        raise NotImplementedError

    @abstractmethod
    def load_dataframe(
        self, df: pd.DataFrame, table_name: str, connection: Any
    ) -> None:
        """Load a pandas DataFrame into the specified table."""
        raise NotImplementedError

    @abstractmethod
    def get_completed_file_hashes(self, connection: Any) -> set[str]:
        """Get a set of file hashes for already processed files."""
        raise NotImplementedError

    @abstractmethod
    def create_all_tables(self) -> None:
        """Create all tables required by the loader."""
        raise NotImplementedError

    def validate_schema(self, tables: dict) -> None:
        """
        Validates the database schema against the provided table definitions.
        This method is optional and the default implementation does nothing.
        """
        pass

    @abstractmethod
    def add_file_to_history(
        self, file_path: str, file_hash: str, connection: Any
    ) -> None:
        """Add a file to the ETL history table."""
        raise NotImplementedError

    @abstractmethod
    def get_table_schema(self, table_name: str) -> Any:
        """Get the schema for a specific table."""
        raise NotImplementedError
