"""
Configuration management for the application.

This module handles loading settings from a YAML file and overriding
them with environment variables for flexible configuration.
"""

import os
import yaml
from dataclasses import dataclass
from typing import Dict, Any, Optional

CONFIG_FILE_NAME = "config.yaml"

@dataclass
class DatabaseConfig:
    """Dataclass for database connection settings."""
    dsn: str

@dataclass
class Settings:
    """Main configuration class for the application."""
    database: DatabaseConfig
    source_uri: Optional[str] = None
    schema_type: str = "normalized"

def load_config(path: str = f"./{CONFIG_FILE_NAME}") -> Settings:
    """
    Loads configuration from a YAML file and environment variables.

    Environment variables can override YAML settings. For example, to override
    the database DSN, set the environment variable:
    PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN

    Args:
        path: The path to the configuration file.

    Returns:
        A Settings object with the loaded configuration.
    """
    config_from_file = _load_config_from_yaml(path)

    # Override with environment variables
    db_dsn_env = os.getenv("PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN")
    source_uri_env = os.getenv("PY_LOAD_EUDRAVIGILANCE_SOURCE_URI")
    schema_type_env = os.getenv("PY_LOAD_EUDRAVIGILANCE_SCHEMA_TYPE")

    db_dsn = db_dsn_env or config_from_file.get("database", {}).get("dsn")
    source_uri = source_uri_env or config_from_file.get("source_uri")
    schema_type = schema_type_env or config_from_file.get("schema_type", "normalized")

    if not db_dsn:
        raise ValueError("Database DSN must be provided in config.yaml or via PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN env var.")

    if schema_type not in ["normalized", "audit"]:
        raise ValueError("schema_type must be either 'normalized' or 'audit'")

    return Settings(
        database=DatabaseConfig(dsn=db_dsn),
        source_uri=source_uri,
        schema_type=schema_type,
    )

def _load_config_from_yaml(path: str) -> Dict[str, Any]:
    """Loads configuration from a YAML file if it exists."""
    if os.path.exists(path):
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    return {}

# A default settings instance for easy importing
# In a real app, you might have a more sophisticated way to manage this
# but for the CLI, we will load it explicitly.
# settings = load_config()
