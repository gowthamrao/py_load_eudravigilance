"""
Configuration management for the application.

This module uses Pydantic to provide a robust, hierarchical, and type-safe
configuration system. It supports loading settings from a YAML file and
overriding them with environment variables.
"""

from __future__ import annotations

import collections.abc
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

CONFIG_FILE_NAME = "config.yaml"


def deep_merge(source: Dict, destination: Dict) -> Dict:
    """
    Recursively merges source dict into destination dict.
    Nested dictionaries are merged, other values in destination are overwritten.
    """
    for key, value in source.items():
        if isinstance(value, collections.abc.Mapping) and key in destination:
            destination[key] = deep_merge(value, destination.get(key, {}))
        else:
            destination[key] = value
    return destination


class DatabaseConfig(BaseModel):
    """Pydantic model for database connection settings."""

    dsn: str = Field(..., description="The full database connection string (DSN).")


# This is the final, strict configuration model that will be used by the application.
# It enforces that required fields like 'database' are present.
class Settings(BaseModel):
    """The application's strict configuration model."""

    database: DatabaseConfig
    source_uri: Optional[str] = None
    schema_type: str = "normalized"
    quarantine_uri: Optional[str] = None
    xsd_schema_path: Optional[str] = None

    @field_validator("schema_type")
    @classmethod
    def validate_schema_type(cls, v: str) -> str:
        if v not in ["normalized", "audit"]:
            raise ValueError("schema_type must be either 'normalized' or 'audit'")
        return v


# This is a helper model used ONLY to load settings from environment variables.
# All fields are optional, so it won't raise validation errors if env vars are missing.
class _EnvSettings(BaseSettings):
    """Helper model to load environment variables without strict validation."""

    model_config = SettingsConfigDict(
        env_prefix="PY_LOAD_EUDRAVIGILANCE_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    database: Optional[DatabaseConfig] = None
    source_uri: Optional[str] = None
    schema_type: Optional[str] = None
    quarantine_uri: Optional[str] = None
    xsd_schema_path: Optional[str] = None


def load_config(path: Optional[str] = None) -> Settings:
    """
    Loads application configuration with correct precedence.

    The loading precedence is:
    1. Environment Variables
    2. YAML Configuration File
    3. Model Default Values (from the `Settings` model)

    Args:
        path: Path to the YAML configuration file. Defaults to './config.yaml'.

    Returns:
        A fully populated and validated Settings object.
    """
    config_path = path or f"./{CONFIG_FILE_NAME}"

    # 1. Load base configuration from the YAML file.
    file_config: Dict[str, Any] = {}
    if Path(config_path).is_file():
        with open(config_path, "r") as f:
            file_config = yaml.safe_load(f) or {}

    # 2. Load settings from environment variables using the helper model.
    env_loader = _EnvSettings()
    # `exclude_unset=True` ensures we only get values explicitly set in the env.
    env_config = env_loader.model_dump(exclude_unset=True)

    # 3. Merge the configurations.
    # We start with the file config and merge the env config into it,
    # so environment variables take precedence.
    merged_config = deep_merge(source=env_config, destination=file_config)

    try:
        # 4. Create the final, validated settings object from the merged config.
        # The main `Settings` class provides the strict validation.
        return Settings(**merged_config)
    except ValidationError as e:
        raise ValueError(f"Configuration validation error: {e}") from e
