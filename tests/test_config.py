import os
import yaml
import pytest
from pathlib import Path
from pydantic import ValidationError
from py_load_eudravigilance.config import load_config, Settings

@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """Creates a dummy config.yaml file for testing."""
    config_content = {
        "database": {
            "dsn": "dsn_from_file"
        },
        "source_uri": "uri_from_file",
        "schema_type": "audit"
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_content, f)
    return config_path

def test_load_from_yaml_file(config_file: Path):
    """
    Tests that configuration is loaded correctly from a YAML file when
    no environment variables are set.
    """
    settings = load_config(path=str(config_file))
    assert isinstance(settings, Settings)
    assert settings.database.dsn == "dsn_from_file"
    assert settings.source_uri == "uri_from_file"
    assert settings.schema_type == "audit"

def test_env_vars_override_yaml(config_file: Path, monkeypatch):
    """
    Tests that environment variables correctly override settings from the YAML file.
    This verifies the source priority.
    """
    # Set environment variables that correspond to the Pydantic model fields
    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_DATABASE__DSN", "dsn_from_env")
    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_SOURCE_URI", "uri_from_env")

    # Load the configuration
    settings = load_config(path=str(config_file))

    # Assert that the environment variable values took precedence
    assert settings.database.dsn == "dsn_from_env"
    assert settings.source_uri == "uri_from_env"
    # This value was not overridden, so it should come from the file
    assert settings.schema_type == "audit"

def test_load_from_env_only(monkeypatch):
    """
    Tests that the configuration can be loaded entirely from environment
    variables when no config file is present.
    """
    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_DATABASE__DSN", "dsn_from_env_only")
    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_SCHEMA_TYPE", "audit")

    # Use a path that does not exist
    non_existent_path = "/tmp/non_existent_config.yaml"
    assert not os.path.exists(non_existent_path)

    settings = load_config(path=non_existent_path)
    assert settings.database.dsn == "dsn_from_env_only"
    # This was not set, so it should use the model's default
    assert settings.source_uri is None
    # This was set in the environment
    assert settings.schema_type == "audit"

def test_missing_required_field_raises_error(tmp_path: Path, monkeypatch):
    """
    Tests that a validation error is raised if a required field (like DSN)
    is not provided in any source.
    """
    # Create an empty config file
    empty_config = tmp_path / "empty.yaml"
    empty_config.touch()

    # Ensure the relevant env var is not set
    monkeypatch.delenv("PY_LOAD_EUDRAVIGILANCE_DATABASE__DSN", raising=False)

    with pytest.raises(ValueError, match="Configuration validation error"):
        load_config(path=str(empty_config))

def test_invalid_schema_type_raises_error(config_file: Path, monkeypatch):
    """
    Tests that a validation error is raised for an invalid 'schema_type' value.
    """
    # Override with an invalid value
    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_SCHEMA_TYPE", "invalid_type")

    with pytest.raises(ValueError, match="Configuration validation error"):
        load_config(path=str(config_file))
