import os
import yaml
import pytest
from pathlib import Path
from py_load_eudravigilance.config import load_config, Settings, DatabaseConfig

@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    """Creates a dummy config.yaml file."""
    config_content = {
        "database": {
            "dsn": "dbname=test_from_file user=test"
        },
        "source_uri": "data/*.xml"
    }
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config_content, f)
    return config_path

def test_load_config_from_file(config_file: Path):
    """Tests that configuration is loaded correctly from a YAML file."""
    settings = load_config(path=str(config_file))
    assert isinstance(settings, Settings)
    assert settings.database.dsn == "dbname=test_from_file user=test"
    assert settings.source_uri == "data/*.xml"

def test_load_config_env_override(config_file: Path, monkeypatch):
    """Tests that environment variables override YAML file settings."""
    # Set environment variables
    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN", "dbname=test_from_env user=env")
    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_SOURCE_URI", "s3://overridden/data/*.xml")

    settings = load_config(path=str(config_file))
    assert settings.database.dsn == "dbname=test_from_env user=env"
    assert settings.source_uri == "s3://overridden/data/*.xml"

def test_load_config_missing_dsn_raises_error(tmp_path: Path):
    """Tests that a ValueError is raised if the DSN is not provided."""
    # Create an empty config file
    config_path = tmp_path / "config.yaml"
    config_path.touch()

    # Ensure env var is not set
    if "PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN" in os.environ:
        del os.environ["PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN"]

    with pytest.raises(ValueError, match="Database DSN must be provided"):
        load_config(path=str(config_path))

def test_load_config_only_env_works(monkeypatch):
    """Tests loading configuration purely from environment variables."""
    # Ensure no config file exists
    non_existent_path = "./non_existent_config.yaml"
    assert not os.path.exists(non_existent_path)

    monkeypatch.setenv("PY_LOAD_EUDRAVIGILANCE_DATABASE_DSN", "dbname=env_only user=env")

    settings = load_config(path=non_existent_path)
    assert settings.database.dsn == "dbname=env_only user=env"
    assert settings.source_uri is None
