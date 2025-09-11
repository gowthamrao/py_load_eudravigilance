"""
Handles the Loading phase of the ETL process.

This module defines the database loader interface and provides concrete
implementations for different database backends (e.g., PostgreSQL).
It is responsible for all database interactions, including native bulk loading.
"""
from importlib import metadata
from typing import Any, Dict

from .base import BaseLoader


def get_loader(config: Dict[str, Any]) -> BaseLoader:
    """
    Factory function to get the appropriate database loader using a plugin system.
    It discovers available loaders via the `py_load_eudravigilance.loaders`
    entry point group.
    """
    loader_type = config.get("type")
    if not loader_type:
        raise ValueError("Configuration must include a 'type' key.")

    # Discover registered loader plugins
    try:
        loaders = metadata.entry_points(group="py_load_eudravigilance.loaders")
    except Exception as e:
        # This can happen if the entry points are misconfigured.
        raise RuntimeError(f"Could not load entry points: {e}") from e

    # Find a matching loader by its registered name (which should match the loader type)
    for ep in loaders:
        if ep.name == loader_type:
            loader_class = ep.load()
            try:
                # Instantiate and return the loader
                return loader_class(config["config"])
            except NameError as ne:
                # This typically means a soft dependency is missing.
                # The original NameError is obscure, so we raise a more helpful error.
                raise ImportError(
                    f"Dependencies for the '{loader_type}' loader are not installed. "
                    f"Please install the required extras (e.g., 'pip install .[{loader_type}]'). "
                    f"Original error: {ne}"
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to instantiate loader for type '{loader_type}'."
                ) from e

    # If no loader was found after checking all entry points
    available = [ep.name for ep in loaders]
    raise ValueError(
        f"No registered loader for type '{loader_type}'. "
        f"Available loaders: {available}"
    )


