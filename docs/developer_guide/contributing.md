# Contributing

Contributions are welcome! This guide outlines the basic steps for setting up your development environment and contributing to the project.

## Development Environment Setup

This project uses Poetry for dependency management and packaging.

1.  **Fork and Clone:** Fork the repository on GitHub and clone your fork locally.
    ```bash
    git clone https://github.com/YOUR_USERNAME/py-load-eudravigilance.git
    cd py-load-eudravigilance
    ```

2.  **Install Dependencies:** Install all dependencies, including development and optional dependencies, using Poetry.
    ```bash
    poetry install --all-extras
    ```

3.  **Activate Virtual Environment:** Use the virtual environment created by Poetry.
    ```bash
    poetry shell
    ```

## Running Tests

The project has a comprehensive test suite using `pytest`.

To run all tests:
```bash
pytest
```

To run tests with coverage reporting:
```bash
pytest --cov=src/py_load_eudravigilance
```

The integration tests require **Docker** to be running, as they will automatically spin up a PostgreSQL container to run tests against a live database.

## Code Quality and Style

This project uses `Ruff` for linting and `Black` for code formatting. Before committing, please run these tools to ensure your code adheres to the project's standards.

```bash
# Format code with Black
black .

# Lint code with Ruff (and auto-fix where possible)
ruff check --fix .
```

Type hints are mandatory and are checked with `MyPy`.

```bash
mypy src
```

A CI pipeline will run these checks on all pull requests.

## Submitting a Pull Request

1.  Create a new branch for your feature or bug fix.
2.  Make your changes and add/update tests as appropriate.
3.  Ensure all tests and code quality checks pass.
4.  Push your branch to your fork and open a pull request against the main repository.
