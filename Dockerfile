# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy the entire project context
COPY . .

# Install all dependencies, including dev dependencies, and the project
# itself in editable mode. This ensures that entry points are registered
# and the package is importable.
RUN poetry install --no-interaction --no-ansi --with dev

# Default command to run tests
CMD ["poetry", "run", "pytest"]
