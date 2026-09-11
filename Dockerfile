# Use a standard Python image that matches your ^3.9 requirement
FROM python:3.9-slim

# Install system dependencies if needed (slim image is barebones)
RUN apt-get update && apt-get install -y gcc

# Set the working directory
WORKDIR /app

# Install poetry
RUN pip install --no-cache-dir poetry

# Copy the poetry config files first to leverage Docker cache
COPY pyproject.toml poetry.lock ./

# Install dependencies (disable virtualenvs since Docker provides isolation)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main --no-root

# Copy the rest of your project code
COPY . .

# Entrypoint command
ENTRYPOINT ["poetry", "run", "python", "otacon/main.py"]