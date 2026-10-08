# Northwind Data Engineering Task

This project contains a data engineering task for modeling and interacting with the Northwind dataset using PostgreSQL, Docker, and Python.

## Prerequisites

- Docker and Docker Compose
- Python 3
- `psycopg2` Python package

## Setup Instructions

1. Start the database service using Docker Compose:
   ```bash
   docker-compose up -d
   ```

2. Install dependencies using uv:
   ```bash
   uv pip install psycopg2-binary
   ```

3. Run the database migration script to initialize the dataset:
   ```bash
   uv run python migrate.py
   ```

4. Run the main script to interact with the database and view statistical analysis:
   ```bash
   uv run python main.py
   ```

## Project Structure

- `docker-compose.yml`: Defines the PostgreSQL database service.
- `init.sql`: The SQL script used to initialize the Northwind dataset.
- `migrate.py`: A Python script that connects to the database and runs `init.sql`.
- `main.py`: A Python script containing functions to interact with the dataset, perform filtering, and run statistical analysis.
