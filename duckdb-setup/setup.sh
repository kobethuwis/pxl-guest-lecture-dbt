#!/bin/bash

echo "Setting up Zoo Analytics dbt Exercise..."
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is required but not installed."
    exit 1
fi

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Create databases
echo ""
echo "Creating ACT and VRM databases..."
python3 duckdb-setup/setup_databases.py

# Initialize main DuckDB
echo ""
echo "Initializing main DuckDB database..."
python3 duckdb-setup/init_duckdb.py

# Create results directory for CSV exports
echo ""
echo "Creating results directory..."
mkdir -p results

echo ""
echo "Setup complete!"