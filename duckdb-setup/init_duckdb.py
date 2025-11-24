"""
Initialize DuckDB connection with attached databases.
This creates the main DuckDB database and attaches ACT and VRM databases.
"""

import duckdb
import os

# Ensure data directory exists
os.makedirs('data', exist_ok=True)

# Create main DuckDB database
print("Creating main DuckDB database...")
conn = duckdb.connect('data/zoo_analytics.duckdb')

# Attach ACT database
print("Attaching ACT database...")
conn.execute("ATTACH 'data/act_system.duckdb' AS act_system (TYPE DUCKDB)")

# Attach VRM database
print("Attaching VRM database...")
conn.execute("ATTACH 'data/vrm_system.duckdb' AS vrm_system (TYPE DUCKDB)")

# Verify attachments
print("\nVerifying attached databases...")
act_tables = conn.execute("SHOW TABLES FROM act_system").fetchall()
print(f"ACT System tables: {[t[0] for t in act_tables]}")

vrm_tables = conn.execute("SHOW TABLES FROM vrm_system").fetchall()
print(f"VRM System tables: {[t[0] for t in vrm_tables]}")

conn.close()
print("\nMain DuckDB database initialized with attached databases!")

