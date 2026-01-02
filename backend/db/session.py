"""
Database session management for DuckDB.

Uses raw DuckDB Python API instead of SQLAlchemy for better compatibility
and performance with analytical workloads.
"""

import os
import duckdb
from pathlib import Path
from contextlib import contextmanager
from typing import Generator

# Get database path from environment or use default
DB_PATH = os.getenv("DATABASE_URL", "data/weather.duckdb")

# Ensure data directory exists
data_dir = Path(DB_PATH).parent
data_dir.mkdir(parents=True, exist_ok=True)


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Get DuckDB connection.

    Returns:
        DuckDB connection instance
    """
    return duckdb.connect(DB_PATH)


@contextmanager
def get_session() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager for database connections.

    Usage:
        with get_session() as con:
            con.execute("INSERT INTO ...")
            result = con.execute("SELECT ...").fetchall()

    Yields:
        DuckDB connection
    """
    con = get_connection()
    try:
        yield con
    finally:
        con.close()


def init_db():
    """
    Initialize database - create all tables from schema.sql.

    This is idempotent - safe to run multiple times.
    """
    schema_path = Path(__file__).parent / "schema.sql"

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    # Read schema
    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    # Execute schema
    with get_session() as con:
        con.execute(schema_sql)

    print(f"✓ Database initialized at {DB_PATH}")
    print(f"✓ Schema loaded from {schema_path}")


def drop_all_tables():
    """
    Drop all tables from database.

    WARNING: This will delete all data!
    """
    tables = [
        # Satellites (drop first due to foreign keys)
        'sat_observation',
        'sat_grid_data',
        'sat_forecast_hourly',
        'sat_forecast_period',
        'sat_office_details',
        'sat_station_details',
        'sat_zone_details',
        'sat_resort_details',
        # Links
        'link_zone_office',
        'link_resort_station',
        'link_resort_zone',
        # Hubs
        'hub_office',
        'hub_station',
        'hub_zone',
        'hub_resort',
    ]

    with get_session() as con:
        for table in tables:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print("✓ All tables dropped")


def get_tables() -> list:
    """
    Get list of all tables in the database.

    Returns:
        List of table names
    """
    with get_session() as con:
        result = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()

    return [row[0] for row in result]


def execute_query(query: str, params: tuple = None):
    """
    Execute a query and return results.

    Args:
        query: SQL query string
        params: Query parameters (optional)

    Returns:
        Query results
    """
    with get_session() as con:
        if params:
            return con.execute(query, params).fetchall()
        else:
            return con.execute(query).fetchall()


def execute_query_df(query: str, params: tuple = None):
    """
    Execute a query and return results as pandas DataFrame.

    Args:
        query: SQL query string
        params: Query parameters (optional)

    Returns:
        pandas DataFrame
    """
    with get_session() as con:
        if params:
            return con.execute(query, params).df()
        else:
            return con.execute(query).df()


if __name__ == "__main__":
    # Test database connection
    print(f"Database path: {DB_PATH}")
    print("\nInitializing database...")
    init_db()

    print("\nGetting tables...")
    tables = get_tables()
    print(f"✓ Found {len(tables)} tables:")
    for table in tables:
        print(f"  - {table}")

    print("\nTesting query...")
    result = execute_query("SELECT 'Hello from DuckDB!' as message")
    print(f"✓ Query result: {result[0][0]}")

    print("\n✓ Database tests passed!")
