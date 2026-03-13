"""
DuckDB helper — creates in-memory views over all Parquet files so every
agent can query the full dataset with plain SQL.
"""

import os
from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).parent.parent
_scratch = os.environ.get("COUCHE_SCRATCH")
PROCESSED_DIR = Path(_scratch) / "processed" if _scratch else BASE_DIR / "processed"


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with `tracks` and `events` views."""
    conn = duckdb.connect()

    tracks_glob = str(PROCESSED_DIR / "tracks" / "*.parquet")
    events_glob = str(PROCESSED_DIR / "events" / "*.parquet")

    conn.execute(f"CREATE VIEW tracks AS SELECT * FROM read_parquet('{tracks_glob}')")
    conn.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{events_glob}')")

    return conn


def query_df(sql: str):
    """Convenience: execute SQL and return a pandas DataFrame."""
    return get_connection().execute(sql).df()
