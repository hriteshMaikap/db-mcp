from typing import List
from .db import DatabaseManager
from .models import SchemaMetadata, QueryResult

def get_schema() -> SchemaMetadata:
    """
    Get the database schema metadata.
    Returns a list of tables, columns, types, and comments.
    Useful for understanding the database structure before querying.
    """
    return DatabaseManager().get_schema()

def sample_rows(table_name: str, n: int = 5) -> QueryResult:
    """
    Get random sample rows from a table.
    Useful for understanding the content and format of data in a table.
    Args:
        table_name: The name of the table to sample.
        n: Number of rows to sample (default 5).
    """
    return DatabaseManager().sample_rows(table_name, n)

def run_select_query(query: str) -> QueryResult:
    """
    Execute a read-only SQL SELECT query.
    Only SELECT, SHOW, DESCRIBE, EXPLAIN statements are allowed.
    Args:
        query: The SQL query string.
    """
    return DatabaseManager().execute_query(query)

def refresh_schema() -> str:
    """
    Refresh the schema cache.
    Call this if the database schema has changed.
    """
    DatabaseManager().get_schema(refresh=True)
    return "Schema cache refreshed."
