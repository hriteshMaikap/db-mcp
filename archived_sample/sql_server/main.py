from fastmcp import FastMCP
from .db import DatabaseManager
from .models import SchemaMetadata, QueryResult

# Initialize FastMCP
mcp = FastMCP("SQL Context Server")

@mcp.tool()
def get_schema() -> SchemaMetadata:
    """
    Get the database schema metadata.
    Returns a list of tables, columns, types, and comments.
    Useful for understanding the database structure before querying.
    """
    return DatabaseManager().get_schema()

@mcp.tool()
def sample_rows(table_name: str, n: int = 5) -> QueryResult:
    """
    Get random sample rows from a table.
    Useful for understanding the content and format of data in a table.
    Args:
        table_name: The name of the table to sample.
        n: Number of rows to sample (default 5).
    """
    return DatabaseManager().sample_rows(table_name, n)

@mcp.tool()
def run_select_query(query: str) -> QueryResult:
    """
    Execute a read-only SQL SELECT query.
    Only SELECT, SHOW, DESCRIBE, EXPLAIN statements are allowed.
    Args:
        query: The SQL query string.
    """
    return DatabaseManager().execute_query(query)

@mcp.tool()
def refresh_schema() -> str:
    """
    Refresh the schema cache.
    Call this if the database schema has changed.
    """
    DatabaseManager().get_schema(refresh=True)
    return "Schema cache refreshed."

if __name__ == "__main__":
    # Run the server using SSE (Server-Sent Events) over HTTP
    # This enables the server to be accessed via network/HTTP
    print("Starting SQL MCP Server on http://127.0.0.1:8000/sse")
    mcp.run(transport="sse", host="127.0.0.1", port=8000)
