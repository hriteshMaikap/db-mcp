from mcp.server.fastmcp import FastMCP
from sqlalchemy import create_engine, inspect, MetaData, text
from sqlalchemy.engine.url import URL
import os
import json
from dotenv import load_dotenv
load_dotenv()

# Database connection setup
engine = create_engine(URL.create(
    drivername="mysql+pymysql",
    username=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASS"),
    host=os.getenv("MYSQL_HOST"),
    port=os.getenv("MYSQL_PORT"),
    database=os.getenv("MYSQL_DB"),
    query={"charset": "utf8mb4"}
))

mcp = FastMCP("SQLDatabase")

# Tool: List of tables
@mcp.tool()
async def get_table_names() -> str:
    """Returns all table names in the database as a JSON list"""
    inspector = inspect(engine)
    return json.dumps(inspector.get_table_names())

# Tool: Table schema with relationships
@mcp.tool()
async def get_table_schema(table_name: str) -> str:
    """Returns detailed schema including relationships as JSON"""
    inspector = inspect(engine)
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    table = metadata.tables[table_name]

    schema = {
        "table": table_name,
        "columns": [],
        "primary_key": [c.name for c in table.primary_key.columns],
        "foreign_keys": []
    }

    for column in table.columns:
        schema["columns"].append({
            "name": column.name,
            "type": str(column.type),
            "nullable": column.nullable,
            "default": str(column.default) if column.default else None
        })

    for fk in table.foreign_key_constraints:
        schema["foreign_keys"].append({
            "columns": [c.name for c in fk.columns],
            "references": fk.referred_table,
            "remote_columns": [c.parent.name for c in fk.elements]
        })

    return json.dumps(schema, indent=2)

# Tool: Sample data from table
@mcp.tool()
async def get_table_sample(table_name: str) -> str:
    """Returns sample record from table as JSON"""
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return json.dumps({"error": f"Table '{table_name}' does not exist"})
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM `{table_name}` LIMIT 1"))
            row = result.mappings().first()
            return json.dumps(dict(row)) if row else "{}"
    except Exception as e:
        return json.dumps({"error": str(e)})

# Tool: SQL execution
@mcp.tool()
async def execute_sql(query: str) -> str:
    """Executes SQL query and returns results as JSON"""
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return json.dumps([dict(row) for row in result.mappings()])

if __name__ == "__main__":
    mcp.run(transport="streamable-http")