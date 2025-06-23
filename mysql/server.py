from mcp.server.fastmcp import FastMCP
from sqlalchemy import create_engine, inspect, text
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
    port=int(os.getenv("MYSQL_PORT", 3306)),
    database=os.getenv("MYSQL_DB"),
    query={"charset": "utf8mb4"}
))

mcp = FastMCP("SQLDatabase")

@mcp.tool()
async def list_tables() -> str:
    """Return a JSON array of all table names in the database."""
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return json.dumps(tables)
    except Exception as e:
        return json.dumps({"error": str(e)})

@mcp.tool()
async def table_schema(table_name: str) -> str:
    """
    Return the schema for the given table as a JSON list of columns and types,
    plus one sample row (if exists).
    """
    try:
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return json.dumps({"error": f"Table '{table_name}' does not exist"})

        columns = inspector.get_columns(table_name)
        schema = [[col["name"], str(col["type"])] for col in columns]

        # Get one sample row
        sample_row = None
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM `{table_name}` LIMIT 1"))
                row = result.mappings().first()
                if row:
                    sample_row = dict(row)
        except Exception as e:
            sample_row = {"error": f"Could not fetch sample row: {str(e)}"}

        return json.dumps({
            "table": table_name,
            "schema": schema,
            "sample_row": sample_row
        })
    except Exception as e:
        return json.dumps({"error": str(e)})

if __name__ == "__main__":
    mcp.run(transport="streamable-http")