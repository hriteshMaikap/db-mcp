from mcp.server.fastmcp import FastMCP
from .tools.base import MongoDBSession
from ..config import MongoDBConfig
from .tools.connection import ConnectTool
from .tools.read.find import FindTool
from .tools.read.count import CountTool
from .tools.read.aggregate import AggregateTool
from .tools.read.collection_indexes import CollectionIndexesTool
from ..metadata.list_databases import ListDatabasesTool
from ..metadata.list_collections import ListCollectionsTool

# Initialize MCP server
mcp = FastMCP("MongoDB Read-Only Server", port=8002)

# Initialize session and config
session = MongoDBSession()
config = MongoDBConfig.from_env()

# Initialize tools
tools = {
    "connect": ConnectTool(session, config),
    "find": FindTool(session, config),
    "count": CountTool(session, config),
    "aggregate": AggregateTool(session, config),
    "collection_indexes": CollectionIndexesTool(session, config),
    "list_databases": ListDatabasesTool(session, config),
    "list_collections": ListCollectionsTool(session, config),
}

@mcp.tool()
async def connect() -> str:
    """Connect to MongoDB instance and test the connection"""
    result = await tools["connect"].execute()
    return str(result)

@mcp.tool()
async def find(
    collection: str,
    database: str = None,
    filter: dict = None,
    projection: dict = None,
    sort: dict = None,
    limit: int = 10,
    skip: int = 0
) -> str:
    """Query documents from a MongoDB collection"""
    result = await tools["find"].execute(
        collection=collection,
        database=database,
        filter=filter,
        projection=projection,
        sort=sort,
        limit=limit,
        skip=skip
    )
    return str(result)

@mcp.tool()
async def count(collection: str, database: str = None, filter: dict = None) -> str:
    """Count documents in a MongoDB collection"""
    result = await tools["count"].execute(
        collection=collection,
        database=database,
        filter=filter
    )
    return str(result)

@mcp.tool()
async def aggregate(collection: str, pipeline: list, database: str = None) -> str:
    """Run aggregation pipeline against a MongoDB collection"""
    result = await tools["aggregate"].execute(
        collection=collection,
        pipeline=pipeline,
        database=database
    )
    return str(result)

@mcp.tool()
async def collection_indexes(collection: str, database: str = None) -> str:
    """List all indexes for a MongoDB collection"""
    result = await tools["collection_indexes"].execute(
        collection=collection,
        database=database
    )
    return str(result)

@mcp.tool()
async def list_databases() -> str:
    """List all databases in the MongoDB instance"""
    result = await tools["list_databases"].execute()
    return str(result)

@mcp.tool()
async def list_collections(database: str = None) -> str:
    """List all collections in a MongoDB database"""
    result = await tools["list_collections"].execute(database=database)
    return str(result)

if __name__ == "__main__":
    mcp.run(transport="streamable-http")