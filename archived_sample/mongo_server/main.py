from fastmcp import FastMCP
from typing import List, Dict, Optional, Any
from .db import DatabaseManager
from .models import SchemaMetadata, QueryResult

# Initialize FastMCP
mcp = FastMCP("MongoDB Context Server")

@mcp.tool()
def list_collections() -> List[str]:
    """
    List all collections in the MongoDB database.
    
    Returns:
        List of collection names available in the database.
    
    Example:
        ["users", "orders", "products"]
    """
    return DatabaseManager().list_collections()

@mcp.tool()
def get_schema(collection_name: str) -> SchemaMetadata:
    """
    Get the schema of a collection by inferring from sample documents.
    
    Args:
        collection_name: The name of the collection to analyze.
    
    Returns:
        Schema metadata including field names, types, examples, and document count.
    
    Example:
        collection_name: "users"
        Returns: Schema with fields like {name: str, email: str, age: int}
    """
    return DatabaseManager().get_schema(collection_name)

@mcp.tool()
def run_find_query(
    collection_name: str, 
    filter: Dict = {}, 
    projection: Optional[Dict] = None, 
    sort: Optional[List] = None, 
    limit: int = 5
) -> QueryResult:
    """
    Execute a MongoDB find query to retrieve documents.
    
    Args:
        collection_name: Name of the collection to query.
        filter: MongoDB filter query using operators like $gt, $lt, $in, $regex.
        projection: Fields to include (1) or exclude (0). Example: {"name": 1, "price": 1, "_id": 0}
        sort: Sort order as list of tuples. Example: [("price", -1), ("name", 1)]
        limit: Maximum documents to return (default: 5, max recommended: 100).
    
    Returns:
        QueryResult with documents list and count.
    
    Examples:
        1. Find products over $100:
           filter={"price": {"$gt": 100}}
        
        2. Find users with specific emails:
           filter={"email": {"$in": ["user1@example.com", "user2@example.com"]}}
        
        3. Search by pattern:
           filter={"name": {"$regex": "^Product", "$options": "i"}}
        
        4. Complex query with projection and sort:
           filter={"status": "active", "price": {"$gte": 50}}
           projection={"name": 1, "price": 1}
           sort=[("price", -1)]
    
    Common operators:
        - $eq, $ne: Equal, not equal
        - $gt, $gte, $lt, $lte: Comparison operators
        - $in, $nin: In array, not in array
        - $and, $or, $not: Logical operators
        - $regex: Pattern matching
        - $exists: Field exists check
    """
    return DatabaseManager().execute_query(
        collection_name, filter, projection, sort, limit
    )

@mcp.tool()
def run_aggregate_query(collection_name: str, pipeline: List[Dict]) -> QueryResult:
    """
    Execute a MongoDB aggregation pipeline for complex data analysis.
    
    Args:
        collection_name: Name of the collection to query.
        pipeline: List of aggregation stages in order.
    
    Returns:
        QueryResult with aggregated documents and count.
    
    CRITICAL RULES FOR AGGREGATION PIPELINES:
    
    1. $group stage REQUIRES accumulator operators:
       ✓ CORRECT: {"$group": {"_id": "$category", "total": {"$sum": "$price"}}}
       ✗ WRONG:   {"$group": {"_id": "$category", "total": "$price"}}
    
    2. Common accumulator operators:
       - $sum: Sum values or count documents
       - $avg: Calculate average
       - $min, $max: Find minimum/maximum
       - $first, $last: Get first/last value
       - $push: Create array of values
       - $addToSet: Create array of unique values
    
    3. Stage order matters: $match early, $sort after $group
    
    Examples:
    
    1. Group and count by category:
       [
         {"$group": {
           "_id": "$category",
           "count": {"$sum": 1},
           "total_revenue": {"$sum": "$price"}
         }}
       ]
    
    2. Average with filter:
       [
         {"$match": {"status": "completed"}},
         {"$group": {
           "_id": "$product_id",
           "avg_price": {"$avg": "$price"},
           "total_sold": {"$sum": "$quantity"}
         }}
       ]
    
    3. Multi-stage pipeline:
       [
         {"$match": {"date": {"$gte": "2024-01-01"}}},
         {"$group": {
           "_id": {"year": {"$year": "$date"}, "month": {"$month": "$date"}},
           "revenue": {"$sum": "$amount"},
           "order_count": {"$sum": 1}
         }},
         {"$sort": {"_id.year": 1, "_id.month": 1}},
         {"$limit": 12}
       ]
    
    4. Calculate average session duration (CORRECT way):
       [
         {"$group": {
           "_id": "$user_id",
           "avg_session_seconds": {"$avg": "$session_duration_seconds"},
           "total_sessions": {"$sum": 1}
         }}
       ]
    
    5. Top N with projection:
       [
         {"$match": {"active": true}},
         {"$sort": {"revenue": -1}},
         {"$limit": 10},
         {"$project": {
           "name": 1,
           "revenue": 1,
           "revenue_formatted": {"$concat": ["$", {"$toString": "$revenue"}]}
         }}
       ]
    
    Common stages:
    - $match: Filter documents (use early for performance)
    - $group: Group and aggregate with accumulators
    - $project: Reshape documents, compute fields
    - $sort: Order results
    - $limit, $skip: Pagination
    - $lookup: Join collections
    - $unwind: Deconstruct arrays
    - $addFields: Add computed fields
    """
    return DatabaseManager().execute_aggregate(collection_name, pipeline)

@mcp.tool()
def count_documents(collection_name: str, filter: Dict = {}) -> int:
    """
    Count documents matching a filter query.
    
    Args:
        collection_name: Name of the collection.
        filter: MongoDB filter query (same format as run_find_query).
    
    Returns:
        Integer count of matching documents.
    
    Examples:
        1. Total documents: filter={}
        2. Count by status: filter={"status": "active"}
        3. Count in range: filter={"price": {"$gte": 100, "$lte": 500}}
    """
    return DatabaseManager().count_documents(collection_name, filter)

if __name__ == "__main__":
    # Run the server using SSE (Server-Sent Events) over HTTP
    print("Starting MongoDB MCP Server on http://127.0.0.1:8001/sse")
    mcp.run(transport="sse", host="127.0.0.1", port=8001)