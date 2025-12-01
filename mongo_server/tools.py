from typing import List, Dict, Optional, Any
from .db import DatabaseManager
from .models import SchemaMetadata, QueryResult

def list_collections() -> List[str]:
    """
    List all collections in the MongoDB database.
    """
    return DatabaseManager().list_collections()

def get_schema(collection_name: str) -> SchemaMetadata:
    """
    Get the schema of a collection by inferring from sample documents.
    Args:
        collection_name: The name of the collection.
    """
    return DatabaseManager().get_schema(collection_name)

def run_find_query(collection_name: str, filter: Dict = {}, projection: Optional[Dict] = None, sort: Optional[List] = None, limit: int = 5) -> QueryResult:
    """
    Execute a MongoDB find query.
    Args:
        collection_name: The name of the collection.
        filter: MongoDB filter query (e.g., {"price": {"$gt": 100}}).
        projection: Fields to include/exclude (e.g., {"name": 1, "price": 1}).
        sort: Sort criteria (e.g., [("price", -1)]).
        limit: Maximum number of documents to return (default 5).
    """
    return DatabaseManager().execute_query(collection_name, filter, projection, sort, limit)

def run_aggregate_query(collection_name: str, pipeline: List[Dict]) -> QueryResult:
    """
    Execute a MongoDB aggregation pipeline.
    Args:
        collection_name: The name of the collection.
        pipeline: List of aggregation stages (e.g., [{"$match": ...}, {"$group": ...}]).
    """
    return DatabaseManager().execute_aggregate(collection_name, pipeline)
