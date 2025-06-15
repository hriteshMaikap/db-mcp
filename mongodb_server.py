import json
from datetime import datetime
from typing import List, Dict, Any
from pymongo import MongoClient
from bson import ObjectId
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field
import tiktoken

# MongoDB configuration
MONGODB_URI = "mongodb://localhost:27017"

# Token limits
MAX_REQUEST_TOKENS = 6000
MAX_RESPONSE_TOKENS = 6000

# Initialize FastMCP server
mcp = FastMCP("MongoDB", port=8002)

# Simple JSON encoder for MongoDB data
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Structured output models
class DataDisplay(BaseModel):
    """Display of sample data and basic information"""
    sample_documents: List[Dict[str, Any]] = Field(description="Sample documents from the collection")
    total_count: int = Field(description="Total number of documents in the collection")
    collection_name: str = Field(description="Name of the collection")
    database_name: str = Field(description="Name of the database")

class NumericalInsights(BaseModel):
    """Numerical insights and analytics"""
    document_count: int = Field(description="Total document count")
    field_analysis: Dict[str, List[str]] = Field(description="Field types analysis")
    query_results: List[Dict[str, Any]] = Field(description="Results from analytical queries")
    execution_time_ms: float = Field(description="Query execution time in milliseconds")

class AnalysisResult(BaseModel):
    """Complete structured analysis result"""
    data_display: DataDisplay = Field(description="Sample data and basic information")
    numerical_insights: NumericalInsights = Field(description="Numerical analysis and metrics")
    textual_summary: str = Field(description="Human-readable summary of the analysis")

def count_tokens(text: str) -> int:
    """Count tokens in text using tiktoken"""
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(text))
    except Exception:
        # Fallback to rough estimation
        return len(text.split()) * 1.3

def truncate_to_token_limit(text: str, max_tokens: int) -> str:
    """Truncate text to stay within token limit"""
    current_tokens = count_tokens(text)
    if current_tokens <= max_tokens:
        return text
    
    # Rough truncation - could be improved
    words = text.split()
    target_words = int(len(words) * (max_tokens / current_tokens))
    return " ".join(words[:target_words]) + "... [TRUNCATED DUE TO TOKEN LIMIT]"

def ensure_token_limits(data: List[Dict], max_items: int = 10) -> List[Dict]:
    """Ensure data doesn't exceed reasonable limits"""
    if len(data) > max_items:
        return data[:max_items]
    return data

@mcp.tool()
def list_databases() -> str:
    """List all available databases in MongoDB"""
    try:
        client = MongoClient(MONGODB_URI)
        databases = client.list_database_names()
        # Filter out system databases
        user_databases = [db for db in databases if db not in ['admin', 'local', 'config']]
        client.close()
        
        result = {
            "available_databases": user_databases,
            "mongodb_uri": MONGODB_URI,
            "total_databases": len(user_databases)
        }
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error listing databases: {str(e)}"

@mcp.tool()
def list_collections(database_name: str) -> str:
    """List all collections in a specific database"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[database_name]
        collections = db.list_collection_names()
        
        # Get basic stats for each collection
        collection_info = []
        for collection_name in collections:
            try:
                count = db[collection_name].count_documents({})
                collection_info.append({
                    "name": collection_name,
                    "document_count": count
                })
            except Exception:
                collection_info.append({
                    "name": collection_name,
                    "document_count": "unknown"
                })
        
        client.close()
        
        result = {
            "database": database_name,
            "collections": collection_info,
            "total_collections": len(collections)
        }
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error listing collections: {str(e)}"

@mcp.tool()
def get_sample_data(database_name: str, collection_name: str, limit: int = 5) -> str:
    """Get sample documents from a collection - simple version for basic exploration"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[database_name]
        collection = db[collection_name]
        
        # Get sample documents
        sample_docs = list(collection.find().limit(min(limit, 10)))
        total_count = collection.count_documents({})
        
        client.close()
        
        result = {
            "database": database_name,
            "collection": collection_name,
            "total_documents": total_count,
            "sample_size": len(sample_docs),
            "sample_documents": sample_docs
        }
        
        result_str = json.dumps(result, indent=2, cls=MongoJSONEncoder)
        return truncate_to_token_limit(result_str, MAX_RESPONSE_TOKENS)
        
    except Exception as e:
        return f"Error getting sample data: {str(e)}"

@mcp.tool()
def get_collection_stats(database_name: str, collection_name: str) -> str:
    """Get basic statistics about a collection with structured output"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[database_name]
        collection = db[collection_name]
        
        start_time = datetime.now()
        
        # Get basic stats
        total_count = collection.count_documents({})
        
        # Get field analysis from sample with token limits
        sample_docs = list(collection.find().limit(20))
        sample_docs = ensure_token_limits(sample_docs, 10)
        
        field_types = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(type(value).__name__)
        
        # Convert sets to lists for JSON serialization
        for key in field_types:
            field_types[key] = list(field_types[key])
        
        # Get some basic analytics
        analytics_results = []
        
        # Count unique values for first string field (if exists)
        string_fields = [k for k, v in field_types.items() if 'str' in v and k != '_id']
        if string_fields:
            first_string_field = string_fields[0]
            try:
                pipeline = [
                    {"$group": {"_id": f"${first_string_field}", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 5}
                ]
                field_counts = list(collection.aggregate(pipeline))
                analytics_results.extend(field_counts)
            except Exception:
                pass
        
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Create structured result
        result = AnalysisResult(
            data_display=DataDisplay(
                sample_documents=sample_docs[:3],  # Show only 3 for display
                total_count=total_count,
                collection_name=collection_name,
                database_name=database_name
            ),
            numerical_insights=NumericalInsights(
                document_count=total_count,
                field_analysis=field_types,
                query_results=analytics_results,
                execution_time_ms=execution_time
            ),
            textual_summary=f"Statistical analysis of '{collection_name}' in database '{database_name}': "
                           f"Contains {total_count} documents with {len(field_types)} unique fields. "
                           f"Field types identified: {', '.join(field_types.keys())}. "
                           f"Analysis of {len(sample_docs)} sample documents completed in {execution_time:.2f}ms."
        )
        
        client.close()
        
        # Convert to JSON and ensure token limits
        result_str = result.model_dump_json(indent=2)
        result_str = truncate_to_token_limit(result_str, MAX_RESPONSE_TOKENS)
        
        return result_str
        
    except Exception as e:
        return f"Error getting collection stats: {str(e)}"

@mcp.tool()
def analyze_mongodb_collection(database_name: str, collection_name: str, analysis_type: str = "overview") -> str:
    """Comprehensive MongoDB collection analysis with structured output
    
    analysis_type options:
    - 'overview': General collection overview with samples and stats
    - 'recent': Get most recent documents
    - 'aggregation': Perform smart aggregation based on data types
    - 'field_analysis': Detailed field type and distribution analysis
    - 'time_series': Time-based analysis if date fields exist
    """
    try:
        client = MongoClient(MONGODB_URI)
        db = client[database_name]
        collection = db[collection_name]
        
        start_time = datetime.now()
        
        # Get basic info
        total_count = collection.count_documents({})
        sample_docs = list(collection.find().limit(5))
        sample_docs = ensure_token_limits(sample_docs, 5)
        
        # Field analysis
        field_types = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(type(value).__name__)
        
        # Convert sets to lists
        for key in field_types:
            field_types[key] = list(field_types[key])
        
        query_results = []
        analysis_description = ""
        
        if analysis_type == "overview":
            # Basic overview
            query_results = sample_docs
            analysis_description = "General collection overview"
            
        elif analysis_type == "recent":
            # Most recent documents
            recent_docs = list(collection.find().sort("_id", -1).limit(5))
            query_results = ensure_token_limits(recent_docs, 5)
            analysis_description = "5 most recent documents"
            
        elif analysis_type == "aggregation":
            # Smart aggregation based on available fields
            string_fields = [k for k, v in field_types.items() if 'str' in v and k != '_id']
            if string_fields:
                field = string_fields[0]
                pipeline = [
                    {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 10}
                ]
                agg_results = list(collection.aggregate(pipeline))
                query_results = ensure_token_limits(agg_results, 10)
                analysis_description = f"Aggregation by {field} field"
            else:
                query_results = [{"message": "No suitable fields for aggregation"}]
                analysis_description = "No aggregation possible"
                
        elif analysis_type == "field_analysis":
            # Detailed field analysis
            field_stats = {}
            for field, types in field_types.items():
                if field != '_id':
                    try:
                        # Count non-null values
                        non_null_count = collection.count_documents({field: {"$ne": None}})
                        field_stats[field] = {
                            "types": types,
                            "non_null_count": non_null_count,
                            "null_percentage": round((total_count - non_null_count) / total_count * 100, 2)
                        }
                    except:
                        field_stats[field] = {"types": types, "non_null_count": "unknown"}
            
            query_results = [field_stats]
            analysis_description = "Detailed field analysis"
            
        elif analysis_type == "time_series":
            # Time-based analysis
            date_fields = [k for k, v in field_types.items() if 'datetime' in v]
            if date_fields:
                date_field = date_fields[0]
                from datetime import timedelta
                week_ago = datetime.now() - timedelta(days=7)
                
                pipeline = [
                    {"$match": {date_field: {"$gte": week_ago}}},
                    {"$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${date_field}"}},
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"_id": 1}}
                ]
                time_results = list(collection.aggregate(pipeline))
                query_results = ensure_token_limits(time_results, 10)
                analysis_description = f"Time series analysis using {date_field}"
            else:
                query_results = [{"message": "No date fields found for time series analysis"}]
                analysis_description = "No time series data available"
        
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        # Create structured result
        result = AnalysisResult(
            data_display=DataDisplay(
                sample_documents=sample_docs[:3],  # Limit for display
                total_count=total_count,
                collection_name=collection_name,
                database_name=database_name
            ),
            numerical_insights=NumericalInsights(
                document_count=total_count,
                field_analysis=field_types,
                query_results=query_results,
                execution_time_ms=execution_time
            ),
            textual_summary=f"Analysis of '{collection_name}' in database '{database_name}': "
                           f"{analysis_description}. Collection contains {total_count} documents "
                           f"with {len(field_types)} fields. Analysis completed in {execution_time:.2f}ms."
        )
        
        client.close()
        
        # Convert to JSON and ensure token limits
        result_str = result.model_dump_json(indent=2)
        result_str = truncate_to_token_limit(result_str, MAX_RESPONSE_TOKENS)
        
        return result_str
        
    except Exception as e:
        return f"Error analyzing collection: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
