import os
import json
import datetime
import logging
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from bson import ObjectId, json_util
from dotenv import load_dotenv
from .models import SchemaMetadata, SchemaField, QueryResult, CollectionModel

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

class DatabaseManager:
    _instance = None
    _client: Optional[MongoClient] = None
    _db: Optional[Any] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._init_db()

    def _init_db(self):
        """Initialize the database connection using environment variables."""
        mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        db_name = os.getenv("MONGODB_DB_NAME", "ecommerce_analytics")
        
        try:
            self._client = MongoClient(mongo_uri)
            self._db = self._client[db_name]
            logger.info(f"Connected to MongoDB: {db_name} at {mongo_uri}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def list_collections(self) -> List[str]:
        """List all collections in the database."""
        return self._db.list_collection_names()

    def get_schema(self, collection_name: str, sample_size: int = 100) -> SchemaMetadata:
        """Infer schema from sample documents."""
        collection = self._db[collection_name]
        cursor = collection.find().limit(sample_size)
        documents = list(cursor)
        
        if not documents:
            return SchemaMetadata(
                collection_name=collection_name,
                document_count=0,
                fields=[],
                sample_document="{}"
            )

        schema_fields = {}
        for doc in documents:
            for field, value in doc.items():
                if field == '_id':
                    continue
                
                field_type = type(value).__name__
                if field not in schema_fields:
                    schema_fields[field] = {"type": field_type, "example": value}
                elif schema_fields[field]["type"] != field_type:
                    if isinstance(schema_fields[field]["type"], list):
                        if field_type not in schema_fields[field]["type"]:
                            schema_fields[field]["type"].append(field_type)
                    else:
                        schema_fields[field]["type"] = [schema_fields[field]["type"], field_type]

        fields_list = []
        for name, info in schema_fields.items():
            fields_list.append(SchemaField(
                name=name,
                type=info["type"],
                example=str(info["example"])
            ))

        sample_docs = documents[:3]
        sample_doc_str = json.dumps(sample_docs, indent=2, cls=MongoJSONEncoder)
        
        return SchemaMetadata(
            collection_name=collection_name,
            document_count=collection.count_documents({}),
            fields=fields_list,
            sample_document=sample_doc_str
        )

    def execute_query(self, collection_name: str, filter_query: Dict = None, projection: Dict = None, sort: List = None, limit: int = 5) -> QueryResult:
        """Execute a find query."""
        collection = self._db[collection_name]
        filter_query = filter_query or {}
        
        cursor = collection.find(filter_query, projection)
        
        if sort:
            cursor = cursor.sort(sort)
            
        if limit > 0:
            cursor = cursor.limit(limit)
            
        documents = list(cursor)
        
        # Convert ObjectId and datetime to string for Pydantic compatibility
        serialized_docs = json.loads(json.dumps(documents, cls=MongoJSONEncoder))
        
        return QueryResult(
            documents=serialized_docs,
            count=len(serialized_docs)
        )

    def execute_aggregate(self, collection_name: str, pipeline: List[Dict]) -> QueryResult:
        """Execute an aggregation pipeline."""
        collection = self._db[collection_name]
        documents = list(collection.aggregate(pipeline))
        
        serialized_docs = json.loads(json.dumps(documents, cls=MongoJSONEncoder))
        
        return QueryResult(
            documents=serialized_docs,
            count=len(serialized_docs)
        )

    def count_documents(self, collection_name: str, filter_query: Dict = None) -> int:
        """Count documents matching a filter."""
        collection = self._db[collection_name]
        filter_query = filter_query or {}
        return collection.count_documents(filter_query)
