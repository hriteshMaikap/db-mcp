from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import json
from bson import json_util
from ...config import MongoDBConfig
from ...errors import MongoDBError, ErrorCodes

class MongoDBSession:
    def __init__(self):
        self.client: Optional[MongoClient] = None
        self.current_database: Optional[str] = None
        
    async def connect(self, config: MongoDBConfig) -> None:
        try:
            self.client = MongoClient(
                config.connection_string,
                connectTimeoutMS=config.connect_timeout,
                serverSelectionTimeoutMS=config.server_selection_timeout
            )
            # Test connection
            self.client.admin.command('ping')
            self.current_database = config.database_name
        except Exception as e:
            raise MongoDBError(
                ErrorCodes.MISCONFIGURED_CONNECTION_STRING,
                f"Failed to connect to MongoDB: {str(e)}"
            )
    
    def get_database(self, database_name: Optional[str] = None) -> Database:
        if not self.client:
            raise MongoDBError(ErrorCodes.NOT_CONNECTED_TO_MONGODB, "Not connected to MongoDB")
        
        db_name = database_name or self.current_database
        if not db_name:
            raise MongoDBError(ErrorCodes.DATABASE_NOT_FOUND, "No database specified")
        
        return self.client[db_name]
    
    def get_collection(self, collection_name: str, database_name: Optional[str] = None) -> Collection:
        database = self.get_database(database_name)
        return database[collection_name]

class MongoDBToolBase(ABC):
    def __init__(self, session: MongoDBSession, config: MongoDBConfig):
        self.session = session
        self.config = config
    
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        pass
    
    @abstractmethod
    async def execute(self, **kwargs) -> Dict[str, Any]:
        pass
    
    def format_documents(self, documents: List[Dict[str, Any]], collection_name: str) -> Dict[str, Any]:
        """Format MongoDB documents for MCP response"""
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Found {len(documents)} documents in collection '{collection_name}'"
                }
            ] + [
                {
                    "type": "text", 
                    "text": json.dumps(doc, default=json_util.default, indent=2)
                } for doc in documents
            ]
        }
    
    def handle_error(self, error: Exception) -> Dict[str, Any]:
        """Handle and format errors for MCP response"""
        if isinstance(error, MongoDBError):
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"MongoDB Error: {error.message}"
                    }
                ],
                "isError": True
            }
        
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Unexpected error: {str(error)}"
                }
            ],
            "isError": True
        }