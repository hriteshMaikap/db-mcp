from typing import Dict, Any, Optional, List
from ...tools.base import MongoDBToolBase
from ....errors import MongoDBError, ErrorCodes

class FindTool(MongoDBToolBase):
    @property
    def name(self) -> str:
        return "find"
    
    @property
    def description(self) -> str:
        return "Query documents from a MongoDB collection with optional filtering, projection, sorting, and limiting"
    
    async def execute(
        self,
        collection: str,
        database: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        skip: int = 0
    ) -> Dict[str, Any]:
        try:
            mongo_collection = self.session.get_collection(collection, database)
            
            # Build the query
            cursor = mongo_collection.find(filter or {})
            
            if projection:
                cursor = cursor.projection(projection)
            
            if sort:
                # Convert sort dict to list of tuples for pymongo
                sort_list = [(k, v) for k, v in sort.items()]
                cursor = cursor.sort(sort_list)
            
            if skip > 0:
                cursor = cursor.skip(skip)
            
            cursor = cursor.limit(limit)
            
            # Execute query
            documents = list(cursor)
            
            return self.format_documents(documents, collection)
            
        except Exception as e:
            return self.handle_error(e)