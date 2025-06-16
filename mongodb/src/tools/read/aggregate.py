from typing import Dict, Any, Optional, List
from ...tools.base import MongoDBToolBase

class AggregateTool(MongoDBToolBase):
    @property
    def name(self) -> str:
        return "aggregate"
    
    @property
    def description(self) -> str:
        return "Run aggregation pipeline against a MongoDB collection"
    
    async def execute(
        self,
        collection: str,
        pipeline: List[Dict[str, Any]],
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            mongo_collection = self.session.get_collection(collection, database)
            documents = list(mongo_collection.aggregate(pipeline))
            
            return self.format_documents(documents, collection)
            
        except Exception as e:
            return self.handle_error(e)