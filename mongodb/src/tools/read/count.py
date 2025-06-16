from typing import Dict, Any, Optional
from ...tools.base import MongoDBToolBase

class CountTool(MongoDBToolBase):
    @property
    def name(self) -> str:
        return "count"
    
    @property
    def description(self) -> str:
        return "Count documents in a MongoDB collection with optional filtering"
    
    async def execute(
        self,
        collection: str,
        database: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        try:
            mongo_collection = self.session.get_collection(collection, database)
            count = mongo_collection.count_documents(filter or {})
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Found {count} documents in collection '{collection}'"
                    }
                ]
            }
            
        except Exception as e:
            return self.handle_error(e)