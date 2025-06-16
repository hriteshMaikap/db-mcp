from typing import Dict, Any, Optional
from ...tools.base import MongoDBToolBase

class CollectionIndexesTool(MongoDBToolBase):
    @property
    def name(self) -> str:
        return "collection_indexes"
    
    @property
    def description(self) -> str:
        return "List all indexes for a MongoDB collection"
    
    async def execute(
        self,
        collection: str,
        database: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            mongo_collection = self.session.get_collection(collection, database)
            indexes = list(mongo_collection.list_indexes())
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Found {len(indexes)} indexes in collection '{collection}'"
                    }
                ] + [
                    {
                        "type": "text",
                        "text": f"Index '{idx.get('name', 'unnamed')}': {idx.get('key', {})}"
                    } for idx in indexes
                ]
            }
            
        except Exception as e:
            return self.handle_error(e)