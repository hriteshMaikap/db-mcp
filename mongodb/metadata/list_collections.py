from typing import Dict, Any, Optional
from mongodb.src.tools.base import MongoDBToolBase

class ListCollectionsTool(MongoDBToolBase):
    @property
    def name(self) -> str:
        return "list_collections"
    
    @property
    def description(self) -> str:
        return "List all collections in a MongoDB database"
    
    async def execute(self, database: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        try:
            db = self.session.get_database(database)
            collections = db.list_collection_names()
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Found {len(collections)} collections in database '{db.name}'"
                    },
                    {
                        "type": "text",
                        "text": f"Collections: {', '.join(collections)}"
                    }
                ]
            }
            
        except Exception as e:
            return self.handle_error(e)