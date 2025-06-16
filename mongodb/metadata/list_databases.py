from typing import Dict, Any
from mongodb.src.tools.base import MongoDBToolBase

class ListDatabasesTool(MongoDBToolBase):
    @property
    def name(self) -> str:
        return "list_databases"
    
    @property
    def description(self) -> str:
        return "List all databases in the MongoDB instance"
    
    async def execute(self, **kwargs) -> Dict[str, Any]:
        try:
            if not self.session.client:
                return self.handle_error(Exception("Not connected to MongoDB"))
            
            db_info = self.session.client.list_databases()
            databases = [db['name'] for db in db_info]
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Found {len(databases)} databases"
                    },
                    {
                        "type": "text",
                        "text": f"Databases: {', '.join(databases)}"
                    }
                ]
            }
            
        except Exception as e:
            return self.handle_error(e)