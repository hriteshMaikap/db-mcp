from typing import Dict, Any
from .base import MongoDBToolBase

class ConnectTool(MongoDBToolBase):
    @property
    def name(self) -> str:
        return "connect"
    
    @property
    def description(self) -> str:
        return "Connect to MongoDB instance and test the connection"
    
    async def execute(self, **kwargs) -> Dict[str, Any]:
        try:
            await self.session.connect(self.config)
            
            # Test connection by listing databases
            client = self.session.client
            db_names = client.list_database_names()
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Successfully connected to MongoDB at {self.config.connection_string}"
                    },
                    {
                        "type": "text",
                        "text": f"Current database: {self.config.database_name}"
                    },
                    {
                        "type": "text",
                        "text": f"Available databases: {', '.join(db_names)}"
                    }
                ]
            }
        except Exception as e:
            return self.handle_error(e)