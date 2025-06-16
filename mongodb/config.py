import os
from typing import Optional
from dataclasses import dataclass

@dataclass
class MongoDBConfig:
    connection_string: str
    database_name: str
    connect_timeout: int = 10000
    server_selection_timeout: int = 5000
    
    @classmethod
    def from_env(cls) -> 'MongoDBConfig':
        return cls(
            connection_string=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
            database_name=os.getenv("MONGODB_DB_NAME", "ecommerce_analytics"),
            connect_timeout=int(os.getenv("MONGODB_CONNECT_TIMEOUT", "10000")),
            server_selection_timeout=int(os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT", "5000"))
        )