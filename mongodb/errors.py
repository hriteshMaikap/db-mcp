from enum import Enum
from typing import Optional

class ErrorCodes(Enum):
    NOT_CONNECTED_TO_MONGODB = "not_connected_to_mongodb"
    MISCONFIGURED_CONNECTION_STRING = "misconfigured_connection_string"
    COLLECTION_NOT_FOUND = "collection_not_found"
    DATABASE_NOT_FOUND = "database_not_found"
    INVALID_QUERY = "invalid_query"
    CONNECTION_TIMEOUT = "connection_timeout"

class MongoDBError(Exception):
    def __init__(self, code: ErrorCodes, message: str, details: Optional[str] = None):
        self.code = code
        self.message = message
        self.details = details
        super().__init__(f"{code.value}: {message}")