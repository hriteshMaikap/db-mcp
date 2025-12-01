from typing import List, Any, Optional, Dict, Union
from pydantic import BaseModel, Field

class CollectionModel(BaseModel):
    name: str
    count: int

class SchemaField(BaseModel):
    name: str
    type: Union[str, List[str]]
    example: Any

class SchemaMetadata(BaseModel):
    collection_name: str
    document_count: int
    fields: List[SchemaField]
    sample_document: str

class QueryResult(BaseModel):
    documents: List[Dict[str, Any]]
    count: int
