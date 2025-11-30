from typing import List, Any, Optional, Dict
from pydantic import BaseModel, Field

class ColumnMetadata(BaseModel):
    name: str
    type: str
    primary_key: bool = False
    nullable: bool = True
    comment: Optional[str] = None

class TableMetadata(BaseModel):
    name: str
    columns: List[ColumnMetadata]
    comment: Optional[str] = None

class SchemaMetadata(BaseModel):
    tables: List[TableMetadata]
    database_name: Optional[str] = None

class QueryResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
