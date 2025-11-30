import os
import logging
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

from .models import SchemaMetadata, TableMetadata, ColumnMetadata, QueryResult

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    _instance = None
    _engine: Optional[Engine] = None
    _schema_cache: Optional[SchemaMetadata] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._engine is None:
            self._init_db()

    def _init_db(self):
        """Initialize the database connection using environment variables."""
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASS", "")
        host = os.getenv("MYSQL_HOST", "localhost")
        port = os.getenv("MYSQL_PORT", "3306")
        db_name = os.getenv("MYSQL_DB", "test_db")

        # Construct connection string
        # Default to pymysql for MySQL
        db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
        
        try:
            self._engine = create_engine(db_url, pool_pre_ping=True)
            logger.info(f"Connected to database: {db_name} at {host}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def get_schema(self, refresh: bool = False) -> SchemaMetadata:
        """Get database schema, using cache if available."""
        if self._schema_cache and not refresh:
            return self._schema_cache

        logger.info("Refreshing schema cache...")
        inspector = inspect(self._engine)
        tables = []
        
        for table_name in inspector.get_table_names():
            columns = []
            for col in inspector.get_columns(table_name):
                columns.append(ColumnMetadata(
                    name=col['name'],
                    type=str(col['type']),
                    primary_key=col.get('primary_key', False),
                    nullable=col.get('nullable', True),
                    comment=col.get('comment')
                ))
            
            tables.append(TableMetadata(
                name=table_name,
                columns=columns,
                comment=inspector.get_table_comment(table_name).get('text')
            ))

        self._schema_cache = SchemaMetadata(
            tables=tables,
            database_name=os.getenv("MYSQL_DB")
        )
        return self._schema_cache

    def is_read_only(self, query: str) -> bool:
        """Check if a query is read-only (naive check)."""
        forbidden_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 
            'TRUNCATE', 'REPLACE', 'CREATE', 'GRANT', 'REVOKE',
            'COMMIT', 'ROLLBACK' # Transaction control might be risky in this context
        ]
        
        # Normalize query
        query_upper = query.upper().strip()
        
        # Check if it starts with SELECT or SHOW or DESCRIBE or EXPLAIN
        if not (query_upper.startswith('SELECT') or 
                query_upper.startswith('SHOW') or 
                query_upper.startswith('DESCRIBE') or 
                query_upper.startswith('EXPLAIN') or
                query_upper.startswith('WITH')): # CTEs
            return False

        # Check for forbidden keywords inside the query (naive, might flag valid strings)
        # A better approach is to rely on the user/role permissions in the DB, 
        # but this is a safety net.
        # For now, we will trust the startswith check + DB user permissions mostly,
        # but let's be a bit paranoid about multiple statements.
        if ';' in query:
            # Check each statement
            statements = [s.strip() for s in query.split(';') if s.strip()]
            for stmt in statements:
                if not self.is_read_only(stmt):
                    return False
            return True

        return True

    def execute_query(self, query: str) -> QueryResult:
        """Execute a read-only query."""
        if not self.is_read_only(query):
            raise ValueError("Only read-only queries are allowed.")

        with self._engine.connect() as conn:
            result = conn.execute(text(query))
            columns = list(result.keys())
            rows = [list(row) for row in result.fetchall()]
            
            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows)
            )

    def sample_rows(self, table_name: str, n: int = 5) -> QueryResult:
        """Get random sample rows from a table."""
        # Validate table name to prevent injection
        schema = self.get_schema()
        if table_name not in [t.name for t in schema.tables]:
            raise ValueError(f"Table '{table_name}' not found.")

        query = f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT {n}"
        return self.execute_query(query)
