import os
import json
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from openai import OpenAI  # ✅ New syntax for openai>=1.0.0
except ImportError:
    try:
        import openai  # Fallback for older versions
    except ImportError:
        openai = None

class LLMQueryAdapter:
    """
    Adapter to convert natural language queries to SQL using LLM.
    Supports OpenAI API (GPT-4, GPT-3.5-turbo).
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-3.5-turbo"):
        if not openai:
            raise ImportError("openai not installed. Run: pip install openai")
        
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not set in environment")
        
        # ✅ Use new OpenAI client
        try:
            self.client = OpenAI(api_key=self.api_key)
        except Exception:
            # Fallback for older API
            import openai
            openai.api_key = self.api_key
            self.client = None
        
        self.model = model
        self.max_tokens = 500

    def generate_sql(self, nl_query: str, schema: Dict, db_type: str = "postgres") -> str:
        """
        Generate SQL from natural language query using LLM.
        
        Args:
            nl_query: Natural language question (e.g., "show me top 10 products by price")
            schema: JSON schema with properties and DB mappings
            db_type: Database type (postgres, mongo, neo4j)
        
        Returns:
            SQL query string
        """
        # Build schema context for LLM
        properties = schema.get("properties", {})
        schema_str = json.dumps(properties, indent=2)
        
        # System prompt instructs LLM to generate safe, parameterized SQL
        system_prompt = f"""You are a SQL expert. Convert natural language queries to {db_type.upper()} SQL.
        
SCHEMA:
{schema_str}

RULES:
1. Only generate SELECT statements
2. Use parameterized queries (e.g., WHERE id = %s)
3. Limit results to 1000 rows max
4. Return ONLY the SQL query, no explanation
5. Use table name inferred from schema context
6. Never use DROP, DELETE, UPDATE, INSERT statements

Example:
NL: "top 5 products"
SQL: SELECT * FROM products ORDER BY price DESC LIMIT 5;
"""
        
        try:
            if self.client:
                # ✅ New API style
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": nl_query}
                    ],
                    max_tokens=self.max_tokens,
                    temperature=0.2,
                )
                sql = response.choices[0].message.content.strip()
            else:
                # Fallback for old API
                import openai
                response = openai.ChatCompletion.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": nl_query}
                    ],
                    max_tokens=self.max_tokens,
                    temperature=0.2,
                )
                sql = response["choices"][0]["message"]["content"].strip()
            
            logger.info(f"Generated SQL: {sql}")
            return sql
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            raise

class QuerySanitizer:
    """
    Sanitizes SQL queries to prevent injection and enforce safety rules.
    """
    
    ALLOWED_KEYWORDS = {
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "BETWEEN",
        "LIKE", "ORDER", "BY", "LIMIT", "OFFSET", "GROUP", "HAVING",
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AS",
        "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",
    }
    
    FORBIDDEN_KEYWORDS = {
        "DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "ALTER", "TRUNCATE",
        "EXEC", "EXECUTE", "SCRIPT", "PRAGMA", "VACUUM", "ANALYZE",
    }
    
    @staticmethod
    def sanitize(query: str) -> bool:
        """
        Check if query is safe to execute.
        
        Args:
            query: SQL query string
            
        Returns:
            True if safe, False otherwise
        """
        query_upper = query.upper().strip()
        
        # Check for forbidden keywords
        for keyword in QuerySanitizer.FORBIDDEN_KEYWORDS:
            if keyword in query_upper:
                logger.warning(f"Forbidden keyword detected: {keyword}")
                return False
        
        # Must start with SELECT
        if not query_upper.startswith("SELECT"):
            logger.warning("Query does not start with SELECT")
            return False
        
        # Check for SQL injection patterns
        injection_patterns = [
            r"'.*OR.*'",  # OR 1=1
            r"--",        # SQL comments
            r"/\*.*\*/",  # Block comments
            r"xp_",       # Extended stored procedures
            r"sp_",       # System stored procedures
        ]
        import re
        for pattern in injection_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                logger.warning(f"Potential injection pattern: {pattern}")
                return False
        
        return True

class QueryExecutor:
    """
    Safely executes SQL queries against databases with timeout and result limits.
    """
    
    def __init__(self, engine: str = "postgres", conn_config: Optional[Dict] = None):
        self.engine = engine.lower()
        self.conn_config = conn_config or {}
        self.max_rows = 1000
        self.timeout = 30
    
    def execute(self, query: str) -> List[Dict]:
        """
        Execute sanitized SQL query and return results.
        
        Args:
            query: SQL query (must be sanitized)
            
        Returns:
            List of result rows as dicts
        """
        if not QuerySanitizer.sanitize(query):
            raise ValueError("Query failed safety checks")
        
        if self.engine == "postgres":
            return self._execute_postgres(query)
        elif self.engine == "mongo":
            return self._execute_mongo(query)
        else:
            raise ValueError(f"Unsupported engine: {self.engine}")
    
    def _execute_postgres(self, query: str) -> List[Dict]:
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor  # ✅ Add this
        except ImportError:
            raise ImportError("psycopg2 not installed")
        
        try:
            conn = psycopg2.connect(
                host=self.conn_config.get("host", "localhost"),
                port=self.conn_config.get("port", 5432),
                dbname=self.conn_config.get("dbname"),
                user=self.conn_config.get("user"),
                password=self.conn_config.get("password"),
                connect_timeout=self.timeout,
            )
            # Use cursor factory to get dict-like rows and use context managers to ensure cleanup
            with conn.cursor(cursor_factory=RealDictCursor) as cur:  # ✅ Use context manager
                cur.execute(query)
                rows = cur.fetchmany(self.max_rows)
                result = [dict(r) for r in rows]
            conn.close()
            logger.info(f"Query returned {len(result)} rows")
            return result
        except Exception as e:
            logger.error(f"Postgres execution failed: {e}")
            try:
                conn.close()
            except Exception:
                pass
            raise
    
    def _execute_mongo(self, query: str) -> List[Dict]:
        """
        Placeholder for MongoDB aggregation pipeline execution.
        In practice, convert SQL to MongoDB aggregation syntax.
        """
        logger.warning("MongoDB execution not fully implemented; returning empty results")
        return []