"""Cache implementations for SQLite operations."""
from typing import Any, Dict, Optional
import time
import logging
from functools import lru_cache
import aiosqlite

logger = logging.getLogger(__name__)

class StatementCache:
    """Cache for prepared SQL statements."""
    
    def __init__(self, max_size: int = 100):
        self._cache: Dict[str, aiosqlite.Statement] = {}
        self._max_size = max_size
        
    async def get_statement(
        self,
        conn: aiosqlite.Connection,
        sql: str
    ) -> aiosqlite.Statement:
        """Get cached statement or prepare new one."""
        if sql not in self._cache:
            if len(self._cache) >= self._max_size:
                # Remove oldest entry
                self._cache.pop(next(iter(self._cache)))
            self._cache[sql] = await conn.prepare(sql)
        return self._cache[sql]
    
    def clear(self):
        """Clear the statement cache."""
        self._cache.clear()

class QueryResultCache:
    """Cache for query results."""
    
    def __init__(self, ttl: int = 300):  # 5 min default TTL
        self._cache: Dict[str, tuple[Any, float]] = {}
        self._ttl = ttl
        
    def get(self, key: str) -> Optional[Any]:
        """Get cached result if not expired."""
        if key in self._cache:
            result, timestamp = self._cache[key]
            if time.time() - timestamp <= self._ttl:
                return result
            del self._cache[key]
        return None
        
    def set(self, key: str, value: Any):
        """Cache a query result."""
        self._cache[key] = (value, time.time())
        
    def invalidate(self, key: str):
        """Remove specific entry from cache."""
        self._cache.pop(key, None)
        
    def clear(self):
        """Clear all cached results."""
        self._cache.clear()
