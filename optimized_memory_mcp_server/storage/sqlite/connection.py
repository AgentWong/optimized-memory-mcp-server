import asyncio
import aiosqlite
import logging
from typing import AsyncGenerator, List, Optional, Dict, Any, Tuple, Set
from contextlib import asynccontextmanager
import time
from datetime import datetime, timedelta

# Cache configuration constants
DEFAULT_CACHE_TTL = 300  # 5 minutes
MAX_PREPARED_STATEMENTS = 100
MAX_QUERY_CACHE_SIZE = 1000
CACHE_CLEANUP_INTERVAL = 60  # 1 minute

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(
        self, 
        db_path: str, 
        pool_size: int = 5, 
        echo: bool = False,
        cache_ttl: int = 300  # 5 minutes default TTL
    ) -> None:
        self.db_path: str = db_path
        self.echo: bool = echo
        self._pool_size: int = pool_size
        self._pool: List[aiosqlite.Connection] = []
        self._pool_semaphore: asyncio.Semaphore = asyncio.Semaphore(pool_size)
        self._prepared_statements: Dict[str, aiosqlite.Statement] = {}
        self._query_cache: Dict[str, Tuple[Any, float]] = {}  # (result, timestamp)
        self._cache_ttl = cache_ttl
        self._last_cleanup = time.time()
        self._cleanup_interval = 60  # Cleanup every minute

    async def prepare_cached(self, conn: aiosqlite.Connection, sql: str) -> aiosqlite.Statement:
        """Get or create a cached prepared statement with usage tracking."""
        if sql not in self._prepared_statements:
            if len(self._prepared_statements) >= MAX_PREPARED_STATEMENTS:
                # Remove least recently used statement
                lru_sql = min(
                    self._prepared_statements.keys(),
                    key=lambda k: self._prepared_statements[k].last_used
                )
                await self._prepared_statements[lru_sql].close()
                del self._prepared_statements[lru_sql]
            
            stmt = await conn.prepare(sql)
            stmt.last_used = time.time()  # Add usage tracking
            self._prepared_statements[sql] = stmt
        else:
            stmt = self._prepared_statements[sql]
            stmt.last_used = time.time()  # Update usage timestamp
            
        return stmt

    def get_cached_query(self, key: str) -> Optional[Any]:
        """Get cached query result if not expired."""
        if key in self._query_cache:
            result, timestamp = self._query_cache[key]
            if time.time() - timestamp <= self._cache_ttl:
                return result
            del self._query_cache[key]
        return None

    def _get_cache_key(self, sql: str, params: tuple = None) -> str:
        """Generate a unique cache key for a query."""
        if params:
            return f"{sql}:{hash(params)}"
        return sql

    def cache_query(self, key: str, value: Any, ttl: int = None) -> None:
        """Cache a query result with optional custom TTL."""
        if ttl is None:
            ttl = self._cache_ttl

        # Implement LRU-style eviction if cache is full
        if len(self._query_cache) >= MAX_QUERY_CACHE_SIZE:
            current_time = time.time()
            # First try to remove expired entries
            expired_keys = [
                k for k, (_, ts, _) in self._query_cache.items()
                if current_time - ts > self._cache_ttl
            ]
            for k in expired_keys:
                del self._query_cache[k]
                
            # If still too full, remove oldest entries
            if len(self._query_cache) >= MAX_QUERY_CACHE_SIZE:
                sorted_keys = sorted(
                    self._query_cache.keys(),
                    key=lambda k: self._query_cache[k][1]
                )
                for k in sorted_keys[:100]:  # Remove oldest 100 entries
                    del self._query_cache[k]

        self._query_cache[key] = (value, time.time(), ttl)

    async def _cleanup_caches(self) -> None:
        """Enhanced cache cleanup."""
        current_time = time.time()
        if current_time - self._last_cleanup < CACHE_CLEANUP_INTERVAL:
            return

        # Clean up query cache
        expired_keys = [
            k for k, (_, ts, ttl) in self._query_cache.items()
            if current_time - ts > (ttl or self._cache_ttl)
        ]
        for k in expired_keys:
            del self._query_cache[k]

        # Clean up prepared statements
        if len(self._prepared_statements) > MAX_PREPARED_STATEMENTS:
            # Keep most recently used statements
            statements_to_remove = sorted(
                self._prepared_statements.items(),
                key=lambda x: x[1].last_used,
                reverse=True
            )[MAX_PREPARED_STATEMENTS:]
            
            for sql, stmt in statements_to_remove:
                try:
                    await stmt.close()
                    del self._prepared_statements[sql]
                except Exception as e:
                    logger.warning(f"Error closing prepared statement: {e}")

        self._last_cleanup = current_time

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """Get a connection from the pool or create a new one."""
        await self._cleanup_caches()
        
        async with self._pool_semaphore:
            conn = None
            try:
                if not self._pool:
                    conn = await aiosqlite.connect(self.db_path)
                    await conn.execute("PRAGMA journal_mode=WAL")
                    await conn.execute("PRAGMA synchronous=NORMAL")
                    await conn.execute("PRAGMA cache_size=-2000")  # 2MB cache
                    await conn.execute("PRAGMA temp_store=MEMORY")
                    await conn.execute("PRAGMA mmap_size=30000000000")  # 30GB mmap
                    conn.row_factory = aiosqlite.Row
                else:
                    conn = self._pool.pop()
                yield conn
            except Exception as e:
                logger.error(f"Database connection error: {e}")
                if conn:
                    await conn.close()
                raise
            else:
                if conn:
                    self._pool.append(conn)

    @asynccontextmanager
    async def transaction(self, conn: aiosqlite.Connection):
        """Manage database transactions."""
        try:
            await conn.execute("BEGIN")
            yield
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    async def cleanup(self):
        """Clean up database connections and cached statements."""
        # Close all prepared statements
        for stmt in self._prepared_statements.values():
            try:
                await stmt.close()
            except Exception as e:
                logger.warning(f"Error closing prepared statement: {e}")
        self._prepared_statements.clear()
        
        # Clear query cache
        self._query_cache.clear()
        
        # Close all connections
        for conn in self._pool:
            await conn.close()
        self._pool.clear()

    def invalidate_cache(self, pattern: str = None) -> None:
        """Invalidate cache entries matching pattern or all if pattern is None."""
        if pattern is None:
            self._query_cache.clear()
        else:
            self.invalidate_cache_pattern(pattern)

    def invalidate_cache_pattern(self, pattern: str) -> None:
        """Invalidate all cache entries matching a pattern."""
        keys_to_remove = [
            k for k in self._query_cache.keys()
            if pattern in k
        ]
        for k in keys_to_remove:
            del self._query_cache[k]

    async def execute_cached(
        self,
        conn: aiosqlite.Connection,
        sql: str,
        params: tuple = None,
        ttl: int = None
    ) -> List[Dict[str, Any]]:
        """Execute a query with caching."""
        cache_key = self._get_cache_key(sql, params)
        
        # Check cache first
        cached_result = self.get_cached_query(cache_key)
        if cached_result is not None:
            return cached_result

        # Prepare and execute
        stmt = await self.prepare_cached(conn, sql)
        if params:
            cursor = await stmt.execute(params)
        else:
            cursor = await stmt.execute()
            
        rows = await cursor.fetchall()
        result = [dict(row) for row in rows]
        
        # Cache the result
        self.cache_query(cache_key, result, ttl)
        return result
