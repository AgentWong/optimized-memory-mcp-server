import asyncio
import aiosqlite
import logging
from typing import AsyncGenerator, List, Optional, Dict, Any, Tuple
from contextlib import asynccontextmanager
import time

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
        """Get or create a cached prepared statement."""
        if sql not in self._prepared_statements:
            # Limit cache size
            if len(self._prepared_statements) >= 100:  # Max 100 prepared statements
                # Remove oldest statement
                oldest_sql = next(iter(self._prepared_statements))
                await self._prepared_statements[oldest_sql].close()
                del self._prepared_statements[oldest_sql]
            
            self._prepared_statements[sql] = await conn.prepare(sql)
        return self._prepared_statements[sql]

    def get_cached_query(self, key: str) -> Optional[Any]:
        """Get cached query result if not expired."""
        if key in self._query_cache:
            result, timestamp = self._query_cache[key]
            if time.time() - timestamp <= self._cache_ttl:
                return result
            del self._query_cache[key]
        return None

    def cache_query(self, key: str, value: Any) -> None:
        """Cache a query result."""
        # Limit cache size
        if len(self._query_cache) >= 1000:  # Max 1000 cached results
            # Remove oldest entries
            current_time = time.time()
            expired_keys = [
                k for k, (_, ts) in self._query_cache.items()
                if current_time - ts > self._cache_ttl
            ]
            for k in expired_keys:
                del self._query_cache[k]
            
            # If still too many entries, remove oldest
            if len(self._query_cache) >= 1000:
                oldest_key = min(
                    self._query_cache.keys(),
                    key=lambda k: self._query_cache[k][1]
                )
                del self._query_cache[oldest_key]
        
        self._query_cache[key] = (value, time.time())

    async def _cleanup_caches(self) -> None:
        """Clean up expired cache entries."""
        current_time = time.time()
        if current_time - self._last_cleanup < self._cleanup_interval:
            return

        # Clean up query cache
        expired_keys = [
            k for k, (_, ts) in self._query_cache.items()
            if current_time - ts > self._cache_ttl
        ]
        for k in expired_keys:
            del self._query_cache[k]

        # Clean up prepared statements older than 1 hour
        for sql, stmt in list(self._prepared_statements.items()):
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
            keys_to_remove = [
                k for k in self._query_cache.keys()
                if pattern in k
            ]
            for k in keys_to_remove:
                del self._query_cache[k]
