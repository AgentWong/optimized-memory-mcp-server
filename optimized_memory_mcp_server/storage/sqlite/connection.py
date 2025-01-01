import asyncio
import aiosqlite
import logging
from typing import AsyncGenerator, List, Optional, Dict, Any
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class SQLiteConnectionPool:
    def __init__(self, db_path: str, pool_size: int = 5, echo: bool = False) -> None:
        self.db_path: str = db_path
        self.echo: bool = echo
        self._pool_size: int = pool_size
        self._pool: List[aiosqlite.Connection] = []
        self._pool_semaphore: asyncio.Semaphore = asyncio.Semaphore(pool_size)
        self._prepared_statements: Dict[str, Any] = {}

    async def prepare_statement(self, conn: aiosqlite.Connection, sql: str) -> Any:
        """Get or create a prepared statement."""
        if sql not in self._prepared_statements:
            self._prepared_statements[sql] = await conn.prepare(sql)
        return self._prepared_statements[sql]

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """Get a connection from the pool or create a new one."""
        async with self._pool_semaphore:
            conn = None
            try:
                if not self._pool:
                    conn = await aiosqlite.connect(self.db_path)
                    await conn.execute("PRAGMA journal_mode=WAL")
                    await conn.execute("PRAGMA synchronous=NORMAL")
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
        """Clean up database connections and prepared statements in the pool."""
        for stmt in self._prepared_statements.values():
            await stmt.close()
        self._prepared_statements.clear()
        
        for conn in self._pool:
            await conn.close()
        self._pool.clear()
