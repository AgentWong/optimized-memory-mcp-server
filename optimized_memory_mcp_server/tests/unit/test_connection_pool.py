"""Unit tests for SQLite connection pool."""
import pytest
import asyncio
import aiosqlite
from datetime import datetime, timedelta

async def test_pool_initialization(connection_pool):
    """Test connection pool initializes correctly."""
    assert connection_pool._pool_size == 3  # From conftest.py
    assert isinstance(connection_pool._pool_semaphore, asyncio.Semaphore)
    assert connection_pool._pool_semaphore._value == 3
    assert len(connection_pool._pool) == 0  # Pool starts empty
    assert connection_pool._prepared_statements == {}
    assert connection_pool._query_cache == {}

async def test_get_connection(connection_pool):
    """Test getting a connection from the pool."""
    async with connection_pool.get_connection() as conn:
        assert isinstance(conn, aiosqlite.Connection)
        # Test basic query functionality
        async with conn.execute("SELECT 1") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1

async def test_connection_reuse(connection_pool):
    """Test connections are reused."""
    first_conn = None
    async with connection_pool.get_connection() as conn1:
        first_conn = conn1
        
    async with connection_pool.get_connection() as conn2:
        assert conn2 is first_conn  # Connection should be reused

async def test_max_connections(connection_pool):
    """Test pool enforces maximum connections."""
    connections = []
    try:
        # Acquire max connections
        for _ in range(connection_pool._pool_size):
            conn = await connection_pool.get_connection().__aenter__()
            connections.append(conn)
        
        # Try to get one more connection
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(0.1):
                async with connection_pool.get_connection():
                    pass
    finally:
        # Clean up connections
        for conn in connections:
            await connection_pool.get_connection().__aexit__(None, None, None)

async def test_prepared_statement_cache(connection_pool):
    """Test prepared statement caching."""
    sql = "SELECT * FROM entities WHERE name = ?"
    
    async with connection_pool.get_connection() as conn:
        # First preparation should cache
        stmt1 = await connection_pool.prepare_cached(conn, sql)
        assert sql in connection_pool._prepared_statements
        
        # Second preparation should return cached statement
        stmt2 = await connection_pool.prepare_cached(conn, sql)
        assert stmt1 is stmt2

async def test_prepared_statement_lru(connection_pool):
    """Test LRU eviction of prepared statements."""
    async with connection_pool.get_connection() as conn:
        # Fill cache to max size
        for i in range(connection_pool.MAX_PREPARED_STATEMENTS + 1):
            sql = f"SELECT {i}"
            await connection_pool.prepare_cached(conn, sql)
        
        # Verify cache size hasn't exceeded max
        assert len(connection_pool._prepared_statements) <= connection_pool.MAX_PREPARED_STATEMENTS

async def test_query_cache(connection_pool, populated_db):
    """Test query result caching."""
    sql = "SELECT * FROM entities"
    params = ()
    cache_key = connection_pool._get_cache_key(sql, params)
    
    async with connection_pool.get_connection() as conn:
        # First query should cache
        result1 = await connection_pool.execute_cached(conn, sql, params)
        assert cache_key in connection_pool._query_cache
        
        # Second query should use cache
        result2 = await connection_pool.execute_cached(conn, sql, params)
        assert result1 == result2

async def test_query_cache_ttl(connection_pool, populated_db):
    """Test query cache TTL expiration."""
    sql = "SELECT * FROM entities"
    params = ()
    cache_key = connection_pool._get_cache_key(sql, params)
    
    # Set very short TTL for test
    connection_pool._cache_ttl = 0.1
    
    async with connection_pool.get_connection() as conn:
        # Cache the query
        await connection_pool.execute_cached(conn, sql, params)
        assert cache_key in connection_pool._query_cache
        
        # Wait for TTL to expire
        await asyncio.sleep(0.2)
        
        # Cache should be invalid
        assert connection_pool.get_cached_query(cache_key) is None

async def test_cache_invalidation(connection_pool, populated_db):
    """Test cache invalidation functionality."""
    async with connection_pool.get_connection() as conn:
        # Cache some queries
        await connection_pool.execute_cached(conn, "SELECT * FROM entities")
        await connection_pool.execute_cached(conn, "SELECT * FROM relations")
        
        # Invalidate specific pattern
        connection_pool.invalidate_cache_pattern("entities")
        assert "SELECT * FROM relations" in connection_pool._get_cache_key("SELECT * FROM relations")
        assert "SELECT * FROM entities" not in connection_pool._get_cache_key("SELECT * FROM entities")
        
        # Invalidate all
        connection_pool.invalidate_cache()
        assert len(connection_pool._query_cache) == 0

async def test_transaction_management(connection_pool):
    """Test transaction context manager."""
    async with connection_pool.get_connection() as conn:
        # Test successful transaction
        async with connection_pool.transaction(conn):
            await conn.execute("""
                INSERT INTO entities (name, entity_type, observations)
                VALUES (?, ?, ?)
            """, ("test_entity", "test_type", "test_observation"))
        
        # Verify commit worked
        cursor = await conn.execute("SELECT * FROM entities WHERE name = ?", ("test_entity",))
        assert await cursor.fetchone() is not None
        
        # Test failed transaction
        with pytest.raises(Exception):
            async with connection_pool.transaction(conn):
                await conn.execute("INSERT INTO non_existent_table VALUES (1)")
        
        # Verify rollback worked
        cursor = await conn.execute("SELECT * FROM entities WHERE name = ?", ("rollback_test",))
        assert await cursor.fetchone() is None

async def test_metrics_collection(connection_pool):
    """Test metrics are collected during operations."""
    async with connection_pool.get_connection() as conn:
        await connection_pool.execute_cached(
            conn,
            "SELECT * FROM entities",
            query_type="select"
        )
        
        metrics = connection_pool.metrics.query_metrics
        assert "select" in metrics
        assert metrics["select"].total_queries > 0
        assert metrics["select"].cache_hits == 0  # First query, no cache hit

async def test_cleanup(connection_pool):
    """Test cleanup closes all connections and clears caches."""
    # Create some connections and cache some data
    async with connection_pool.get_connection() as conn:
        await connection_pool.execute_cached(conn, "SELECT 1")
        await connection_pool.prepare_cached(conn, "SELECT 1")
    
    # Perform cleanup
    await connection_pool.cleanup()
    
    assert len(connection_pool._pool) == 0
    assert len(connection_pool._prepared_statements) == 0
    assert len(connection_pool._query_cache) == 0
