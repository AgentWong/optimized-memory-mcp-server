"""Unit tests for database health checks."""
import pytest
import asyncio
from datetime import datetime, timedelta
import os

async def test_basic_health_status(connection_pool):
    """Test basic health check returns valid status."""
    health = await connection_pool.health_check.check_health()
    assert health["status"] == "healthy"
    assert health["response_time"] >= 0
    assert health["database_size"] > 0

async def test_database_response_time(connection_pool):
    """Test database response time measurement."""
    # Perform a simple query to measure response time
    start_time = datetime.now()
    async with connection_pool.get_connection() as conn:
        await conn.execute("SELECT 1")
    actual_time = (datetime.now() - start_time).total_seconds()
    
    # Check health and verify response time
    health = await connection_pool.health_check.check_health()
    assert "response_time" in health
    assert isinstance(health["response_time"], float)
    assert health["response_time"] > 0
    assert health["response_time"] <= actual_time * 2  # Allow some overhead

async def test_database_size_monitoring(connection_pool, populated_db):
    """Test database size monitoring."""
    health = await connection_pool.health_check.check_health()
    initial_size = health["database_size"]
    
    # Add some data to increase size
    async with connection_pool.get_connection() as conn:
        await conn.execute("""
            INSERT INTO entities (name, entity_type, observations)
            VALUES (?, ?, ?)
        """, ("test_entity", "test_type", "test_observation"))
        await conn.commit()
    
    # Check size increased
    health = await connection_pool.health_check.check_health()
    assert health["database_size"] > initial_size

async def test_wal_file_status(connection_pool):
    """Test WAL (Write-Ahead Log) file status checking."""
    health = await connection_pool.health_check.check_health()
    assert "wal_status" in health
    wal_status = health["wal_status"]
    
    assert "exists" in wal_status
    assert "size" in wal_status
    assert isinstance(wal_status["size"], int)
    assert wal_status["size"] >= 0

async def test_connection_pool_status(connection_pool):
    """Test connection pool status monitoring."""
    health = await connection_pool.health_check.check_health()
    pool_status = health["connection_pool"]
    
    assert pool_status["max_connections"] == connection_pool._pool_size
    assert pool_status["active_connections"] >= 0
    assert pool_status["available_connections"] >= 0
    assert pool_status["active_connections"] + pool_status["available_connections"] == pool_status["max_connections"]

async def test_degraded_status_simulation(connection_pool):
    """Test health check reports degraded status under load."""
    # Simulate high connection usage
    connections = []
    try:
        for _ in range(connection_pool._pool_size):
            conn = await connection_pool.get_connection().__aenter__()
            connections.append(conn)
        
        health = await connection_pool.health_check.check_health()
        assert health["status"] == "degraded"
        assert health["connection_pool"]["available_connections"] == 0
        
    finally:
        # Clean up connections
        for conn in connections:
            await connection_pool.get_connection().__aexit__(None, None, None)

async def test_health_check_performance(connection_pool):
    """Test health check completes within reasonable time."""
    start_time = datetime.now()
    await connection_pool.health_check.check_health()
    duration = (datetime.now() - start_time).total_seconds()
    
    assert duration < 1.0  # Health check should complete within 1 second

@pytest.mark.parametrize("table_name", [
    "entities",
    "relations",
    "cloud_resources",
    "terraform_states"
])
async def test_table_statistics(connection_pool, table_name):
    """Test table statistics collection for different tables."""
    health = await connection_pool.health_check.check_health()
    assert "table_stats" in health
    
    table_stats = health["table_stats"]
    assert table_name in table_stats
    assert "row_count" in table_stats[table_name]
    assert "size_bytes" in table_stats[table_name]
    assert isinstance(table_stats[table_name]["row_count"], int)
    assert isinstance(table_stats[table_name]["size_bytes"], int)

async def test_index_health(connection_pool):
    """Test index health checking."""
    health = await connection_pool.health_check.check_health()
    assert "index_stats" in health
    
    index_stats = health["index_stats"]
    assert isinstance(index_stats, dict)
    assert len(index_stats) > 0  # Should have at least some indices
    
    # Check format of index statistics
    for index_name, stats in index_stats.items():
        assert "table_name" in stats
        assert "column_names" in stats
        assert "size_bytes" in stats
        assert isinstance(stats["size_bytes"], int)
