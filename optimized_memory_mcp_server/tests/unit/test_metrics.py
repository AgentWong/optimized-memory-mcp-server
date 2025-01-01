"""Unit tests for metrics collection and health checks."""
import pytest
import asyncio
from datetime import datetime, timedelta
from ....monitoring.metrics import MetricsCollector, HealthCheck

async def test_query_metrics_recording(connection_pool):
    """Test recording and retrieving query metrics."""
    # Record some test queries
    connection_pool.metrics.record_query("select", 0.1, cache_hit=False)
    connection_pool.metrics.record_query("select", 0.2, cache_hit=True)
    connection_pool.metrics.record_query("insert", 0.3, cache_hit=False)
    
    # Check metrics
    metrics = connection_pool.metrics.query_metrics
    assert "select" in metrics
    assert metrics["select"].total_queries == 2
    assert metrics["select"].cache_hits == 1
    assert metrics["select"].avg_duration > 0
    
    assert "insert" in metrics
    assert metrics["insert"].total_queries == 1
    assert metrics["insert"].cache_hits == 0

async def test_connection_metrics(connection_pool):
    """Test connection pool metrics tracking."""
    # Get some connections
    async with connection_pool.get_connection() as conn1:
        metrics = connection_pool.metrics.connection_metrics
        assert metrics["active_connections"] == 1
        
        async with connection_pool.get_connection() as conn2:
            metrics = connection_pool.metrics.connection_metrics
            assert metrics["active_connections"] == 2
            assert metrics["total_connections"] > 0
            assert metrics["peak_connections"] >= 2

    # Check connections are released
    metrics = connection_pool.metrics.connection_metrics
    assert metrics["active_connections"] == 0

async def test_percentile_calculation(connection_pool):
    """Test query time percentile calculations."""
    # Record query times
    times = [0.1, 0.2, 0.3, 0.4, 0.5]
    for t in times:
        connection_pool.metrics.record_query("test_query", t)
        
    p95 = connection_pool.metrics._calculate_percentile("test_query", 0.95)
    assert p95 >= max(times) * 0.9  # Allow some floating point variance

async def test_metrics_cleanup(connection_pool):
    """Test automatic cleanup of old metrics."""
    # Record old metrics
    old_time = datetime.now() - timedelta(hours=2)
    connection_pool.metrics.query_times["old_query"] = [
        (0.1, old_time.timestamp())
    ]
    
    # Record new metrics
    connection_pool.metrics.record_query("new_query", 0.1)
    
    # Trigger cleanup
    connection_pool.metrics._cleanup_old_metrics()
    
    assert "old_query" not in connection_pool.metrics.query_times
    assert "new_query" in connection_pool.metrics.query_times

async def test_health_check(connection_pool):
    """Test database health check functionality."""
    health_status = await connection_pool.health_check.check_health()
    
    assert "status" in health_status
    assert "response_time" in health_status
    assert "database_size" in health_status
    assert "connection_pool" in health_status
    
    assert health_status["status"] in ("healthy", "degraded", "unhealthy")
    assert isinstance(health_status["response_time"], float)
    assert isinstance(health_status["database_size"], int)
    assert isinstance(health_status["connection_pool"], dict)

async def test_health_check_connection_pool(connection_pool):
    """Test health check connection pool metrics."""
    health_status = await connection_pool.health_check.check_health()
    pool_status = health_status["connection_pool"]
    
    assert "active_connections" in pool_status
    assert "available_connections" in pool_status
    assert "max_connections" in pool_status
    assert pool_status["max_connections"] == connection_pool._pool_size
